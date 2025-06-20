#!/usr/bin/env python3
"""
Statistics Canada Dimension Registry Creator
===========================================

Script:     14_create_dimension_registry.py
Purpose:    Create canonical dimension registry in dictionary schema
Author:     Paul Verbrugge with Claude Sonnet 4 (Anthropic)
Created:    2025
Updated:    June 2025

Overview:
--------
This script creates the canonical dimension registry by loading processed
dimensions and computing additional characteristics like hierarchy and mixed UOM flags.
Only new dimensions are added (deduplication via ON CONFLICT DO NOTHING).

Requires: 12_build_canonical_registry.py and 13_create_dimension_set_members.py 
to have run successfully first.

Key Operations:
--------------
• Load canonical dimensions from processing.dimension_set
• Compute is_tree flag (any members have parent relationships)
• Compute has_mixed_uom flag (multiple different UOM codes)
• Insert new dimensions into dictionary.dimension_set
• Generate summary statistics

Processing Pipeline:
-------------------
1. Load processed canonical dimensions
2. Load member data to compute dimension characteristics
3. Compute is_tree and has_mixed_uom flags
4. Insert new dimensions into dictionary registry
5. Report on new vs existing dimensions
"""

import pandas as pd
import psycopg2
from loguru import logger
from statcan.tools.config import DB_CONFIG

logger.add("/app/logs/create_dimension_registry.log", rotation="1 MB", retention="7 days")

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

def check_required_tables():
    """Verify required tables exist"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        
        required_tables = [
            ('processing', 'dimension_set'),
            ('processing', 'dimension_set_members'),
            ('dictionary', 'dimension_set')
        ]
        
        for schema, table_name in required_tables:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                )
            """, (schema, table_name))
            
            if not cur.fetchone()[0]:
                raise Exception(
                    f"❌ Required table {schema}.{table_name} does not exist! "
                    "Please run the DDL script to create it first."
                )
        
        logger.info("✅ All required tables exist")

def compute_dimension_characteristics():
    """Compute additional dimension characteristics from member data"""
    logger.info("🔨 Computing dimension characteristics...")
    
    with get_db_conn() as conn:
        # Compute is_tree flag (any members have parent relationships)
        tree_dimensions = pd.read_sql("""
            SELECT dimension_hash, 
                   BOOL_OR(parent_member_id IS NOT NULL) as is_tree
            FROM processing.dimension_set_members
            GROUP BY dimension_hash
        """, conn)
        
        # Compute has_mixed_uom flag (multiple different UOM codes, ignoring nulls)
        mixed_uom_dimensions = pd.read_sql("""
            SELECT dimension_hash,
                   COUNT(DISTINCT member_uom_code) > 1 as has_mixed_uom
            FROM processing.dimension_set_members
            WHERE member_uom_code IS NOT NULL
            GROUP BY dimension_hash
        """, conn)
        
        logger.info(f"📊 Computed characteristics for {len(tree_dimensions)} dimensions")
        
        return tree_dimensions, mixed_uom_dimensions

def create_dimension_registry():
    """Create canonical dimension registry from processed dimensions"""
    logger.info("🚀 Starting dimension registry creation...")
    
    check_required_tables()
    
    with get_db_conn() as conn:
        # Load processed canonical dimensions
        logger.info("📥 Loading processed canonical dimensions...")
        canonical_dims = pd.read_sql("""
            SELECT dimension_hash, dimension_name_en, dimension_name_fr,
                   dimension_name_en_slug, dimension_name_fr_slug,
                   has_uom, usage_count
            FROM processing.dimension_set
        """, conn)
        
        logger.info(f"📊 Processing {len(canonical_dims)} canonical dimensions")
        
        # Compute additional characteristics
        tree_dims, mixed_uom_dims = compute_dimension_characteristics()
        
        # Merge characteristics
        registry_data = canonical_dims.merge(
            tree_dims, on='dimension_hash', how='left'
        ).merge(
            mixed_uom_dims, on='dimension_hash', how='left'
        )
        
        # Fill missing values
        registry_data['is_tree'] = registry_data['is_tree'].fillna(False)
        registry_data['has_mixed_uom'] = registry_data['has_mixed_uom'].fillna(False)
        
        logger.info("🔨 Inserting dimensions into registry...")
        
        # Insert into dictionary registry (deduplication via ON CONFLICT DO NOTHING)
        cur = conn.cursor()
        inserted_count = 0
        
        for _, row in registry_data.iterrows():
            cur.execute("""
                INSERT INTO dictionary.dimension_set (
                    dimension_hash, dimension_name_en, dimension_name_fr,
                    dimension_name_en_slug, dimension_name_fr_slug,
                    has_uom, is_tree, has_mixed_uom, usage_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dimension_hash) DO NOTHING
            """, (
                row['dimension_hash'],
                row['dimension_name_en'],
                row['dimension_name_fr'],
                row['dimension_name_en_slug'],
                row['dimension_name_fr_slug'],
                row['has_uom'],
                row['is_tree'],
                row['has_mixed_uom'],
                int(row['usage_count'])
            ))
            
            if cur.rowcount > 0:
                inserted_count += 1
        
        conn.commit()
        
        existing_count = len(registry_data) - inserted_count
        
        logger.success(f"✅ Registry update complete:")
        logger.success(f"   • {inserted_count:,} new dimensions added")
        logger.success(f"   • {existing_count:,} dimensions already existed")

def generate_registry_stats():
    """Generate and log registry statistics"""
    logger.info("📊 Generating registry statistics...")
    
    with get_db_conn() as conn:
        # Total dimensions
        result = pd.read_sql("SELECT COUNT(*) as count FROM dictionary.dimension_set", conn)
        total_dimensions = result.iloc[0]['count']
        
        # Characteristics breakdown
        characteristics = pd.read_sql("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN is_tree THEN 1 ELSE 0 END) as hierarchical,
                SUM(CASE WHEN has_mixed_uom THEN 1 ELSE 0 END) as mixed_uom,
                SUM(CASE WHEN has_uom THEN 1 ELSE 0 END) as has_uom
            FROM dictionary.dimension_set
        """, conn)
        
        # Top dimensions by usage
        top_dims = pd.read_sql("""
            SELECT dimension_name_en, usage_count, is_tree, has_mixed_uom
            FROM dictionary.dimension_set 
            ORDER BY usage_count DESC 
            LIMIT 5
        """, conn)
        
        stats = characteristics.iloc[0]
        
        logger.success(f"📈 Registry Statistics:")
        logger.success(f"   • {total_dimensions:,} total dimensions in registry")
        logger.success(f"   • {stats['hierarchical']:,} hierarchical dimensions ({stats['hierarchical']/stats['total']*100:.1f}%)")
        logger.success(f"   • {stats['mixed_uom']:,} mixed UOM dimensions ({stats['mixed_uom']/stats['total']*100:.1f}%)")
        logger.success(f"   • {stats['has_uom']:,} dimensions with UOM ({stats['has_uom']/stats['total']*100:.1f}%)")
        
        logger.info("🏆 Top 5 most used dimensions:")
        for _, dim in top_dims.iterrows():
            flags = []
            if dim['is_tree']: flags.append("hierarchical")
            if dim['has_mixed_uom']: flags.append("mixed UOM")
            flag_str = f" [{', '.join(flags)}]" if flags else ""
            logger.info(f"   • {dim['dimension_name_en']}: {dim['usage_count']:,} uses{flag_str}")

def main():
    """Main registry creation function"""
    try:
        # Create dimension registry
        create_dimension_registry()
        
        # Generate summary stats
        generate_registry_stats()
        
        logger.success("🎉 Dimension registry creation complete!")
        
    except Exception as e:
        logger.exception(f"❌ Registry creation failed: {e}")
        raise

if __name__ == "__main__":
    main()

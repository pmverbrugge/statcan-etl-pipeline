#!/usr/bin/env python3
"""
Statistics Canada Canonical Dimension Registry Builder
=====================================================

Script:     12_build_canonical_registry.py
Purpose:    Build canonical dimension registry from processed dimensions
Author:     Paul Verbrugge with Claude Sonnet 4 (Anthropic)
Created:    2025
Updated:    June 2025

Overview:
--------
This script creates the canonical dimension registry by summarizing processed
dimensions by dimension_hash and selecting the most common labels with proper
formatting for presentation.

Key Changes:
- All references now point to processing schema (no dictionary schema dependencies)
- Enhanced error handling and validation
- TimescaleDB optimization support
- Improved slugification using python-slugify

Requires: 11_process_dimensions.py to have run successfully first.

Key Operations:
--------------
• Aggregate processed_dimensions by dimension_hash
• Select most common English/French dimension names
• Apply title case formatting to canonical names
• Create slugified versions of names
• Build processing.dimension_set (canonical definitions)

Processing Pipeline:
-------------------
1. Load processed dimensions from script 11
2. Group by dimension_hash and select most common labels
3. Apply title case and create slugs
4. Store canonical definitions in processing.dimension_set
5. Generate summary statistics

Note: Cube-to-dimension mapping remains in processing.processed_dimensions
for now. Final registry mapping will be handled in later scripts.
"""

import pandas as pd
import psycopg2
from slugify import slugify
from loguru import logger
from statcan.tools.config import DB_CONFIG

logger.add("/app/logs/build_canonical_registry.log", rotation="1 MB", retention="7 days")

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

def title_case(text):
    """Apply title case to text"""
    if pd.isna(text) or text is None:
        return None
    return str(text).title()

def create_slug(text):
    """Create URL-friendly slug from text"""
    if pd.isna(text) or text is None:
        return None
    return slugify(text, separator="_").lower()

def check_required_tables():
    """Verify required tables exist"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        
        required_tables = [
            ('processing', 'processed_dimensions'),
            ('processing', 'dimension_set')
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

def build_canonical_dimensions():
    """Build canonical dimension definitions from processed dimensions"""
    logger.info("🚀 Starting canonical dimension registry build...")
    
    check_required_tables()
    
    with get_db_conn() as conn:
        # Load processed dimensions
        logger.info("📥 Loading processed dimension data...")
        processed_dims = pd.read_sql("""
            SELECT dimension_hash, dimension_name_en, dimension_name_fr, has_uom,
                   productid, dimension_position
            FROM processing.processed_dimensions
        """, conn)
        
        logger.info(f"📊 Processing {len(processed_dims)} dimension instances")
        
        # Group by dimension_hash and select most common labels
        logger.info("🔨 Building canonical dimension definitions...")
        
        def select_canonical_labels(group):
            """Select most common labels for a dimension_hash"""
            # Count frequency of each English label
            en_counts = group['dimension_name_en'].value_counts()
            most_common_en = en_counts.index[0] if len(en_counts) > 0 else None
            
            # Count frequency of each French label  
            fr_counts = group['dimension_name_fr'].value_counts()
            most_common_fr = fr_counts.index[0] if len(fr_counts) > 0 else None
            
            # Take maximum value of has_uom (TRUE wins over FALSE)
            max_has_uom = group['has_uom'].max() if group['has_uom'].notna().any() else False
            
            # Count usage
            usage_count = len(group)
            
            return pd.Series({
                'dimension_name_en': most_common_en,
                'dimension_name_fr': most_common_fr,
                'has_uom': max_has_uom,
                'usage_count': usage_count
            })
        
        # Create canonical dimension set
        canonical_dims = (
            processed_dims.groupby('dimension_hash')
            .apply(select_canonical_labels, include_groups=False)
            .reset_index()
        )
        
        # Apply title case and create slugs
        logger.info("🎨 Applying title case and creating slugs...")
        canonical_dims['dimension_name_en'] = canonical_dims['dimension_name_en'].apply(title_case)
        canonical_dims['dimension_name_fr'] = canonical_dims['dimension_name_fr'].apply(title_case)
        
        canonical_dims['dimension_name_en_slug'] = canonical_dims['dimension_name_en'].apply(create_slug)
        canonical_dims['dimension_name_fr_slug'] = canonical_dims['dimension_name_fr'].apply(create_slug)
        
        logger.info(f"📈 Created {len(canonical_dims)} canonical dimension definitions")
        
        # Store canonical dimensions
        logger.info("💾 Storing canonical dimension definitions...")
        cur = conn.cursor()
        
        # Clear existing data
        cur.execute("TRUNCATE TABLE processing.dimension_set")
        
        # Insert canonical definitions
        for _, row in canonical_dims.iterrows():
            cur.execute("""
                INSERT INTO processing.dimension_set (
                    dimension_hash, dimension_name_en, dimension_name_fr,
                    dimension_name_en_slug, dimension_name_fr_slug,
                    has_uom, usage_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                row['dimension_hash'],
                row['dimension_name_en'],
                row['dimension_name_fr'], 
                row['dimension_name_en_slug'],
                row['dimension_name_fr_slug'],
                row['has_uom'],
                int(row['usage_count'])
            ))
        
        conn.commit()
        logger.success(f"✅ Stored {len(canonical_dims)} canonical dimension definitions")

def generate_summary_stats():
    """Generate and log summary statistics"""
    logger.info("📊 Generating registry statistics...")
    
    with get_db_conn() as conn:
        # Canonical dimensions
        result = pd.read_sql("SELECT COUNT(*) as count FROM processing.dimension_set", conn)
        canonical_count = result.iloc[0]['count']
        
        # Total dimension instances
        result = pd.read_sql("SELECT COUNT(*) as count FROM processing.processed_dimensions", conn)
        total_instances = result.iloc[0]['count']
        
        # Deduplication rate
        deduplication_rate = ((total_instances - canonical_count) / total_instances * 100) if total_instances > 0 else 0
        
        # Most used dimensions
        top_dims = pd.read_sql("""
            SELECT dimension_name_en, usage_count 
            FROM processing.dimension_set 
            ORDER BY usage_count DESC 
            LIMIT 5
        """, conn)
        
        logger.success(f"📈 Registry Summary:")
        logger.success(f"   • {canonical_count:,} canonical dimensions")
        logger.success(f"   • {total_instances:,} total dimension instances")
        logger.success(f"   • {deduplication_rate:.1f}% deduplication rate")
        
        logger.info("🏆 Top 5 most used dimensions:")
        for _, dim in top_dims.iterrows():
            logger.info(f"   • {dim['dimension_name_en']}: {dim['usage_count']:,} uses")

def main():
    """Main registry building function"""
    try:
        # Build canonical dimensions
        build_canonical_dimensions()
        
        # Generate summary
        generate_summary_stats()
        
        logger.success("🎉 Canonical dimension registry build complete!")
        logger.info("📝 Cube-to-dimension mappings remain in processing.processed_dimensions")
        
    except Exception as e:
        logger.exception(f"❌ Registry build failed: {e}")
        raise

if __name__ == "__main__":
    main()

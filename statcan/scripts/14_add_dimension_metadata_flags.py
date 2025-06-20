#!/usr/bin/env python3
"""
Statistics Canada Dimension Metadata Flag Generator
==================================================

Script:     14_add_dimension_metadata_flags.py
Purpose:    Calculate and populate metadata flags for dimension registry
Author:     Paul Verbrugge with Claude Sonnet 4 (Anthropic)
Created:    2025
Updated:    June 2025

Overview:
--------
This script analyzes the members within each dimension to calculate metadata flags:
- is_tree: True if any members have parent-child relationships (hierarchical structure)
- is_hetero: True if members have varying units of measure (heterogeneous UOM)

These flags provide important metadata about dimension structure for downstream analysis
and help categorize dimensions by their organizational patterns.

Requires: Scripts 10-13 to have run successfully first.

Key Operations:
--------------
• Analyze parent_member_id relationships to detect hierarchical structures (is_tree)
• Analyze member_uom_code variation to detect heterogeneous units (is_hetero)
• Update processing.dimension_set with calculated flags
• Generate summary statistics on dimension characteristics

Processing Pipeline:
-------------------
1. Load dimension_set_members data from script 13
2. Group by dimension_hash and analyze member relationships
3. Calculate is_tree flag based on parent_member_id presence
4. Calculate is_hetero flag based on member_uom_code variation
5. Update processing.dimension_set with calculated flags
6. Generate summary statistics and validation reports
"""

import pandas as pd
import psycopg2
from loguru import logger
from statcan.tools.config import DB_CONFIG

logger.add("/app/logs/add_dimension_metadata_flags.log", rotation="1 MB", retention="7 days")

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

def check_required_tables():
    """Verify required tables exist"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        
        required_tables = [
            ('processing', 'dimension_set'),
            ('processing', 'dimension_set_members')
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
                    "Please run scripts 10-13 first."
                )
        
        # Check if the new columns exist
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'processing' 
            AND table_name = 'dimension_set'
            AND column_name IN ('is_tree', 'is_hetero')
        """)
        
        existing_columns = [row[0] for row in cur.fetchall()]
        missing_columns = set(['is_tree', 'is_hetero']) - set(existing_columns)
        
        if missing_columns:
            raise Exception(
                f"❌ Missing columns in processing.dimension_set: {missing_columns}. "
                "Please add these columns first."
            )
        
        logger.info("✅ All required tables and columns exist")

def calculate_metadata_flags():
    """Calculate is_tree and is_hetero flags for each dimension"""
    logger.info("🚀 Starting dimension metadata flag calculation...")
    
    check_required_tables()
    
    with get_db_conn() as conn:
        # Load dimension members data
        logger.info("📥 Loading dimension members data...")
        
        members_data = pd.read_sql("""
            SELECT 
                dimension_hash,
                member_id,
                parent_member_id,
                member_uom_code
            FROM processing.dimension_set_members
            ORDER BY dimension_hash, member_id
        """, conn)
        
        logger.info(f"📊 Analyzing {len(members_data)} member records across dimensions")
        
        # Calculate flags by dimension
        logger.info("🔨 Calculating metadata flags...")
        
        def calculate_dimension_flags(group):
            """Calculate is_tree and is_hetero flags for a dimension group"""
            # is_tree: True if any member has a parent (non-null parent_member_id)
            has_parents = group['parent_member_id'].notna().any()
            is_tree = bool(has_parents)
            
            # is_hetero: True if there are multiple distinct non-null UOM codes
            uom_codes = group['member_uom_code'].dropna().unique()
            is_hetero = len(uom_codes) > 1
            
            return pd.Series({
                'is_tree': is_tree,
                'is_hetero': is_hetero,
                'member_count': len(group),
                'parent_count': group['parent_member_id'].notna().sum(),
                'uom_code_count': len(uom_codes),
                'uom_codes': list(uom_codes) if len(uom_codes) <= 5 else list(uom_codes[:5]) + ['...']
            })
        
        # Group by dimension and calculate flags
        dimension_flags = (
            members_data.groupby('dimension_hash')
            .apply(calculate_dimension_flags, include_groups=False)
            .reset_index()
        )
        
        logger.info(f"📈 Calculated flags for {len(dimension_flags)} dimensions")
        
        # Update dimension_set table with flags
        logger.info("💾 Updating dimension_set with metadata flags...")
        
        cur = conn.cursor()
        update_count = 0
        
        for _, row in dimension_flags.iterrows():
            cur.execute("""
                UPDATE processing.dimension_set 
                SET is_tree = %s, is_hetero = %s
                WHERE dimension_hash = %s
            """, (
                row['is_tree'],
                row['is_hetero'],
                row['dimension_hash']
            ))
            update_count += cur.rowcount
        
        conn.commit()
        logger.success(f"✅ Updated {update_count} dimension records with metadata flags")
        
        return dimension_flags

def generate_summary_statistics(dimension_flags):
    """Generate and log summary statistics"""
    logger.info("📊 Generating metadata flag statistics...")
    
    total_dimensions = len(dimension_flags)
    tree_dimensions = dimension_flags['is_tree'].sum()
    hetero_dimensions = dimension_flags['is_hetero'].sum()
    both_flags = ((dimension_flags['is_tree']) & (dimension_flags['is_hetero'])).sum()
    
    tree_percentage = (tree_dimensions / total_dimensions * 100) if total_dimensions > 0 else 0
    hetero_percentage = (hetero_dimensions / total_dimensions * 100) if total_dimensions > 0 else 0
    both_percentage = (both_flags / total_dimensions * 100) if total_dimensions > 0 else 0
    
    logger.success(f"📈 Metadata Flag Summary:")
    logger.success(f"   • Total dimensions: {total_dimensions:,}")
    logger.success(f"   • Hierarchical (is_tree=true): {tree_dimensions:,} ({tree_percentage:.1f}%)")
    logger.success(f"   • Heterogeneous UOM (is_hetero=true): {hetero_dimensions:,} ({hetero_percentage:.1f}%)")
    logger.success(f"   • Both tree and hetero: {both_flags:,} ({both_percentage:.1f}%)")
    
    # Show examples of each type
    tree_examples = dimension_flags[dimension_flags['is_tree']].head(3)
    hetero_examples = dimension_flags[dimension_flags['is_hetero']].head(3)
    
    if len(tree_examples) > 0:
        logger.info("🌳 Examples of hierarchical dimensions (is_tree=true):")
        for _, dim in tree_examples.iterrows():
            logger.info(f"   • {dim['dimension_hash']}: {dim['member_count']} members, {dim['parent_count']} with parents")
    
    if len(hetero_examples) > 0:
        logger.info("🔀 Examples of heterogeneous UOM dimensions (is_hetero=true):")
        for _, dim in hetero_examples.iterrows():
            uom_display = ', '.join(map(str, dim['uom_codes'])) if dim['uom_codes'] else 'None'
            logger.info(f"   • {dim['dimension_hash']}: {dim['uom_code_count']} UOM codes ({uom_display})")

def validate_flag_calculations():
    """Validate the calculated flags against the raw data"""
    logger.info("🔍 Validating flag calculations...")
    
    with get_db_conn() as conn:
        # Check a few dimensions manually to validate logic
        validation_query = """
            SELECT 
                ds.dimension_hash,
                ds.dimension_name_en,
                ds.is_tree,
                ds.is_hetero,
                COUNT(dsm.member_id) as member_count,
                COUNT(dsm.parent_member_id) as parent_count,
                COUNT(DISTINCT dsm.member_uom_code) as distinct_uom_count
            FROM processing.dimension_set ds
            LEFT JOIN processing.dimension_set_members dsm ON ds.dimension_hash = dsm.dimension_hash
            GROUP BY ds.dimension_hash, ds.dimension_name_en, ds.is_tree, ds.is_hetero
            HAVING (ds.is_tree = true AND COUNT(dsm.parent_member_id) = 0)
                OR (ds.is_tree = false AND COUNT(dsm.parent_member_id) > 0)
                OR (ds.is_hetero = true AND COUNT(DISTINCT dsm.member_uom_code) <= 1)
                OR (ds.is_hetero = false AND COUNT(DISTINCT dsm.member_uom_code) > 1)
            LIMIT 5
        """
        
        validation_results = pd.read_sql(validation_query, conn)
        
        if len(validation_results) > 0:
            logger.warning(f"⚠️ Found {len(validation_results)} potential flag calculation inconsistencies:")
            for _, row in validation_results.iterrows():
                logger.warning(f"   • {row['dimension_hash']} ({row['dimension_name_en']}): "
                              f"is_tree={row['is_tree']}, parent_count={row['parent_count']}, "
                              f"is_hetero={row['is_hetero']}, uom_count={row['distinct_uom_count']}")
        else:
            logger.success("✅ All flag calculations appear consistent with underlying data")



def main():
    """Main metadata flag calculation function"""
    try:
        # Calculate metadata flags
        dimension_flags = calculate_metadata_flags()
        
        # Generate summary statistics
        generate_summary_statistics(dimension_flags)
        
        # Validate calculations
        validate_flag_calculations()
        
        logger.success("🎉 Dimension metadata flag calculation complete!")
        logger.info("📝 Flags available in processing.dimension_set: is_tree, is_hetero")
        
    except Exception as e:
        logger.exception(f"❌ Metadata flag calculation failed: {e}")
        raise

if __name__ == "__main__":
    main()

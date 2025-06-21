#!/usr/bin/env python3
"""
Statcan Public Data ETL Pipeline
Script: 14_add_dimension_metadata_flags.py
Date: 2025-06-21
Author: Paul Verbrugge with Claude Sonnet 4 (Anthropic)

Calculate and populate dimension metadata flags for registry classification.

This script analyzes member relationships within each dimension to calculate structural 
metadata flags that help categorize dimensions by their organizational patterns.
The flags provide essential metadata for downstream analytical processes.

Key Operations:
- Analyze parent_member_id relationships to detect hierarchical structures (is_tree)
- Analyze member_uom_code variation to detect heterogeneous units (is_hetero)
- Update processing.dimension_set with calculated flags using efficient SQL
- Generate comprehensive statistics on dimension characteristics

Processing Logic:
1. Validate that dimension_set and dimension_set_members tables exist
2. Calculate metadata flags using PostgreSQL aggregation functions
3. Update dimension_set table with is_tree and is_hetero flags
4. Validate flag calculations for consistency
5. Generate summary statistics for operational monitoring

Dependencies:
- Requires processing.dimension_set from 12_create_dimension_set.py
- Requires processing.dimension_set_members from 13_create_dimension_set_members.py
- Updates flags in processing.dimension_set for downstream processing
"""

import psycopg2
from loguru import logger
from statcan.tools.config import DB_CONFIG

# Configure logging with minimal approach
logger.add("/app/logs/add_dimension_metadata_flags.log", rotation="5 MB", retention="7 days")

def validate_prerequisites():
    """Validate that prerequisite tables exist and have required columns"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Check dimension_set table
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'processing' AND table_name = 'dimension_set'
                )
            """)
            if not cur.fetchone()[0]:
                raise Exception("❌ Table processing.dimension_set does not exist")
            
            # Check dimension_set_members table
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'processing' AND table_name = 'dimension_set_members'
                )
            """)
            if not cur.fetchone()[0]:
                raise Exception("❌ Table processing.dimension_set_members does not exist")
            
            # Check required columns exist
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
                raise Exception(f"❌ Missing columns in processing.dimension_set: {missing_columns}")
            
            # Get record counts for validation
            cur.execute("SELECT COUNT(*) FROM processing.dimension_set")
            dimension_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM processing.dimension_set_members")
            member_count = cur.fetchone()[0]
            
            if dimension_count == 0:
                raise Exception("❌ No dimensions found - run script 12 first")
            
            if member_count == 0:
                raise Exception("❌ No dimension members found - run script 13 first")
            
            return dimension_count, member_count

def calculate_and_update_flags():
    """Calculate metadata flags using efficient PostgreSQL operations"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Calculate and update flags in a single efficient query
            cur.execute("""
                UPDATE processing.dimension_set 
                SET 
                    is_tree = flag_data.has_hierarchy,
                    is_hetero = flag_data.has_hetero_uom
                FROM (
                    SELECT 
                        dimension_hash,
                        -- is_tree: true if any member has a parent
                        COUNT(parent_member_id) > 0 as has_hierarchy,
                        -- is_hetero: true if multiple distinct non-null UOM codes exist
                        COUNT(DISTINCT member_uom_code) FILTER (WHERE member_uom_code IS NOT NULL) > 1 as has_hetero_uom
                    FROM processing.dimension_set_members
                    GROUP BY dimension_hash
                ) flag_data
                WHERE processing.dimension_set.dimension_hash = flag_data.dimension_hash
            """)
            
            updated_count = cur.rowcount
            conn.commit()
            
            return updated_count

def validate_flag_calculations():
    """Validate flag calculations for consistency"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Check for inconsistencies in flag calculations
            cur.execute("""
                SELECT 
                    ds.dimension_hash,
                    ds.dimension_name_en,
                    ds.is_tree,
                    ds.is_hetero,
                    COUNT(dsm.member_id) as member_count,
                    COUNT(dsm.parent_member_id) as parent_count,
                    COUNT(DISTINCT dsm.member_uom_code) FILTER (WHERE dsm.member_uom_code IS NOT NULL) as distinct_uom_count
                FROM processing.dimension_set ds
                LEFT JOIN processing.dimension_set_members dsm ON ds.dimension_hash = dsm.dimension_hash
                GROUP BY ds.dimension_hash, ds.dimension_name_en, ds.is_tree, ds.is_hetero
                HAVING 
                    -- Check for is_tree inconsistencies
                    (ds.is_tree = true AND COUNT(dsm.parent_member_id) = 0)
                    OR (ds.is_tree = false AND COUNT(dsm.parent_member_id) > 0)
                    -- Check for is_hetero inconsistencies
                    OR (ds.is_hetero = true AND COUNT(DISTINCT dsm.member_uom_code) FILTER (WHERE dsm.member_uom_code IS NOT NULL) <= 1)
                    OR (ds.is_hetero = false AND COUNT(DISTINCT dsm.member_uom_code) FILTER (WHERE dsm.member_uom_code IS NOT NULL) > 1)
                LIMIT 10
            """)
            
            inconsistencies = cur.fetchall()
            return inconsistencies

def generate_flag_statistics():
    """Generate comprehensive flag statistics"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Get overall flag statistics
            cur.execute("""
                SELECT 
                    COUNT(*) as total_dimensions,
                    COUNT(*) FILTER (WHERE is_tree = true) as tree_dimensions,
                    COUNT(*) FILTER (WHERE is_hetero = true) as hetero_dimensions,
                    COUNT(*) FILTER (WHERE is_tree = true AND is_hetero = true) as both_flags,
                    COUNT(*) FILTER (WHERE is_tree = false AND is_hetero = false) as neither_flags
                FROM processing.dimension_set
            """)
            
            total, tree, hetero, both, neither = cur.fetchone()
            
            # Calculate percentages
            tree_pct = (tree / total * 100) if total > 0 else 0
            hetero_pct = (hetero / total * 100) if total > 0 else 0
            both_pct = (both / total * 100) if total > 0 else 0
            
            # Get examples of each type for validation
            cur.execute("""
                SELECT ds.dimension_hash, ds.dimension_name_en, 
                       COUNT(dsm.member_id) as member_count,
                       COUNT(dsm.parent_member_id) as parent_count
                FROM processing.dimension_set ds
                JOIN processing.dimension_set_members dsm ON ds.dimension_hash = dsm.dimension_hash
                WHERE ds.is_tree = true
                GROUP BY ds.dimension_hash, ds.dimension_name_en
                ORDER BY member_count DESC
                LIMIT 3
            """)
            tree_examples = cur.fetchall()
            
            cur.execute("""
                SELECT ds.dimension_hash, ds.dimension_name_en,
                       COUNT(DISTINCT dsm.member_uom_code) FILTER (WHERE dsm.member_uom_code IS NOT NULL) as uom_count
                FROM processing.dimension_set ds
                JOIN processing.dimension_set_members dsm ON ds.dimension_hash = dsm.dimension_hash
                WHERE ds.is_hetero = true
                GROUP BY ds.dimension_hash, ds.dimension_name_en
                ORDER BY uom_count DESC
                LIMIT 3
            """)
            hetero_examples = cur.fetchall()
            
            return {
                'total': total,
                'tree': tree,
                'hetero': hetero,
                'both': both,
                'neither': neither,
                'tree_pct': tree_pct,
                'hetero_pct': hetero_pct,
                'both_pct': both_pct,
                'tree_examples': tree_examples,
                'hetero_examples': hetero_examples
            }

def main():
    """Main metadata flag calculation function"""
    logger.info("🚀 Starting dimension metadata flag calculation...")
    
    try:
        # Validate prerequisites
        dimension_count, member_count = validate_prerequisites()
        logger.info(f"📊 Processing {dimension_count:,} dimensions with {member_count:,} members...")
        
        # Calculate and update flags
        updated_count = calculate_and_update_flags()
        
        # Validate calculations
        inconsistencies = validate_flag_calculations()
        
        # Generate statistics
        stats = generate_flag_statistics()
        
        # Final summary
        logger.success(f"✅ Flag calculation complete: {updated_count:,} dimensions updated")
        
        # Warn about inconsistencies
        if inconsistencies:
            logger.warning(f"⚠️ Found {len(inconsistencies)} flag calculation inconsistencies - check data quality")
        
        # Log key statistics only if significant
        if stats['tree'] > 0 or stats['hetero'] > 0:
            logger.info(f"📈 Hierarchical dimensions: {stats['tree']:,} ({stats['tree_pct']:.1f}%)")
            logger.info(f"📈 Heterogeneous UOM dimensions: {stats['hetero']:,} ({stats['hetero_pct']:.1f}%)")
        
        if stats['both_pct'] > 5:  # Only log if significant percentage
            logger.info(f"📈 Both hierarchical and heterogeneous: {stats['both']:,} ({stats['both_pct']:.1f}%)")
        
    except Exception as e:
        logger.exception(f"❌ Metadata flag calculation failed: {e}")
        raise

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Statcan Public Data ETL Pipeline
Script: 15_calculate_tree_levels.py
Date: 2025-06-21
Author: Paul Verbrugge with Claude Sonnet 4 (Anthropic)

Calculate hierarchical tree levels for dimension members using efficient SQL recursion.

This script calculates tree_level values for members in hierarchical dimensions using
PostgreSQL's recursive Common Table Expressions (CTEs) for optimal performance. It
handles validation, circular reference detection, and level assignment efficiently.

Key Operations:
- Clear tree_level for non-hierarchical dimensions (is_tree=false)
- Use recursive CTEs to calculate levels efficiently in SQL
- Detect and handle circular references and orphaned members
- Update tree_level values using set-based operations
- Generate comprehensive validation and summary statistics

Processing Logic:
1. Clear tree_level for non-hierarchical dimensions
2. Use PostgreSQL recursive CTE to calculate tree levels efficiently
3. Detect circular references and orphaned members using SQL
4. Update tree_level values in batch operations
5. Validate results and generate summary statistics

Dependencies:
- Requires processing.dimension_set with is_tree flags from script 14
- Requires processing.dimension_set_members from script 13
- Updates tree_level column in processing.dimension_set_members
"""

import psycopg2
from loguru import logger
from statcan.tools.config import DB_CONFIG

# Configure logging with minimal approach
logger.add("/app/logs/calculate_tree_levels.log", rotation="5 MB", retention="7 days")

def validate_prerequisites():
    """Validate that prerequisite tables exist and have required columns"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Check dimension_set table with is_tree column
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_schema = 'processing' 
                      AND table_name = 'dimension_set'
                      AND column_name = 'is_tree'
                )
            """)
            if not cur.fetchone()[0]:
                raise Exception("❌ Column is_tree does not exist in processing.dimension_set")
            
            # Check dimension_set_members table with tree_level column
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_schema = 'processing' 
                      AND table_name = 'dimension_set_members'
                      AND column_name = 'tree_level'
                )
            """)
            if not cur.fetchone()[0]:
                raise Exception("❌ Column tree_level does not exist in processing.dimension_set_members")
            
            # Get counts for validation
            cur.execute("""
                SELECT 
                    COUNT(*) as total_dimensions,
                    COUNT(*) FILTER (WHERE is_tree = true) as hierarchical_dimensions
                FROM processing.dimension_set
            """)
            total_dims, hierarchical_dims = cur.fetchone()
            
            cur.execute("SELECT COUNT(*) FROM processing.dimension_set_members")
            total_members = cur.fetchone()[0]
            
            if total_dims == 0:
                raise Exception("❌ No dimensions found - run script 12 first")
            
            if total_members == 0:
                raise Exception("❌ No dimension members found - run script 13 first")
            
            return total_dims, hierarchical_dims, total_members

def clear_non_hierarchical_tree_levels():
    """Clear tree_level for members in non-hierarchical dimensions"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE processing.dimension_set_members 
                SET tree_level = NULL
                WHERE dimension_hash IN (
                    SELECT dimension_hash 
                    FROM processing.dimension_set 
                    WHERE is_tree = false OR is_tree IS NULL
                )
            """)
            
            cleared_count = cur.rowcount
            conn.commit()
            
            return cleared_count

def detect_data_quality_issues():
    """Detect circular references, orphaned members, and self-references using SQL"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Detect self-references
            cur.execute("""
                SELECT dimension_hash, member_id, 'self_reference' as issue_type
                FROM processing.dimension_set_members
                WHERE member_id = parent_member_id
            """)
            self_refs = cur.fetchall()
            
            # Detect orphaned members (parent doesn't exist in same dimension)
            cur.execute("""
                SELECT dsm1.dimension_hash, dsm1.member_id, dsm1.parent_member_id, 'orphaned_member' as issue_type
                FROM processing.dimension_set_members dsm1
                LEFT JOIN processing.dimension_set_members dsm2 
                    ON dsm1.dimension_hash = dsm2.dimension_hash 
                    AND dsm1.parent_member_id = dsm2.member_id
                WHERE dsm1.parent_member_id IS NOT NULL 
                    AND dsm2.member_id IS NULL
                LIMIT 10  -- Limit to avoid excessive logging
            """)
            orphaned_members = cur.fetchall()
            
            return self_refs, orphaned_members

def calculate_tree_levels_sql():
    """Calculate tree levels using PostgreSQL recursive CTE for maximum efficiency"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Use recursive CTE to calculate tree levels efficiently
            cur.execute("""
                WITH RECURSIVE tree_levels AS (
                    -- Base case: root nodes (no parent) start at level 1
                    SELECT 
                        dimension_hash,
                        member_id,
                        1 as tree_level
                    FROM processing.dimension_set_members dsm
                    WHERE parent_member_id IS NULL
                      AND dimension_hash IN (
                          SELECT dimension_hash 
                          FROM processing.dimension_set 
                          WHERE is_tree = true
                      )
                    
                    UNION ALL
                    
                    -- Recursive case: children are one level deeper than their parents
                    SELECT 
                        dsm.dimension_hash,
                        dsm.member_id,
                        tl.tree_level + 1
                    FROM processing.dimension_set_members dsm
                    INNER JOIN tree_levels tl 
                        ON dsm.dimension_hash = tl.dimension_hash 
                        AND dsm.parent_member_id = tl.member_id
                    WHERE tl.tree_level < 20  -- Prevent infinite recursion
                )
                UPDATE processing.dimension_set_members
                SET tree_level = tree_levels.tree_level
                FROM tree_levels
                WHERE processing.dimension_set_members.dimension_hash = tree_levels.dimension_hash
                    AND processing.dimension_set_members.member_id = tree_levels.member_id
            """)
            
            updated_count = cur.rowcount
            conn.commit()
            
            return updated_count

def detect_circular_references():
    """Detect circular references by finding members that couldn't be assigned levels"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Find hierarchical members that should have levels but don't
            cur.execute("""
                SELECT dsm.dimension_hash, dsm.member_id, dsm.parent_member_id
                FROM processing.dimension_set_members dsm
                JOIN processing.dimension_set ds ON dsm.dimension_hash = ds.dimension_hash
                WHERE ds.is_tree = true 
                    AND dsm.tree_level IS NULL
                    AND dsm.parent_member_id IS NOT NULL
                LIMIT 10  -- Limit to avoid excessive logging
            """)
            
            circular_refs = cur.fetchall()
            return circular_refs

def validate_results():
    """Validate tree level calculation results and generate statistics"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Get overall statistics
            cur.execute("""
                SELECT 
                    COUNT(*) as total_members,
                    COUNT(tree_level) as members_with_levels,
                    COUNT(*) FILTER (WHERE tree_level IS NULL) as members_without_levels,
                    MIN(tree_level) as min_level,
                    MAX(tree_level) as max_level,
                    COUNT(DISTINCT tree_level) as distinct_levels
                FROM processing.dimension_set_members
            """)
            total, with_levels, without_levels, min_level, max_level, distinct_levels = cur.fetchone()
            
            # Get hierarchical dimension statistics
            cur.execute("""
                SELECT COUNT(*) 
                FROM processing.dimension_set_members dsm
                JOIN processing.dimension_set ds ON dsm.dimension_hash = ds.dimension_hash
                WHERE ds.is_tree = true
            """)
            hierarchical_members = cur.fetchone()[0]
            
            # Check for members in hierarchical dimensions without levels
            cur.execute("""
                SELECT COUNT(*)
                FROM processing.dimension_set_members dsm
                JOIN processing.dimension_set ds ON dsm.dimension_hash = ds.dimension_hash
                WHERE ds.is_tree = true AND dsm.tree_level IS NULL
            """)
            hierarchical_without_levels = cur.fetchone()[0]
            
            return {
                'total_members': total,
                'with_levels': with_levels,
                'without_levels': without_levels,
                'min_level': min_level,
                'max_level': max_level,
                'distinct_levels': distinct_levels,
                'hierarchical_members': hierarchical_members,
                'hierarchical_without_levels': hierarchical_without_levels
            }

def main():
    """Main tree level calculation function"""
    logger.info("🚀 Starting tree level calculation...")
    
    try:
        # Validate prerequisites
        total_dims, hierarchical_dims, total_members = validate_prerequisites()
        logger.info(f"📊 Processing {hierarchical_dims:,} hierarchical dimensions with {total_members:,} total members...")
        
        # Clear tree levels for non-hierarchical dimensions
        cleared_count = clear_non_hierarchical_tree_levels()
        
        # Detect data quality issues before processing
        self_refs, orphaned_members = detect_data_quality_issues()
        
        # Calculate tree levels using efficient recursive SQL
        updated_count = calculate_tree_levels_sql()
        
        # Detect circular references (members that couldn't be assigned levels)
        circular_refs = detect_circular_references()
        
        # Validate results
        results = validate_results()
        
        # Final summary
        logger.success(f"✅ Tree level calculation complete: {results['with_levels']:,} members assigned levels")
        
        # Warn about data quality issues
        if self_refs:
            logger.warning(f"⚠️ Found {len(self_refs)} self-referencing members")
        
        if orphaned_members:
            logger.warning(f"⚠️ Found {len(orphaned_members)} orphaned members (parent doesn't exist)")
        
        if circular_refs:
            logger.warning(f"⚠️ Found {len(circular_refs)} potential circular references")
        
        if results['hierarchical_without_levels'] > 0:
            logger.warning(f"⚠️ {results['hierarchical_without_levels']} hierarchical members without levels - check for circular references")
        
        # Log level distribution if significant
        if results['max_level'] and results['max_level'] > 1:
            logger.info(f"📈 Tree depth: {results['distinct_levels']} levels (max depth: {results['max_level']})")
        
    except Exception as e:
        logger.exception(f"❌ Tree level calculation failed: {e}")
        raise

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Statcan Public Data ETL Pipeline
Script: 13_create_dimension_set_members.py
Date: 2025-06-21
Author: Paul Verbrugge with Claude Sonnet 4 (Anthropic)

Create canonical member definitions for each dimension with deduplication and normalization.

This script creates canonical member definitions by joining processed members with their
dimension hashes and selecting the most common member attributes within each dimension.
The result provides normalized member definitions that can be reused across cubes.

Key Operations:
- Populate dimension_hash in processed_members from processed_dimensions
- Group members by (dimension_hash, member_id) for canonical definitions
- Select most common member names using SQL mode calculation
- Preserve hierarchical relationships and unit of measure codes
- Store canonical member definitions in processing.dimension_set_members

Processing Logic:
1. Update processed_members with dimension_hash from processed_dimensions
2. Use SQL aggregation to build canonical member definitions
3. Select most common English/French names using subquery mode calculation
4. Preserve parent_member_id and member_uom_code relationships
5. Calculate usage statistics across cube instances
6. Store canonical definitions with comprehensive validation

Dependencies:
- Requires processing.processed_members from 10_process_dimension_members.py
- Requires processing.processed_dimensions from 11_process_dimension.py
- Outputs to processing.dimension_set_members for canonical member registry
"""

import psycopg2
from loguru import logger
from statcan.tools.config import DB_CONFIG

# Configure logging with minimal approach
logger.add("/app/logs/create_dimension_set_members.log", rotation="5 MB", retention="7 days")

def validate_prerequisites():
    """Validate that prerequisite tables exist and have data"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Check processed_members table
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'processing' AND table_name = 'processed_members'
                )
            """)
            if not cur.fetchone()[0]:
                raise Exception("❌ Source table processing.processed_members does not exist")
            
            cur.execute("SELECT COUNT(*) FROM processing.processed_members")
            member_count = cur.fetchone()[0]
            if member_count == 0:
                raise Exception("❌ No processed members found - run script 10 first")
            
            # Check processed_dimensions table
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'processing' AND table_name = 'processed_dimensions'
                )
            """)
            if not cur.fetchone()[0]:
                raise Exception("❌ Source table processing.processed_dimensions does not exist")
            
            cur.execute("SELECT COUNT(*) FROM processing.processed_dimensions")
            dimension_count = cur.fetchone()[0]
            if dimension_count == 0:
                raise Exception("❌ No processed dimensions found - run script 11 first")
            
            # Check target table
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'processing' AND table_name = 'dimension_set_members'
                )
            """)
            if not cur.fetchone()[0]:
                raise Exception("❌ Target table processing.dimension_set_members does not exist")
            
            return member_count, dimension_count

def populate_dimension_hashes():
    """Populate dimension_hash in processed_members from processed_dimensions"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Update processed_members with dimension_hash from processed_dimensions
            cur.execute("""
                UPDATE processing.processed_members 
                SET dimension_hash = pd.dimension_hash
                FROM processing.processed_dimensions pd
                WHERE processing.processed_members.productid = pd.productid
                  AND processing.processed_members.dimension_position = pd.dimension_position
                  AND processing.processed_members.dimension_hash IS NULL
            """)
            
            updated_rows = cur.rowcount
            conn.commit()
            
            return updated_rows

def build_canonical_members():
    """Build canonical member definitions using efficient PostgreSQL window functions"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Clear existing canonical members
            cur.execute("TRUNCATE TABLE processing.dimension_set_members")
            
            # Step 1: Create temporary table with ranked attributes for efficient mode calculation
            cur.execute("""
                CREATE TEMP TABLE member_rankings AS
                WITH member_attributes AS (
                    SELECT 
                        dimension_hash,
                        member_id,
                        member_name_en,
                        member_name_fr,
                        parent_member_id,
                        member_uom_code,
                        COUNT(*) as attr_count
                    FROM processing.processed_members
                    WHERE dimension_hash IS NOT NULL
                    GROUP BY dimension_hash, member_id, member_name_en, member_name_fr, 
                             parent_member_id, member_uom_code
                ),
                ranked_names_en AS (
                    SELECT 
                        dimension_hash, member_id, member_name_en,
                        ROW_NUMBER() OVER (
                            PARTITION BY dimension_hash, member_id 
                            ORDER BY SUM(attr_count) DESC, member_name_en
                        ) as rank_en
                    FROM member_attributes
                    WHERE member_name_en IS NOT NULL
                    GROUP BY dimension_hash, member_id, member_name_en
                ),
                ranked_names_fr AS (
                    SELECT 
                        dimension_hash, member_id, member_name_fr,
                        ROW_NUMBER() OVER (
                            PARTITION BY dimension_hash, member_id 
                            ORDER BY SUM(attr_count) DESC, member_name_fr
                        ) as rank_fr
                    FROM member_attributes
                    WHERE member_name_fr IS NOT NULL
                    GROUP BY dimension_hash, member_id, member_name_fr
                ),
                ranked_parents AS (
                    SELECT 
                        dimension_hash, member_id, parent_member_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY dimension_hash, member_id 
                            ORDER BY SUM(attr_count) DESC, parent_member_id NULLS LAST
                        ) as rank_parent
                    FROM member_attributes
                    GROUP BY dimension_hash, member_id, parent_member_id
                ),
                ranked_uom AS (
                    SELECT 
                        dimension_hash, member_id, member_uom_code,
                        ROW_NUMBER() OVER (
                            PARTITION BY dimension_hash, member_id 
                            ORDER BY SUM(attr_count) DESC, member_uom_code NULLS LAST
                        ) as rank_uom
                    FROM member_attributes
                    GROUP BY dimension_hash, member_id, member_uom_code
                )
                SELECT 
                    ma.dimension_hash,
                    ma.member_id,
                    ren.member_name_en,
                    rfr.member_name_fr,
                    rp.parent_member_id,
                    ru.member_uom_code,
                    SUM(ma.attr_count) as usage_count
                FROM member_attributes ma
                LEFT JOIN ranked_names_en ren ON ma.dimension_hash = ren.dimension_hash 
                    AND ma.member_id = ren.member_id AND ren.rank_en = 1
                LEFT JOIN ranked_names_fr rfr ON ma.dimension_hash = rfr.dimension_hash 
                    AND ma.member_id = rfr.member_id AND rfr.rank_fr = 1
                LEFT JOIN ranked_parents rp ON ma.dimension_hash = rp.dimension_hash 
                    AND ma.member_id = rp.member_id AND rp.rank_parent = 1
                LEFT JOIN ranked_uom ru ON ma.dimension_hash = ru.dimension_hash 
                    AND ma.member_id = ru.member_id AND ru.rank_uom = 1
                GROUP BY ma.dimension_hash, ma.member_id, ren.member_name_en, 
                         rfr.member_name_fr, rp.parent_member_id, ru.member_uom_code
            """)
            
            # Step 2: Insert canonical members from temp table
            cur.execute("""
                INSERT INTO processing.dimension_set_members (
                    dimension_hash, member_id, member_name_en, member_name_fr,
                    parent_member_id, member_uom_code, usage_count
                )
                SELECT 
                    dimension_hash, member_id, member_name_en, member_name_fr,
                    parent_member_id, member_uom_code, usage_count
                FROM member_rankings
                ORDER BY dimension_hash, member_id
            """)
            
            canonical_count = cur.rowcount
            conn.commit()
            
            return canonical_count

def validate_results():
    """Validate canonical member creation and generate statistics"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Basic counts
            cur.execute("SELECT COUNT(*) FROM processing.dimension_set_members")
            total_members = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(DISTINCT dimension_hash) FROM processing.dimension_set_members")
            unique_dimensions = cur.fetchone()[0]
            
            # Data quality checks
            cur.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE member_name_en IS NULL AND member_name_fr IS NULL) as no_names,
                    COUNT(*) FILTER (WHERE parent_member_id IS NOT NULL) as with_parents,
                    COUNT(*) FILTER (WHERE member_uom_code IS NOT NULL) as with_uom,
                    COUNT(*) FILTER (WHERE usage_count = 0) as zero_usage,
                    MAX(usage_count) as max_usage,
                    AVG(usage_count) as avg_usage
                FROM processing.dimension_set_members
            """)
            no_names, with_parents, with_uom, zero_usage, max_usage, avg_usage = cur.fetchone()
            
            # Calculate hierarchy coverage
            hierarchy_rate = (with_parents / total_members * 100) if total_members > 0 else 0
            
            # Get largest dimensions
            cur.execute("""
                SELECT dimension_hash, COUNT(*) as member_count
                FROM processing.dimension_set_members 
                GROUP BY dimension_hash 
                ORDER BY member_count DESC 
                LIMIT 5
            """)
            largest_dimensions = cur.fetchall()
            
            # Get most used members
            cur.execute("""
                SELECT member_name_en, usage_count 
                FROM processing.dimension_set_members 
                WHERE member_name_en IS NOT NULL
                ORDER BY usage_count DESC 
                LIMIT 5
            """)
            top_members = cur.fetchall()
            
            return {
                'total_members': total_members,
                'unique_dimensions': unique_dimensions,
                'no_names': no_names,
                'with_parents': with_parents,
                'with_uom': with_uom,
                'zero_usage': zero_usage,
                'max_usage': max_usage,
                'avg_usage': float(avg_usage) if avg_usage else 0,
                'hierarchy_rate': hierarchy_rate,
                'largest_dimensions': largest_dimensions,
                'top_members': top_members
            }

def main():
    """Main canonical member creation function"""
    logger.info("🚀 Starting dimension set members creation...")
    
    try:
        # Validate prerequisites
        member_count, dimension_count = validate_prerequisites()
        logger.info(f"📊 Processing {member_count:,} member instances across {dimension_count:,} dimensions...")
        
        # Populate dimension hashes in processed_members
        updated_rows = populate_dimension_hashes()
        if updated_rows > 0:
            logger.info(f"📈 Updated {updated_rows:,} member records with dimension hashes")
        
        # Build canonical members
        canonical_count = build_canonical_members()
        
        # Validate results
        results = validate_results()
        
        # Final summary
        logger.success(f"✅ Member creation complete: {results['total_members']:,} canonical members across {results['unique_dimensions']:,} dimensions")
        
        # Warn about concerning issues
        if results['no_names'] > 0:
            logger.warning(f"⚠️ {results['no_names']} members missing both English and French names")
        
        if results['zero_usage'] > 0:
            logger.warning(f"⚠️ {results['zero_usage']} members with zero usage count")
        
        # Check if we have reasonable hierarchy coverage
        if results['hierarchy_rate'] < 10:
            logger.warning(f"⚠️ Low hierarchy coverage: {results['hierarchy_rate']:.1f}% - check parent relationships")
        
    except Exception as e:
        logger.exception(f"❌ Member creation failed: {e}")
        raise

if __name__ == "__main__":
    main()

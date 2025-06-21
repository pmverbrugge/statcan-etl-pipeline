#!/usr/bin/env python3
"""
Statcan Public Data ETL Pipeline
Script: 12_create_dimension_set.py
Date: 2025-06-21
Author: Paul Verbrugge with Claude Sonnet 4 (Anthropic)

Build canonical dimension registry from processed dimensions with deduplication and normalization.

This script creates the canonical dimension registry by aggregating processed dimensions
by dimension_hash and selecting the most common labels with proper formatting. The result
is a deduplicated set of dimension definitions that can be reused across multiple cubes.

Key Operations:
- Aggregate processed dimensions by dimension_hash for deduplication
- Select most common English/French dimension names using SQL aggregation
- Apply title case formatting and create URL-friendly slugs
- Calculate usage statistics and UOM flag consolidation
- Store canonical definitions in processing.dimension_set

Processing Logic:
1. Load processed dimensions from processing.processed_dimensions
2. Group by dimension_hash and aggregate labels using PostgreSQL functions
3. Apply text normalization (title case) and generate slugs
4. Calculate usage counts and consolidate UOM flags
5. Store canonical dimension definitions with comprehensive validation
6. Generate deduplication statistics and quality metrics

Dependencies:
- Requires processing.processed_dimensions from 11_process_dimension.py
- Outputs to processing.dimension_set for canonical dimension registry
- Feeds into 13_create_dimension_set_members.py for member processing
"""

import psycopg2
from loguru import logger
from statcan.tools.config import DB_CONFIG

# Configure logging with minimal approach
logger.add("/app/logs/create_dimension_set.log", rotation="5 MB", retention="7 days")

def title_case_sql(text):
    """Convert text to title case handling None values"""
    if text is None:
        return None
    return str(text).title()

def create_slug(text):
    """Create URL-friendly slug from text"""
    if text is None:
        return None
    
    # Simple slugification - replace spaces and special chars with underscores
    slug = str(text).lower()
    # Replace common special characters
    replacements = {
        ' ': '_', '-': '_', '(': '', ')': '', '[': '', ']': '',
        '&': 'and', '/': '_', ',': '', '.': '', ':': '', ';': '',
        "'": '', '"': '', '?': '', '!': '', '@': '', '#': '', '$': '',
        '%': '', '^': '', '*': '', '+': '', '=': '', '|': '', '\\': '',
        '<': '', '>': '', '~': '', '`': ''
    }
    
    for char, replacement in replacements.items():
        slug = slug.replace(char, replacement)
    
    # Clean up multiple underscores
    while '__' in slug:
        slug = slug.replace('__', '_')
    
    # Remove leading/trailing underscores
    slug = slug.strip('_')
    
    return slug if slug else None

def validate_prerequisites():
    """Validate that prerequisite tables exist and have data"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
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
                    WHERE table_schema = 'processing' AND table_name = 'dimension_set'
                )
            """)
            if not cur.fetchone()[0]:
                raise Exception("❌ Target table processing.dimension_set does not exist")
            
            return dimension_count

def build_canonical_dimensions():
    """Build canonical dimension definitions using PostgreSQL aggregation"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Clear existing canonical dimensions
            cur.execute("TRUNCATE TABLE processing.dimension_set")
            
            # Build canonical dimensions using SQL aggregation
            # This approach is more efficient than pandas for large datasets
            cur.execute("""
                INSERT INTO processing.dimension_set (
                    dimension_hash, 
                    dimension_name_en, 
                    dimension_name_fr,
                    dimension_name_en_slug,
                    dimension_name_fr_slug,
                    has_uom, 
                    usage_count
                )
                SELECT 
                    dimension_hash,
                    -- Select most common English name (mode)
                    (SELECT dimension_name_en 
                     FROM processing.processed_dimensions pd2 
                     WHERE pd2.dimension_hash = pd1.dimension_hash 
                       AND dimension_name_en IS NOT NULL
                     GROUP BY dimension_name_en 
                     ORDER BY COUNT(*) DESC, dimension_name_en 
                     LIMIT 1) as dimension_name_en,
                    -- Select most common French name (mode)
                    (SELECT dimension_name_fr 
                     FROM processing.processed_dimensions pd2 
                     WHERE pd2.dimension_hash = pd1.dimension_hash 
                       AND dimension_name_fr IS NOT NULL
                     GROUP BY dimension_name_fr 
                     ORDER BY COUNT(*) DESC, dimension_name_fr 
                     LIMIT 1) as dimension_name_fr,
                    -- Slugs will be updated in next step
                    NULL as dimension_name_en_slug,
                    NULL as dimension_name_fr_slug,
                    -- Take maximum UOM flag (TRUE wins over FALSE)
                    COALESCE(BOOL_OR(has_uom), FALSE) as has_uom,
                    -- Count total usage
                    COUNT(*) as usage_count
                FROM processing.processed_dimensions pd1
                GROUP BY dimension_hash
            """)
            
            canonical_count = cur.rowcount
            
            # Update with title case and slugs using Python processing
            # Get all canonical dimensions for slug generation
            cur.execute("""
                SELECT dimension_hash, dimension_name_en, dimension_name_fr
                FROM processing.dimension_set
            """)
            
            dimensions_to_update = cur.fetchall()
            
            # Update each dimension with formatted names and slugs
            for dimension_hash, name_en, name_fr in dimensions_to_update:
                # Apply title case formatting
                formatted_en = title_case_sql(name_en) if name_en else None
                formatted_fr = title_case_sql(name_fr) if name_fr else None
                
                # Generate slugs
                slug_en = create_slug(formatted_en) if formatted_en else None
                slug_fr = create_slug(formatted_fr) if formatted_fr else None
                
                # Update the record
                cur.execute("""
                    UPDATE processing.dimension_set 
                    SET 
                        dimension_name_en = %s,
                        dimension_name_fr = %s,
                        dimension_name_en_slug = %s,
                        dimension_name_fr_slug = %s
                    WHERE dimension_hash = %s
                """, (formatted_en, formatted_fr, slug_en, slug_fr, dimension_hash))
            
            conn.commit()
            return canonical_count

def validate_results():
    """Validate canonical dimension creation and generate statistics"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Basic counts
            cur.execute("SELECT COUNT(*) FROM processing.dimension_set")
            canonical_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM processing.processed_dimensions")
            total_instances = cur.fetchone()[0]
            
            # Data quality checks
            cur.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE dimension_name_en IS NULL AND dimension_name_fr IS NULL) as no_names,
                    COUNT(*) FILTER (WHERE dimension_name_en_slug IS NULL AND dimension_name_fr_slug IS NULL) as no_slugs,
                    COUNT(*) FILTER (WHERE usage_count = 0) as zero_usage,
                    MAX(usage_count) as max_usage,
                    AVG(usage_count) as avg_usage
                FROM processing.dimension_set
            """)
            no_names, no_slugs, zero_usage, max_usage, avg_usage = cur.fetchone()
            
            # Calculate deduplication rate
            dedup_rate = ((total_instances - canonical_count) / total_instances * 100) if total_instances > 0 else 0
            
            # Get top dimensions for validation
            cur.execute("""
                SELECT dimension_name_en, usage_count 
                FROM processing.dimension_set 
                WHERE dimension_name_en IS NOT NULL
                ORDER BY usage_count DESC 
                LIMIT 5
            """)
            top_dimensions = cur.fetchall()
            
            return {
                'canonical_count': canonical_count,
                'total_instances': total_instances,
                'dedup_rate': dedup_rate,
                'no_names': no_names,
                'no_slugs': no_slugs,
                'zero_usage': zero_usage,
                'max_usage': max_usage,
                'avg_usage': float(avg_usage) if avg_usage else 0,
                'top_dimensions': top_dimensions
            }

def main():
    """Main canonical dimension registry building function"""
    logger.info("🚀 Starting canonical dimension registry build...")
    
    try:
        # Validate prerequisites
        dimension_count = validate_prerequisites()
        logger.info(f"📊 Processing {dimension_count:,} dimension instances...")
        
        # Build canonical dimensions
        canonical_count = build_canonical_dimensions()
        
        # Validate results
        results = validate_results()
        
        # Final summary
        logger.success(f"✅ Canonical registry complete: {results['canonical_count']:,} canonical dimensions created")
        
        # Warn about concerning issues
        if results['no_names'] > 0:
            logger.warning(f"⚠️ {results['no_names']} dimensions missing both English and French names")
        
        if results['no_slugs'] > 0:
            logger.warning(f"⚠️ {results['no_slugs']} dimensions missing both slugs")
        
        if results['zero_usage'] > 0:
            logger.warning(f"⚠️ {results['zero_usage']} dimensions with zero usage count")
        
    except Exception as e:
        logger.exception(f"❌ Canonical registry build failed: {e}")
        raise

if __name__ == "__main__":
    main()

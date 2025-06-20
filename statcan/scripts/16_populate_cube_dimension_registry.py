#!/usr/bin/env python3
"""
Statistics Canada Cube Dimension Registry Mapping Populator
==========================================================

Script:     16_populate_cube_dimension_registry.py
Purpose:    Populate cube.cube_dimension_map with productid->dimension_hash mappings
Author:     Paul Verbrugge with Claude Sonnet 4 (Anthropic)
Created:    2025
Updated:    June 2025

Overview:
--------
This script populates the cube.cube_dimension_map table by associating productid and 
dimension_position with the corresponding dimension_hash from processing.dimension_set.
The table serves as the canonical mapping between cubes and their normalized dimensions.

The script includes comprehensive data integrity checks and processes one productid at a time
to ensure consistency and enable partial recovery from failures.

Requires: Scripts 10-15 to have run successfully first.

Key Operations:
--------------
• Load processed dimensions with their dimension_hash values
• Validate that all dimension_positions for each productid have corresponding hashes
• Delete existing mappings for productid before repopulating (update logic)
• Insert new mappings with dimension metadata from canonical registry
• Comprehensive validation and error reporting
• Progress tracking for large datasets

Processing Pipeline:
-------------------
1. Load all processed dimensions from processing.processed_dimensions
2. Validate data completeness for each productid
3. For each productid:
   a. Delete existing mappings
   b. Insert new mappings with dimension metadata
   c. Validate insertion completeness
4. Generate summary statistics and validation reports
5. Report any data integrity issues discovered

Data Integrity Checks:
---------------------
• Verify all dimension_positions for a productid have dimension_hashes
• Check for orphaned mappings (dimension_hash not in canonical registry)
• Validate that dimension metadata is correctly joined
• Ensure no duplicate mappings exist
• Detect missing or extra dimension positions
"""

import pandas as pd
import psycopg2
from loguru import logger
from statcan.tools.config import DB_CONFIG

logger.add("/app/logs/populate_cube_dimension_registry.log", rotation="1 MB", retention="7 days")

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

def check_required_tables():
    """Verify all required tables and columns exist"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        
        required_tables = [
            ('processing', 'processed_dimensions'),
            ('processing', 'dimension_set'),
            ('cube', 'cube_dimension_map')
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
                    "Please ensure all prerequisite scripts have run successfully."
                )
        
        # Verify that processed_dimensions has dimension_hash populated
        cur.execute("""
            SELECT COUNT(*) as total, COUNT(dimension_hash) as with_hash
            FROM processing.processed_dimensions
        """)
        
        result = cur.fetchone()
        total, with_hash = result
        
        if total == 0:
            raise Exception(
                "❌ No data found in processing.processed_dimensions! "
                "Please run scripts 10-11 first."
            )
        
        if with_hash == 0:
            raise Exception(
                "❌ No dimension_hash values found in processing.processed_dimensions! "
                "Please run script 11 first."
            )
        
        if with_hash < total:
            logger.warning(f"⚠️ {total - with_hash} dimension records missing dimension_hash values")
        
        logger.info("✅ All required tables exist and contain data")

def load_processed_dimensions():
    """Load processed dimensions with dimension metadata"""
    with get_db_conn() as conn:
        # Join processed dimensions with canonical dimension set for metadata
        processed_dims = pd.read_sql("""
            SELECT 
                pd.productid,
                pd.dimension_position,
                pd.dimension_hash,
                pd.dimension_name_en,
                pd.dimension_name_fr,
                ds.dimension_name_en_slug
            FROM processing.processed_dimensions pd
            LEFT JOIN processing.dimension_set ds ON pd.dimension_hash = ds.dimension_hash
            WHERE pd.dimension_hash IS NOT NULL
            ORDER BY pd.productid, pd.dimension_position
        """, conn)
        
        logger.info(f"📥 Loaded {len(processed_dims)} processed dimension records")
        
        # Check for orphaned dimension_hashes (not in canonical registry)
        orphaned = processed_dims[processed_dims['dimension_name_en_slug'].isna()]
        if len(orphaned) > 0:
            logger.warning(f"⚠️ Found {len(orphaned)} dimension records with orphaned dimension_hash values")
            # Show a few examples
            for _, row in orphaned.head(3).iterrows():
                logger.warning(f"   • Product {row['productid']}, position {row['dimension_position']}: hash {row['dimension_hash']}")
        
        return processed_dims

def validate_productid_completeness(processed_dims):
    """Validate that all dimension_positions for each productid have dimension_hashes"""
    logger.info("🔍 Validating dimension completeness by productid...")
    
    validation_issues = []
    
    # Group by productid and check for gaps in dimension_position
    productid_groups = processed_dims.groupby('productid')
    
    for productid, group in productid_groups:
        positions = sorted(group['dimension_position'].tolist())
        expected_positions = list(range(1, len(positions) + 1))
        
        # Check for gaps in dimension positions
        if positions != expected_positions:
            validation_issues.append({
                'productid': productid,
                'type': 'position_gaps',
                'expected_positions': expected_positions,
                'actual_positions': positions,
                'message': f"Product {productid} has gaps in dimension positions"
            })
        
        # Check for missing dimension_hashes
        missing_hashes = group[group['dimension_hash'].isna()]
        if len(missing_hashes) > 0:
            validation_issues.append({
                'productid': productid,
                'type': 'missing_hash',
                'missing_positions': missing_hashes['dimension_position'].tolist(),
                'message': f"Product {productid} has {len(missing_hashes)} positions without dimension_hash"
            })
    
    if validation_issues:
        logger.warning(f"⚠️ Found {len(validation_issues)} validation issues")
        # Log first few issues
        for issue in validation_issues[:5]:
            logger.warning(f"   • {issue['message']}")
        if len(validation_issues) > 5:
            logger.warning(f"   • ... and {len(validation_issues) - 5} more issues")
    else:
        logger.success("✅ All productids have complete dimension mappings")
    
    return validation_issues

def populate_cube_dimension_mappings(processed_dims):
    """Populate cube.cube_dimension_map table one productid at a time"""
    logger.info("🚀 Starting cube dimension registry population...")
    
    # Get unique productids to process
    productids = processed_dims['productid'].unique()
    logger.info(f"📊 Processing {len(productids)} unique productids")
    
    successful_updates = 0
    failed_updates = 0
    total_mappings_inserted = 0
    
    with get_db_conn() as conn:
        cur = conn.cursor()
        
        for i, productid in enumerate(productids, 1):
            try:
                # Get dimensions for this productid
                productid_dims = processed_dims[processed_dims['productid'] == productid].copy()
                
                if len(productid_dims) == 0:
                    logger.warning(f"⚠️ No dimensions found for productid {productid}")
                    continue
                
                # Validate this productid has complete data
                missing_hashes = productid_dims[productid_dims['dimension_hash'].isna()]
                if len(missing_hashes) > 0:
                    logger.warning(f"⚠️ Skipping productid {productid}: {len(missing_hashes)} positions missing dimension_hash")
                    failed_updates += 1
                    continue
                
                # Step 1: Delete existing mappings for this productid
                cur.execute("""
                    DELETE FROM cube.cube_dimension_map 
                    WHERE productid = %s
                """, (int(productid),))
                
                deleted_count = cur.rowcount
                
                # Step 2: Insert new mappings
                inserted_count = 0
                for _, row in productid_dims.iterrows():
                    cur.execute("""
                        INSERT INTO cube.cube_dimension_map (
                            productid, dimension_position, dimension_hash,
                            dimension_name_en, dimension_name_fr, dimension_name_slug
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        int(row['productid']),
                        int(row['dimension_position']),
                        row['dimension_hash'],
                        row['dimension_name_en'],
                        row['dimension_name_fr'],
                        row['dimension_name_en_slug']
                    ))
                    inserted_count += 1
                
                # Step 3: Validate insertion
                cur.execute("""
                    SELECT COUNT(*) FROM cube.cube_dimension_map 
                    WHERE productid = %s
                """, (int(productid),))
                
                final_count = cur.fetchone()[0]
                
                if final_count != len(productid_dims):
                    logger.warning(f"⚠️ Validation failed for productid {productid}: "
                                   f"expected {len(productid_dims)}, got {final_count}")
                    failed_updates += 1
                else:
                    successful_updates += 1
                    total_mappings_inserted += inserted_count
                
                # Commit after each productid
                conn.commit()
                
                # Progress logging
                if i % 100 == 0 or i == len(productids):
                    logger.info(f"📈 Progress: {i}/{len(productids)} productids processed "
                               f"({successful_updates} successful, {failed_updates} failed)")
                
            except Exception as e:
                logger.error(f"❌ Failed to process productid {productid}: {e}")
                failed_updates += 1
                conn.rollback()
                continue
    
    logger.success(f"✅ Cube dimension registry population complete!")
    logger.success(f"   • {successful_updates:,} productids successfully updated")
    logger.success(f"   • {total_mappings_inserted:,} total dimension mappings inserted")
    
    if failed_updates > 0:
        logger.warning(f"⚠️ {failed_updates} productids failed to update")
    
    return successful_updates, failed_updates, total_mappings_inserted

def validate_final_mappings():
    """Validate the final cube dimension mappings"""
    logger.info("🔍 Validating final cube dimension mappings...")
    
    with get_db_conn() as conn:
        # Overall statistics
        stats = pd.read_sql("""
            SELECT 
                COUNT(*) as total_mappings,
                COUNT(DISTINCT productid) as unique_productids,
                COUNT(DISTINCT dimension_hash) as unique_dimensions,
                COUNT(DISTINCT (productid, dimension_position)) as unique_positions
            FROM cube.cube_dimension_map
        """, conn)
        
        row = stats.iloc[0]
        logger.info(f"📊 Final statistics:")
        logger.info(f"   • {row['total_mappings']:,} total dimension mappings")
        logger.info(f"   • {row['unique_productids']:,} unique productids")
        logger.info(f"   • {row['unique_dimensions']:,} unique dimension_hashes")
        logger.info(f"   • {row['unique_positions']:,} unique (productid, dimension_position) pairs")
        
        # Check for duplicate mappings (should not exist due to primary key)
        duplicates = pd.read_sql("""
            SELECT productid, dimension_position, COUNT(*) as count
            FROM cube.cube_dimension_map
            GROUP BY productid, dimension_position
            HAVING COUNT(*) > 1
        """, conn)
        
        if len(duplicates) > 0:
            logger.error(f"❌ Found {len(duplicates)} duplicate mappings!")
            for _, dup in duplicates.head(5).iterrows():
                logger.error(f"   • Product {dup['productid']}, position {dup['dimension_position']}: {dup['count']} entries")
        else:
            logger.success("✅ No duplicate mappings found")
        
        # Check for orphaned dimension_hashes
        orphaned = pd.read_sql("""
            SELECT DISTINCT cdm.dimension_hash
            FROM cube.cube_dimension_map cdm
            LEFT JOIN processing.dimension_set ds ON cdm.dimension_hash = ds.dimension_hash
            WHERE ds.dimension_hash IS NULL
        """, conn)
        
        if len(orphaned) > 0:
            logger.warning(f"⚠️ Found {len(orphaned)} orphaned dimension_hashes in mappings")
        else:
            logger.success("✅ All dimension_hashes have corresponding canonical definitions")
        
        # Most common dimensions
        top_dimensions = pd.read_sql("""
            SELECT 
                cdm.dimension_hash,
                cdm.dimension_name_en,
                COUNT(*) as usage_count
            FROM cube.cube_dimension_map cdm
            GROUP BY cdm.dimension_hash, cdm.dimension_name_en
            ORDER BY usage_count DESC
            LIMIT 5
        """, conn)
        
        logger.info("🏆 Top 5 most used dimensions:")
        for _, dim in top_dimensions.iterrows():
            logger.info(f"   • {dim['dimension_name_en']}: {dim['usage_count']:,} uses")

def generate_summary_report():
    """Generate a comprehensive summary report"""
    logger.info("📋 Generating summary report...")
    
    with get_db_conn() as conn:
        # Coverage analysis
        coverage = pd.read_sql("""
            SELECT 
                'Processing Dimensions' as source,
                COUNT(DISTINCT productid) as productids,
                COUNT(*) as dimension_count
            FROM processing.processed_dimensions
            WHERE dimension_hash IS NOT NULL
            
            UNION ALL
            
            SELECT 
                'Cube Registry' as source,
                COUNT(DISTINCT productid) as productids,
                COUNT(*) as dimension_count
            FROM cube.cube_dimension_map
        """, conn)
        
        logger.info("📈 Coverage Summary:")
        for _, row in coverage.iterrows():
            logger.info(f"   • {row['source']}: {row['productids']:,} productids, {row['dimension_count']:,} dimensions")
        
        # Check for missing productids
        missing_products = pd.read_sql("""
            SELECT DISTINCT pd.productid
            FROM processing.processed_dimensions pd
            WHERE pd.dimension_hash IS NOT NULL
            AND pd.productid NOT IN (
                SELECT DISTINCT productid FROM cube.cube_dimension_map
            )
            LIMIT 10
        """, conn)
        
        if len(missing_products) > 0:
            logger.warning(f"⚠️ {len(missing_products)} productids from processing not in registry")
            if len(missing_products) <= 10:
                logger.warning(f"   Missing: {missing_products['productid'].tolist()}")
        else:
            logger.success("✅ All processed productids are represented in the registry")

def main():
    """Main cube dimension registry population function"""
    try:
        logger.info("🚀 Starting cube dimension registry mapping population...")
        
        # Verify prerequisites
        check_required_tables()
        
        # Load processed dimensions
        processed_dims = load_processed_dimensions()
        
        # Validate data completeness
        validation_issues = validate_productid_completeness(processed_dims)
        
        if validation_issues:
            logger.warning(f"⚠️ Proceeding with {len(validation_issues)} validation issues")
        
        # Populate cube dimension mappings
        successful, failed, total_inserted = populate_cube_dimension_mappings(processed_dims)
        
        # Validate final results
        validate_final_mappings()
        
        # Generate summary report
        generate_summary_report()
        
        logger.success("🎉 Cube dimension registry population complete!")
        logger.info(f"📝 Registry table: cube.cube_dimension_map")
        logger.info(f"📈 Total mappings: {total_inserted:,}")
        
        if failed > 0:
            logger.warning(f"⚠️ Note: {failed} productids failed to update - check logs for details")
        
    except Exception as e:
        logger.exception(f"❌ Cube dimension registry population failed: {e}")
        raise

if __name__ == "__main__":
    main()

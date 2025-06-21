"""
Statcan Public Data ETL Pipeline
Script: 03_cube_status_init.py
Date: 2025-06-21
Author: Paul Verbrugge with Claude Sonnet 4 (Anthropic)

Initialize cube_status table with entries for product IDs from spine.cube
that don't already have download tracking records. Sets download_pending = TRUE
for new entries to flag them for initial download.
"""

import psycopg2
from loguru import logger
from statcan.tools.config import DB_CONFIG

# Add file logging
logger.add("/app/logs/populate_cube_status.log", rotation="1 MB", retention="7 days")

# Validation constants
MIN_SPINE_CUBES = 1000  # Expect at least 1000 cubes in spine
MIN_PRODUCT_ID = 10000000  # StatCan uses 8-digit product IDs
MAX_PRODUCT_ID = 99999999


def validate_prerequisites():
    """Validate database connectivity and required tables exist"""
    logger.info("📋 Validating prerequisites...")
    
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Check spine.cube table exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'spine' AND table_name = 'cube'
                    )
                """)
                if not cur.fetchone()[0]:
                    raise RuntimeError("❌ spine.cube table does not exist")
                
                # Check raw_files.cube_status table exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'raw_files' AND table_name = 'cube_status'
                    )
                """)
                if not cur.fetchone()[0]:
                    raise RuntimeError("❌ raw_files.cube_status table does not exist")
                
        logger.success("✅ Prerequisites validation passed")
        
    except Exception as e:
        logger.error(f"❌ Prerequisites validation failed: {e}")
        raise


def validate_spine_integrity():
    """Validate spine.cube table has reasonable data before processing"""
    logger.info("🔍 Validating spine data integrity...")
    
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Check if spine.cube table has data
                cur.execute("SELECT COUNT(*) FROM spine.cube")
                cube_count = cur.fetchone()[0]
                
                if cube_count == 0:
                    raise ValueError("❌ spine.cube table is empty - cannot initialize cube_status")
                
                if cube_count < MIN_SPINE_CUBES:
                    raise ValueError(f"❌ Too few cubes in spine: {cube_count} < {MIN_SPINE_CUBES}")
                
                # Check for NULL product IDs
                cur.execute("SELECT COUNT(*) FROM spine.cube WHERE productid IS NULL")
                null_ids = cur.fetchone()[0]
                if null_ids > 0:
                    raise ValueError(f"❌ {null_ids} cubes have NULL product IDs in spine")
                
                # Check product ID format (should be 8-digit numbers)
                cur.execute("""
                    SELECT COUNT(*) FROM spine.cube 
                    WHERE productid < %s OR productid > %s
                """, (MIN_PRODUCT_ID, MAX_PRODUCT_ID))
                invalid_ids = cur.fetchone()[0]
                
                if invalid_ids > 0:
                    logger.warning(f"⚠️  {invalid_ids} cubes have non-standard product ID format")
                    if invalid_ids > cube_count * 0.1:  # More than 10% is suspicious
                        raise ValueError(f"❌ Too many invalid product IDs: {invalid_ids}/{cube_count}")
                
                # Check for duplicate product IDs
                cur.execute("""
                    SELECT COUNT(*) - COUNT(DISTINCT productid) FROM spine.cube
                """)
                duplicates = cur.fetchone()[0]
                if duplicates > 0:
                    raise ValueError(f"❌ {duplicates} duplicate product IDs found in spine")
                
                # Get additional statistics
                cur.execute("SELECT COUNT(*) FROM spine.cube WHERE archived = 1")
                archived_count = cur.fetchone()[0]
                
                cur.execute("SELECT MIN(productid), MAX(productid) FROM spine.cube")
                min_id, max_id = cur.fetchone()
                
                stats = {
                    'total_cubes': cube_count,
                    'archived_cubes': archived_count,
                    'active_cubes': cube_count - archived_count,
                    'min_product_id': min_id,
                    'max_product_id': max_id,
                    'invalid_ids': invalid_ids
                }
                
                logger.success("✅ Spine data validation passed")
                logger.info(f"📊 Spine stats: {cube_count} total, {archived_count} archived, {cube_count - archived_count} active")
                logger.info(f"🔢 Product ID range: {min_id} to {max_id}")
                
                return stats
                
    except Exception as e:
        logger.error(f"❌ Spine validation failed: {e}")
        raise


def get_existing_status_stats():
    """Get current cube_status table statistics"""
    logger.info("📊 Analyzing existing cube_status data...")
    
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM raw_files.cube_status")
                existing_count = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM raw_files.cube_status WHERE download_pending = TRUE")
                pending_count = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM raw_files.cube_status WHERE last_download IS NOT NULL")
                downloaded_count = cur.fetchone()[0]
                
                stats = {
                    'existing_count': existing_count,
                    'pending_count': pending_count,
                    'downloaded_count': downloaded_count,
                    'never_downloaded': existing_count - downloaded_count
                }
                
                logger.info(f"📈 Current status: {existing_count} tracked, {pending_count} pending, {downloaded_count} downloaded")
                return stats
                
    except Exception as e:
        logger.error(f"❌ Failed to get cube_status statistics: {e}")
        raise


def identify_missing_cubes():
    """Find product IDs in spine that are missing from cube_status"""
    logger.info("🔍 Identifying cubes missing from status tracking...")
    
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT c.productid
                    FROM spine.cube c
                    LEFT JOIN raw_files.cube_status cs ON c.productid = cs.productid
                    WHERE cs.productid IS NULL
                    ORDER BY c.productid
                """)
                
                missing_cubes = [row[0] for row in cur.fetchall()]
                logger.info(f"📋 Found {len(missing_cubes)} cubes missing from cube_status")
                
                if missing_cubes:
                    # Log some examples for verification
                    sample_size = min(5, len(missing_cubes))
                    logger.info(f"📝 Sample missing product IDs: {missing_cubes[:sample_size]}")
                    
                    # Check if we're missing a reasonable number
                    cur.execute("SELECT COUNT(*) FROM spine.cube")
                    total_cubes = cur.fetchone()[0]
                    missing_ratio = len(missing_cubes) / total_cubes
                    
                    if missing_ratio > 0.5:  # More than 50% missing is suspicious for existing pipeline
                        logger.warning(f"⚠️  Large number of missing cubes: {missing_ratio:.1%} of total")
                
                return missing_cubes
                
    except Exception as e:
        logger.error(f"❌ Failed to identify missing cubes: {e}")
        raise


def insert_missing_cubes(missing_cubes):
    """Insert missing cube entries into cube_status table"""
    if not missing_cubes:
        logger.info("ℹ️  No missing cubes to insert")
        return 0
    
    logger.info(f"📥 Inserting {len(missing_cubes)} new cube_status entries...")
    
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Use INSERT with explicit conflict handling for safety
                insert_sql = """
                    INSERT INTO raw_files.cube_status (productid, download_pending)
                    SELECT unnest(%s::bigint[]), TRUE
                    ON CONFLICT (productid) DO NOTHING
                """
                
                cur.execute(insert_sql, (missing_cubes,))
                inserted_count = cur.rowcount
                
                if inserted_count != len(missing_cubes):
                    logger.warning(f"⚠️  Expected to insert {len(missing_cubes)} but inserted {inserted_count}")
                    logger.info("ℹ️  Some cubes may have been added concurrently")
                
                conn.commit()
                logger.success(f"✅ Successfully inserted {inserted_count} cube_status entries")
                return inserted_count
                
    except Exception as e:
        logger.error(f"❌ Failed to insert cube_status entries: {e}")
        raise


def validate_post_insert(spine_stats, pre_insert_stats, inserted_count):
    """Validate the cube_status table after insertion"""
    logger.info("🔍 Validating post-insertion state...")
    
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Get updated statistics
                cur.execute("SELECT COUNT(*) FROM raw_files.cube_status")
                final_count = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM raw_files.cube_status WHERE download_pending = TRUE")
                final_pending = cur.fetchone()[0]
                
                # Verify counts make sense
                expected_count = pre_insert_stats['existing_count'] + inserted_count
                if final_count != expected_count:
                    logger.warning(f"⚠️  Unexpected final count: {final_count} vs expected {expected_count}")
                
                # Check that we now track all spine cubes
                cur.execute("""
                    SELECT COUNT(*) FROM spine.cube c
                    LEFT JOIN raw_files.cube_status cs ON c.productid = cs.productid
                    WHERE cs.productid IS NULL
                """)
                still_missing = cur.fetchone()[0]
                
                if still_missing > 0:
                    logger.error(f"❌ Still missing {still_missing} cubes after insertion!")
                    raise ValueError(f"Post-insertion validation failed: {still_missing} cubes still missing")
                
                logger.success("✅ Post-insertion validation passed")
                logger.info(f"📊 Final stats: {final_count} total tracked, {final_pending} pending download")
                
    except Exception as e:
        logger.error(f"❌ Post-insertion validation failed: {e}")
        raise


def main():
    logger.info("🚀 Starting enhanced cube_status initialization...")
    
    try:
        # Validate prerequisites
        validate_prerequisites()
        
        # Validate spine data integrity
        spine_stats = validate_spine_integrity()
        
        # Get current cube_status statistics
        pre_insert_stats = get_existing_status_stats()
        
        # Find cubes missing from cube_status
        missing_cubes = identify_missing_cubes()
        
        if not missing_cubes:
            logger.success("✅ All spine cubes already tracked in cube_status")
            return
        
        # Insert missing cubes
        inserted_count = insert_missing_cubes(missing_cubes)
        
        if inserted_count > 0:
            # Validate final state
            validate_post_insert(spine_stats, pre_insert_stats, inserted_count)
            
            logger.success(f"✅ Successfully initialized {inserted_count} new cube_status entries")
            
            # Log summary
            logger.info("📋 Summary:")
            logger.info(f"   Spine cubes: {spine_stats['total_cubes']}")
            logger.info(f"   Previously tracked: {pre_insert_stats['existing_count']}")
            logger.info(f"   Newly added: {inserted_count}")
            logger.info(f"   Total now tracked: {pre_insert_stats['existing_count'] + inserted_count}")
        else:
            logger.info("ℹ️  No new entries were needed")
            
        logger.success("🎯 SCRIPT 03 COMPLETE")

    except Exception as e:
        logger.error(f"❌ SCRIPT 03 FAILED: {e}")
        raise


if __name__ == "__main__":
    main()

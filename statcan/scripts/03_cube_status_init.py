"""
Enhanced Cube Status Initialization Script - Statistics Canada ETL Pipeline
===========================================================================

This script initializes the cube_status table with entries for all product IDs
present in the spine.cube table that don't already have download tracking records.
It implements safety checks to ensure spine data integrity before creating
download tracking entries.

Key Features:
- Identifies new product IDs from validated spine data
- Creates download tracking entries with download_pending = TRUE
- Validates spine data completeness before processing
- Implements safety checks against corrupted spine data
- Preserves existing cube_status records (no updates to existing entries)

Process Flow:
1. Validate spine.cube table has reasonable data
2. Identify product IDs missing from cube_status table
3. Insert new entries with download_pending = TRUE flag
4. Log summary of changes for audit trail

Protection Mechanisms:
- Pre-flight validation of spine data integrity
- Sanity checks on product ID formats and counts
- Safe INSERT with conflict handling (no overwrites)
- Detailed logging for monitoring and debugging

Use Cases:
- Initial pipeline setup (populate cube_status for first time)
- Recovery after spine updates (catch new cubes for download)
- Regular maintenance (ensure all cubes are tracked)

Dependencies:
- Requires validated spine.cube table from 02_spine_load_to_db.py
- Uses raw_files.cube_status table for download tracking

Last Updated: June 2025
Author: Paul Verbrugge with Claude 3.5 Sonnet (v20241022)
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


def validate_spine_integrity(cur) -> dict:
    """Validate spine.cube table has reasonable data before processing"""
    logger.info("🔍 Validating spine data integrity...")
    
    # Check if spine.cube table exists and has data
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


def get_existing_status_stats(cur) -> dict:
    """Get current cube_status table statistics"""
    logger.info("📊 Analyzing existing cube_status data...")
    
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


def identify_missing_cubes(cur) -> list:
    """Find product IDs in spine that are missing from cube_status"""
    logger.info("🔍 Identifying cubes missing from status tracking...")
    
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


def insert_missing_cubes(cur, missing_cubes: list) -> int:
    """Insert missing cube entries into cube_status table"""
    if not missing_cubes:
        logger.info("ℹ️  No missing cubes to insert")
        return 0
    
    logger.info(f"📥 Inserting {len(missing_cubes)} new cube_status entries...")
    
    # Use INSERT with explicit conflict handling for safety
    insert_sql = """
        INSERT INTO raw_files.cube_status (productid, download_pending)
        SELECT unnest(%s::bigint[]), TRUE
        ON CONFLICT (productid) DO NOTHING
    """
    
    try:
        cur.execute(insert_sql, (missing_cubes,))
        inserted_count = cur.rowcount
        
        if inserted_count != len(missing_cubes):
            logger.warning(f"⚠️  Expected to insert {len(missing_cubes)} but inserted {inserted_count}")
            logger.info("ℹ️  Some cubes may have been added concurrently")
        
        logger.success(f"✅ Successfully inserted {inserted_count} cube_status entries")
        return inserted_count
        
    except Exception as e:
        logger.error(f"❌ Failed to insert cube_status entries: {e}")
        raise


def validate_post_insert(cur, spine_stats: dict, pre_insert_stats: dict, inserted_count: int):
    """Validate the cube_status table after insertion"""
    logger.info("🔍 Validating post-insertion state...")
    
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


def main():
    logger.info("🚀 Starting enhanced cube_status initialization...")
    
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Validate spine data integrity first
                spine_stats = validate_spine_integrity(cur)
                
                # Get current cube_status statistics
                pre_insert_stats = get_existing_status_stats(cur)
                
                # Find cubes missing from cube_status
                missing_cubes = identify_missing_cubes(cur)
                
                if not missing_cubes:
                    logger.success("✅ All spine cubes already tracked in cube_status")
                    return
                
                # Insert missing cubes
                inserted_count = insert_missing_cubes(cur, missing_cubes)
                
                if inserted_count > 0:
                    # Validate final state
                    validate_post_insert(cur, spine_stats, pre_insert_stats, inserted_count)
                    
                    # Commit changes
                    conn.commit()
                    logger.success(f"✅ Successfully initialized {inserted_count} new cube_status entries")
                    
                    # Log summary
                    logger.info("📋 Summary:")
                    logger.info(f"   Spine cubes: {spine_stats['total_cubes']}")
                    logger.info(f"   Previously tracked: {pre_insert_stats['existing_count']}")
                    logger.info(f"   Newly added: {inserted_count}")
                    logger.info(f"   Total now tracked: {pre_insert_stats['existing_count'] + inserted_count}")
                else:
                    logger.info("ℹ️  No new entries were needed")

    except Exception as e:
        logger.exception(f"❌ Cube status initialization failed: {e}")
        raise


if __name__ == "__main__":
    main()

"""
Enhanced Cube Status Update Script - Statistics Canada ETL Pipeline
====================================================================

This script monitors StatCan data cubes for updates by querying the getChangedCubeList API
endpoint for each day since the last checked date. It implements comprehensive API response
validation and safety checks to ensure reliable change detection and download flagging.

Key Features:
- Tracks change detection progress via changed_cubes_log table
- Queries StatCan API for changed cubes on each unchecked day
- Validates API responses for completeness and sanity
- Records all changes with deduplication handling
- Updates cube_status.download_pending for cubes needing re-download
- Handles StatCan's 8:30 AM EST release schedule

Process Flow:
1. Determine last checked date from change log
2. Calculate effective StatCan date (accounting for release time)
3. For each unchecked date: query API, validate response, record changes
4. Update cube_status flags based on accumulated change log
5. Provide summary statistics and audit trail

Protection Mechanisms:
- API response validation (structure, status codes, data quality)
- Graceful handling of individual date failures
- Rate limiting and timeout protection
- Sanity checks on change volumes and patterns
- Atomic database operations with rollback capability

API Behavior:
- getChangedCubeList(date) returns cubes that changed on specific date
- Response includes responseStatusCode (0 = success, others = various errors)
- Empty responses are normal (no changes on that date)
- API failures are logged but don't stop overall process

Dependencies:
- Requires spine.cube table for product ID validation
- Uses raw_files.changed_cubes_log for change tracking
- Updates raw_files.cube_status for download management

Last Updated: June 2025
Author: Paul Verbrugge
"""

from datetime import date, datetime, time, timedelta, timezone
import requests
import psycopg2
from psycopg2.extras import execute_values
from statcan.tools.config import DB_CONFIG
from loguru import logger
from time import sleep 

# Add file logging
logger.add("/app/logs/update_cube_status.log", rotation="10 MB", retention="7 days")

# API and safety constants
WDS_CHANGE_URL = "https://www150.statcan.gc.ca/t1/wds/rest/getChangedCubeList/{}"
SLEEP_SECONDS = 2
NO_CHANGES_MARKER = -1  # Special productid to mark dates with no changes
API_TIMEOUT = 30  # seconds
MAX_CHANGES_PER_DAY = 1000  # Sanity check - more than this seems suspicious
MIN_PRODUCT_ID = 10000000  # StatCan 8-digit product IDs
MAX_PRODUCT_ID = 99999999


def validate_change_tracking_setup(cur) -> dict:
    """Validate that change tracking tables are properly set up"""
    logger.info("🔍 Validating change tracking setup...")
    
    # Check that changed_cubes_log table exists and is accessible
    try:
        cur.execute("SELECT COUNT(*) FROM raw_files.changed_cubes_log")
        log_count = cur.fetchone()[0]
    except Exception as e:
        raise RuntimeError(f"❌ Cannot access changed_cubes_log table: {e}")
    
    # Check that cube_status table exists and is accessible
    try:
        cur.execute("SELECT COUNT(*) FROM raw_files.cube_status")
        status_count = cur.fetchone()[0]
    except Exception as e:
        raise RuntimeError(f"❌ Cannot access cube_status table: {e}")
    
    # Get some basic statistics
    cur.execute("SELECT MIN(change_date), MAX(change_date) FROM raw_files.changed_cubes_log WHERE productid != %s", (NO_CHANGES_MARKER,))
    date_range = cur.fetchone()
    min_date, max_date = date_range if date_range[0] else (None, None)
    
    stats = {
        'log_entries': log_count,
        'status_entries': status_count,
        'min_change_date': min_date,
        'max_change_date': max_date
    }
    
    logger.success("✅ Change tracking setup validated")
    logger.info(f"📊 Change log: {log_count} entries, Status tracking: {status_count} cubes")
    if min_date and max_date:
        logger.info(f"📅 Change date range: {min_date} to {max_date}")
    
    return stats


def get_last_checked_date(cur) -> date:
    """Get the most recent date we've checked for changes in the log"""
    cur.execute("SELECT MAX(change_date) FROM raw_files.changed_cubes_log")
    result = cur.fetchone()[0]
    
    if result is None:
        # If no changes have been logged, start from a reasonable baseline
        baseline_date = date(2024, 1, 1)
        logger.info(f"ℹ️  No previous change checks found, starting from {baseline_date}")
        return baseline_date
    
    logger.info(f"📅 Last checked date found in log: {result}")
    return result


def date_already_checked(cur, check_date: date) -> bool:
    """Check if we've already processed this date"""
    cur.execute("""
        SELECT 1 FROM raw_files.changed_cubes_log 
        WHERE change_date = %s 
        LIMIT 1
    """, (check_date,))
    result = cur.fetchone() is not None
    if result:
        logger.debug(f"📅 Date {check_date} already processed")
    return result


def validate_api_response(response_data: dict, check_date: date) -> bool:
    """Validate API response structure and content"""
    logger.debug(f"🔍 Validating API response for {check_date}")
    
    # Check basic response structure
    if not isinstance(response_data, dict):
        logger.error(f"❌ API response is not a dictionary: {type(response_data)}")
        return False
    
    if 'status' not in response_data:
        logger.error("❌ API response missing 'status' field")
        return False
    
    if response_data['status'] != 'SUCCESS':
        logger.warning(f"⚠️  API returned non-success status: {response_data.get('status')}")
        return False
    
    # Get the object array
    objects = response_data.get('object', [])
    if not isinstance(objects, list):
        logger.error(f"❌ API response 'object' is not a list: {type(objects)}")
        return False
    
    # Validate individual change records
    valid_changes = 0
    for i, entry in enumerate(objects):
        if not isinstance(entry, dict):
            logger.warning(f"⚠️  Change entry {i} is not a dictionary")
            continue
            
        # Check required fields
        if 'productId' not in entry or 'responseStatusCode' not in entry:
            logger.warning(f"⚠️  Change entry {i} missing required fields")
            continue
            
        # Validate product ID
        try:
            product_id = int(entry['productId'])
            if not (MIN_PRODUCT_ID <= product_id <= MAX_PRODUCT_ID):
                logger.warning(f"⚠️  Invalid product ID format: {product_id}")
                continue
        except (ValueError, TypeError):
            logger.warning(f"⚠️  Non-numeric product ID: {entry.get('productId')}")
            continue
            
        # Check response status code (0 = success)
        status_code = entry.get('responseStatusCode')
        if status_code != 0:
            logger.debug(f"ℹ️  Entry {i} has non-zero status code: {status_code}")
            continue  # This is normal - just means the entry has some issue
            
        valid_changes += 1
    
    # Sanity check on change volume
    if valid_changes > MAX_CHANGES_PER_DAY:
        logger.warning(f"⚠️  Unusually high change count: {valid_changes} for {check_date}")
        logger.warning("⚠️  Proceeding but this may indicate API issues")
    
    logger.debug(f"✅ Validated {valid_changes} valid changes for {check_date}")
    return True


def fetch_changed_cubes(for_date: date) -> list[tuple[int, date]]:
    """Fetch and validate changed cubes for a specific date from StatCan API"""
    url = WDS_CHANGE_URL.format(for_date.isoformat())
    logger.info(f"🔍 Checking changes for {for_date}")
    
    try:
        resp = requests.get(url, timeout=API_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        
        # Validate response before processing
        if not validate_api_response(data, for_date):
            logger.error(f"❌ Invalid API response for {for_date}")
            return []
        
        # Extract valid changes (only status code 0)
        changes = []
        objects = data.get("object", [])
        
        for entry in objects:
            try:
                product_id = int(entry["productId"])
                status_code = entry.get("responseStatusCode", -1)
                
                if status_code == 0:  # Only include successful entries
                    changes.append((product_id, for_date))
                else:
                    logger.debug(f"ℹ️  Skipping product {product_id} with status code {status_code}")
                    
            except (ValueError, TypeError, KeyError) as e:
                logger.warning(f"⚠️  Skipping invalid change entry: {e}")
                continue
        
        logger.info(f"📅 {for_date}: Found {len(changes)} valid changed cubes")
        return changes
        
    except requests.exceptions.Timeout:
        logger.error(f"❌ API timeout for {for_date} after {API_TIMEOUT} seconds")
        return []
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ API request failed for {for_date}: {e}")
        return []
    except Exception as e:
        logger.error(f"❌ Unexpected error fetching changes for {for_date}: {e}")
        return []


def record_changes_for_date(cur, for_date: date, changes: list[tuple[int, date]]):
    """Record changes for a date, including a marker for dates with no changes"""
    try:
        if changes:
            # Record actual changes with conflict handling
            execute_values(cur, """
                INSERT INTO raw_files.changed_cubes_log (productid, change_date)
                VALUES %s
                ON CONFLICT (productid, change_date) DO NOTHING;
            """, changes)
            logger.info(f"📝 Recorded {len(changes)} changes for {for_date}")
        else:
            # Record a marker for dates with no changes
            cur.execute("""
                INSERT INTO raw_files.changed_cubes_log (productid, change_date)
                VALUES (%s, %s)
                ON CONFLICT (productid, change_date) DO NOTHING;
            """, (NO_CHANGES_MARKER, for_date))
            logger.info(f"📝 Recorded no-changes marker for {for_date}")
            
    except Exception as e:
        logger.error(f"❌ Failed to record changes for {for_date}: {e}")
        raise


def count_pending_updates(cur) -> int:
    """Count cubes that need downloading based on logged changes"""
    cur.execute("""
        SELECT COUNT(DISTINCT log.productid)
        FROM raw_files.changed_cubes_log log
        JOIN raw_files.cube_status cs ON log.productid = cs.productid
        WHERE log.change_date > COALESCE(cs.last_download::date, '2020-01-01'::date)
          AND log.productid != %s  -- Exclude no-changes markers
    """, (NO_CHANGES_MARKER,))
    return cur.fetchone()[0]


def update_cube_status_from_log(cur):
    """Update cube_status based on all logged changes"""
    logger.info("🔄 Updating cube download status from change log...")
    
    try:
        cur.execute("""
            UPDATE raw_files.cube_status
            SET download_pending = TRUE
            WHERE productid IN (
                SELECT DISTINCT log.productid
                FROM raw_files.changed_cubes_log log
                JOIN raw_files.cube_status cs ON log.productid = cs.productid
                WHERE log.change_date > COALESCE(cs.last_download::date, '2020-01-01'::date)
                  AND log.productid != %s  -- Exclude no-changes markers
            );
        """, (NO_CHANGES_MARKER,))
        
        updated_count = cur.rowcount
        logger.success(f"✅ Marked {updated_count} cubes as pending download")
        return updated_count
        
    except Exception as e:
        logger.error(f"❌ Failed to update cube status: {e}")
        raise


def get_effective_statcan_date() -> date:
    """
    StatCan releases data at 8:30 AM EST (13:30 UTC).
    If it's before 13:30 UTC, we consider yesterday as the effective date.
    """
    now = datetime.now(timezone.utc)
    cutoff = time(13, 30)  # 08:30 EST == 13:30 UTC
    
    if now.time() >= cutoff:
        effective_date = now.date()
    else:
        effective_date = now.date() - timedelta(days=1)
    
    logger.info(f"🕐 Current UTC time: {now.strftime('%H:%M')}, effective StatCan date: {effective_date}")
    return effective_date


def main():
    logger.info("🚀 Starting enhanced change detection...")
    
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Validate setup
                setup_stats = validate_change_tracking_setup(cur)
                
                # Determine date range to check
                last_checked = get_last_checked_date(cur)
                today = get_effective_statcan_date()

                logger.info(f"📅 Last checked date: {last_checked}")
                logger.info(f"📅 Checking through: {today}")

                if last_checked >= today:
                    logger.success("✅ Already up to date - no new dates to check")
                else:
                    # Process each unchecked date
                    start_date = last_checked + timedelta(days=1)
                    current_date = start_date
                    
                    total_days = (today - start_date).days + 1
                    logger.info(f"🗓️ Checking {total_days} days from {start_date} to {today}")

                    processed_days = 0
                    failed_days = 0
                    total_changes = 0
                    
                    while current_date <= today:
                        day_number = processed_days + 1
                        
                        # Skip if already checked (safety check)
                        if date_already_checked(cur, current_date):
                            logger.info(f"⏭️  Day {day_number}/{total_days}: {current_date} - already checked, skipping")
                            current_date += timedelta(days=1)
                            continue
                        
                        try:
                            # Fetch and record changes for this date
                            changes = fetch_changed_cubes(current_date)
                            record_changes_for_date(cur, current_date, changes)
                            
                            total_changes += len(changes)
                            processed_days += 1
                            
                            logger.success(f"✅ Day {day_number}/{total_days}: {current_date} - completed ({len(changes)} changes)")
                            
                            # Commit each day's changes
                            conn.commit()
                            
                            # Rate limiting
                            if current_date < today:  # Don't sleep after last request
                                sleep(SLEEP_SECONDS)
                                
                        except Exception as e:
                            failed_days += 1
                            logger.error(f"❌ Day {day_number}/{total_days}: Failed to process {current_date}: {e}")
                            conn.rollback()
                        
                        current_date += timedelta(days=1)
                    
                    logger.info(f"📊 Processing summary: {processed_days} successful, {failed_days} failed, {total_changes} total changes")

                # Update cube status based on all logged changes
                updated_cubes = update_cube_status_from_log(cur)
                conn.commit()
                
                # Final summary
                pending_count = count_pending_updates(cur)
                logger.success(f"✅ Enhanced change detection complete")
                logger.info(f"📋 Summary: {updated_cubes} cubes flagged, {pending_count} total pending download")

    except Exception as e:
        logger.exception(f"❌ Enhanced change detection failed: {e}")
        raise


if __name__ == "__main__":
    main()

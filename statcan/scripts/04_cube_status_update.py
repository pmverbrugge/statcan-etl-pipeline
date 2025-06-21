"""
Statcan Public Data ETL Project - 04_cube_status_update.py
Script: Cube Status Update - Change Detection and Download Status Management
Date: June 21, 2025
Author: Paul Verbrugge with Claude Sonnet 4

PURPOSE:
    Monitors Statistics Canada data cubes for updates by querying the getChangedCubeList API
    endpoint for each day since the last checked date. Maintains comprehensive change logs
    and updates download pending status for modified cubes.

DEPENDENCIES:
    - PostgreSQL database with raw_files schema
    - Internet connectivity for StatCan API access
    - Proper API rate limiting compliance (2 second delays)

PROCESSING LOGIC:
    1. Determine last checked date from changed_cubes_log table
    2. Calculate effective StatCan date based on 8:30 AM EST release schedule
    3. Query API for each unchecked date between last checked and today
    4. Record all changes with deduplication and no-change markers
    5. Update cube_status.download_pending for cubes requiring updates
    6. Handle API failures gracefully without stopping entire process

OUTPUTS:
    - Updated raw_files.changed_cubes_log table with all detected changes
    - Updated raw_files.cube_status table with download_pending flags
    - Comprehensive logging of all operations and API responses

PERFORMANCE NOTES:
    - Respects StatCan API rate limits with 2-second delays between requests
    - Uses atomic transactions for each date to prevent partial failures
    - Idempotent design allows safe re-execution without data duplication
    - Efficient bulk operations for database updates
"""

from datetime import date, datetime, time, timedelta, timezone
import requests
import psycopg2
from psycopg2.extras import execute_values
from statcan.tools.config import DB_CONFIG
from loguru import logger
import sys
from time import sleep
from typing import List, Tuple, Optional

# Configure logging with rotation and retention
logger.add("/app/logs/04_cube_status_update.log", rotation="10 MB", retention="7 days")

# Constants
WDS_CHANGE_URL = "https://www150.statcan.gc.ca/t1/wds/rest/getChangedCubeList/{}"
SLEEP_SECONDS = 2
NO_CHANGES_MARKER = -1  # Special productid to mark dates with no changes
STATCAN_RELEASE_TIME_UTC = time(13, 30)  # 08:30 EST == 13:30 UTC
DEFAULT_START_DATE = date(2024, 1, 1)


def validate_prerequisites() -> bool:
    """
    Validate all prerequisites before processing begins.
    
    Returns:
        bool: True if all prerequisites are met, False otherwise
    """
    logger.info("📋 Validating prerequisites...")
    
    # Test database connectivity
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Verify required schemas exist
                cur.execute("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name = 'raw_files'
                """)
                if not cur.fetchone():
                    logger.error("❌ Required schema 'raw_files' not found")
                    return False
                
                # Verify required tables exist
                required_tables = ['changed_cubes_log', 'cube_status']
                for table in required_tables:
                    cur.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'raw_files' 
                        AND table_name = %s
                    """, (table,))
                    if not cur.fetchone():
                        logger.error(f"❌ Required table 'raw_files.{table}' not found")
                        return False
                
                logger.info("✅ Database connectivity and schema validation passed")
                
    except Exception as e:
        logger.error(f"❌ Database validation failed: {e}")
        return False
    
    # Test API connectivity (warn but don't fail - API may be temporarily down)
    try:
        test_date = date(2024, 1, 1)
        test_url = WDS_CHANGE_URL.format(test_date.isoformat())
        response = requests.get(test_url, timeout=10)
        response.raise_for_status()
        
        # Verify response structure
        data = response.json()
        if 'object' not in data:
            logger.warning("⚠️ API response missing expected 'object' field - may indicate API changes")
        else:
            logger.info("✅ StatCan API connectivity validation passed")
        
    except Exception as e:
        logger.warning(f"⚠️ API validation failed: {e}")
        logger.warning("⚠️ API may be temporarily unavailable - proceeding anyway")
        logger.warning("⚠️ Script will fail gracefully if API is actually down during processing")
    
    logger.info("✅ Prerequisites validation complete")
    return True


def get_last_checked_date(cur) -> date:
    """
    Get the most recent date we've checked for changes in the log.
    
    Args:
        cur: Database cursor
        
    Returns:
        date: The last date checked, or default start date if none found
    """
    cur.execute("SELECT MAX(change_date) FROM raw_files.changed_cubes_log")
    result = cur.fetchone()[0]
    
    if result is None:
        logger.info(f"🎯 No previous change checks found, starting from {DEFAULT_START_DATE}")
        return DEFAULT_START_DATE
    
    logger.info(f"📅 Last checked date found in log: {result}")
    return result


def date_already_checked(cur, check_date: date) -> bool:
    """
    Check if we've already processed changes for a specific date.
    
    Args:
        cur: Database cursor
        check_date: Date to check
        
    Returns:
        bool: True if date has been processed, False otherwise
    """
    cur.execute("""
        SELECT 1 FROM raw_files.changed_cubes_log 
        WHERE change_date = %s 
        LIMIT 1
    """, (check_date,))
    return cur.fetchone() is not None


def count_pending_updates(cur) -> int:
    """
    Count cubes that need downloading based on logged changes.
    
    Args:
        cur: Database cursor
        
    Returns:
        int: Number of cubes pending download
    """
    cur.execute("""
        SELECT COUNT(DISTINCT log.productid)
        FROM raw_files.changed_cubes_log log
        JOIN raw_files.cube_status cs ON log.productid = cs.productid
        WHERE log.change_date > COALESCE(cs.last_download::date, '2020-01-01'::date)
          AND log.productid != %s  -- Exclude no-changes markers
    """, (NO_CHANGES_MARKER,))
    result = cur.fetchone()
    return result[0] if result else 0


def fetch_changed_cubes(for_date: date) -> List[Tuple[int, date]]:
    """
    Fetch changed cubes for a specific date from StatCan API.
    
    Args:
        for_date: Date to check for changes
        
    Returns:
        List[Tuple[int, date]]: List of (productid, change_date) tuples
        
    Raises:
        requests.RequestException: If API request fails
        ValueError: If API response is invalid
    """
    url = WDS_CHANGE_URL.format(for_date.isoformat())
    logger.info(f"🔍 Checking changes for {for_date}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Validate response structure
        if 'object' not in data:
            raise ValueError(f"API response missing 'object' field for date {for_date}")
        
        # Extract valid changes
        changes = []
        for entry in data.get("object", []):
            if not isinstance(entry, dict):
                logger.warning(f"⚠️ Skipping invalid entry (not dict): {entry}")
                continue
                
            # Validate required fields
            if 'productId' not in entry or 'responseStatusCode' not in entry:
                logger.warning(f"⚠️ Skipping entry missing required fields: {entry}")
                continue
            
            # Only include successful responses
            if entry.get("responseStatusCode") == 0:
                try:
                    product_id = int(entry["productId"])
                    changes.append((product_id, for_date))
                except (ValueError, TypeError) as e:
                    logger.warning(f"⚠️ Skipping invalid productId '{entry['productId']}': {e}")
                    continue
        
        logger.info(f"📅 {for_date}: Found {len(changes)} changed cubes")
        return changes
        
    except requests.RequestException as e:
        logger.error(f"❌ API request failed for {for_date}: {e}")
        raise
    except (ValueError, KeyError) as e:
        logger.error(f"❌ Invalid API response for {for_date}: {e}")
        raise


def record_changes_for_date(cur, for_date: date, changes: List[Tuple[int, date]]) -> None:
    """
    Record changes for a date, including a marker for dates with no changes.
    
    Args:
        cur: Database cursor
        for_date: Date being processed
        changes: List of (productid, change_date) tuples
    """
    if changes:
        # Record actual changes with deduplication
        execute_values(cur, """
            INSERT INTO raw_files.changed_cubes_log (productid, change_date)
            VALUES %s
            ON CONFLICT (productid, change_date) DO NOTHING
        """, changes)
        logger.info(f"📝 Recorded {len(changes)} changes for {for_date}")
    else:
        # Record a marker for dates with no changes
        cur.execute("""
            INSERT INTO raw_files.changed_cubes_log (productid, change_date)
            VALUES (%s, %s)
            ON CONFLICT (productid, change_date) DO NOTHING
        """, (NO_CHANGES_MARKER, for_date))
        logger.info(f"📝 Recorded no-changes marker for {for_date}")


def update_cube_status_from_log(cur) -> int:
    """
    Update cube_status.download_pending based on all logged changes.
    
    Args:
        cur: Database cursor
        
    Returns:
        int: Number of cubes marked as pending download
    """
    cur.execute("""
        UPDATE raw_files.cube_status
        SET download_pending = TRUE
        WHERE productid IN (
            SELECT DISTINCT log.productid
            FROM raw_files.changed_cubes_log log
            JOIN raw_files.cube_status cs ON log.productid = cs.productid
            WHERE log.change_date > COALESCE(cs.last_download::date, '2020-01-01'::date)
              AND log.productid != %s  -- Exclude no-changes markers
        )
    """, (NO_CHANGES_MARKER,))
    
    updated_count = cur.rowcount
    logger.info(f"🔄 Marked {updated_count} cubes as pending download")
    return updated_count


def get_effective_statcan_date() -> date:
    """
    Calculate effective StatCan date based on 8:30 AM EST release schedule.
    
    StatCan releases data at 8:30 AM EST (13:30 UTC).
    If current time is before 13:30 UTC, consider yesterday as the effective date.
    
    Returns:
        date: Effective StatCan date for checking changes
    """
    now = datetime.now(timezone.utc)
    
    if now.time() >= STATCAN_RELEASE_TIME_UTC:
        effective_date = now.date()
    else:
        effective_date = now.date() - timedelta(days=1)
    
    logger.info(f"🕐 Current UTC time: {now.strftime('%H:%M')}, effective StatCan date: {effective_date}")
    return effective_date


def validate_processing_results(cur, start_date: date, end_date: date) -> bool:
    """
    Validate processing results after completion.
    
    Args:
        cur: Database cursor
        start_date: First date that was processed
        end_date: Last date that was processed
        
    Returns:
        bool: True if validation passes, False otherwise
    """
    logger.info("🔍 Validating processing results...")
    
    try:
        # Check that all dates in range have been recorded
        cur.execute("""
            SELECT COUNT(DISTINCT change_date)
            FROM raw_files.changed_cubes_log
            WHERE change_date BETWEEN %s AND %s
        """, (start_date, end_date))
        
        recorded_dates = cur.fetchone()[0]
        expected_dates = (end_date - start_date).days + 1
        
        if recorded_dates != expected_dates:
            logger.error(f"❌ Date coverage mismatch: recorded {recorded_dates}, expected {expected_dates}")
            return False
        
        # Check for any obvious data integrity issues
        cur.execute("""
            SELECT COUNT(*) FROM raw_files.changed_cubes_log
            WHERE productid IS NULL OR change_date IS NULL
        """)
        
        null_records = cur.fetchone()[0]
        if null_records > 0:
            logger.error(f"❌ Found {null_records} records with NULL values")
            return False
        
        # Verify cube_status updates are reasonable
        cur.execute("""
            SELECT COUNT(*) FROM raw_files.cube_status
            WHERE download_pending = TRUE
        """)
        
        pending_count = cur.fetchone()[0]
        logger.info(f"📊 Final state: {pending_count} cubes marked as pending download")
        
        logger.info("✅ Processing results validation passed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Validation failed: {e}")
        return False


def main():
    """Main execution function with comprehensive error handling and validation."""
    logger.info("🎯 SCRIPT 04: Cube Status Update - Starting")
    
    try:
        # Validate prerequisites
        if not validate_prerequisites():
            logger.error("❌ Prerequisites validation failed")
            sys.exit(1)
        
        # Main processing
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Determine date range to process
                last_checked = get_last_checked_date(cur)
                today = get_effective_statcan_date()

                logger.info(f"📅 Last checked date: {last_checked}")
                logger.info(f"📅 Checking through: {today}")

                # Check if we're already up to date
                if last_checked >= today:
                    logger.info("✅ Already up to date - no new dates to check")
                    pending_count = count_pending_updates(cur)
                    logger.info(f"📊 Current status: {pending_count} cubes pending download")
                    logger.success("✅ SCRIPT 04 COMPLETE - No updates needed")
                    return

                # Calculate date range to check
                start_date = last_checked + timedelta(days=1)
                total_days = (today - start_date).days + 1
                logger.info(f"🗓️ Processing {total_days} days from {start_date} to {today}")

                # Process each date
                current_date = start_date
                day_count = 0
                successful_days = 0
                failed_days = 0

                while current_date <= today:
                    day_count += 1
                    
                    # Skip if already checked (safety check)
                    if date_already_checked(cur, current_date):
                        logger.info(f"⏭️  Day {day_count}/{total_days}: {current_date} - already checked, skipping")
                        current_date += timedelta(days=1)
                        successful_days += 1
                        continue
                    
                    # Process this date within a transaction
                    try:
                        logger.info(f"📅 Day {day_count}/{total_days}: Processing {current_date}")
                        
                        # Fetch changes for this date
                        changes = fetch_changed_cubes(current_date)
                        
                        # Record changes in database
                        record_changes_for_date(cur, current_date, changes)
                        
                        # Commit this date's changes
                        conn.commit()
                        successful_days += 1
                        logger.info(f"✅ Day {day_count}/{total_days}: {current_date} - completed successfully")
                        
                        # Rate limiting
                        sleep(SLEEP_SECONDS)
                        
                    except requests.RequestException as e:
                        # Handle specific HTTP error codes
                        if hasattr(e, 'response') and e.response is not None:
                            if e.response.status_code == 409:
                                # 409 = data not yet available (normal for current day)
                                logger.info(f"📅 Day {day_count}/{total_days}: {current_date} - data not yet available (409), recording no-change marker")
                                record_changes_for_date(cur, current_date, [])
                                conn.commit()
                                successful_days += 1
                                logger.info(f"✅ Day {day_count}/{total_days}: {current_date} - completed (no data available)")
                            else:
                                # Other HTTP errors are actual failures
                                conn.rollback()
                                failed_days += 1
                                logger.warning(f"⚠️ Day {day_count}/{total_days}: Failed to process {current_date}: {e}")
                        else:
                            # Network/connection errors are failures
                            conn.rollback()
                            failed_days += 1
                            logger.warning(f"⚠️ Day {day_count}/{total_days}: Failed to process {current_date}: {e}")
                    except Exception as e:
                        # All other errors are failures
                        conn.rollback()
                        failed_days += 1
                        logger.warning(f"⚠️ Day {day_count}/{total_days}: Failed to process {current_date}: {e}")
                    
                    current_date += timedelta(days=1)

                # Update cube status based on all logged changes
                logger.info("🔄 Updating cube download status from change log...")
                updated_count = update_cube_status_from_log(cur)
                conn.commit()
                
                # Validate results (only if we processed some dates successfully)
                if successful_days > 0:
                    if not validate_processing_results(cur, start_date, today):
                        logger.warning("⚠️ Processing results validation had issues")
                        # Don't exit on validation failure if we had some successes
                        if failed_days > 0:
                            logger.warning("⚠️ Some API failures occurred - validation issues may be expected")
                        else:
                            logger.error("❌ Validation failed with no API failures - this may indicate a problem")
                            sys.exit(1)
                
                # Final summary
                pending_count = count_pending_updates(cur)
                logger.info(f"📊 Processing summary:")
                logger.info(f"   • Total days processed: {day_count}")
                logger.info(f"   • Successful days: {successful_days}")
                logger.info(f"   • Failed days: {failed_days}")
                logger.info(f"   • Cubes marked for download: {updated_count}")
                logger.info(f"   • Total cubes pending download: {pending_count}")
                
                if failed_days > 0:
                    logger.warning(f"⚠️ Completed with {failed_days} failed days - may need retry")
                
                logger.success("✅ SCRIPT 04 COMPLETE - Cube status update finished")

    except Exception as e:
        logger.exception(f"❌ SCRIPT 04 FAILED: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

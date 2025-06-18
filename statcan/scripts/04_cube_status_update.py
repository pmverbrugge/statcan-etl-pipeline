"""
StatCan Cube Change Detection and Status Update Script

This script monitors StatCan data cubes for updates by querying the getChangedCubeList API
endpoint for each day since the last checked date. It uses the changed_cubes_log table
as the authoritative record of which dates have been checked for changes.

Key Functions:
- Finds the most recent date checked in changed_cubes_log table
- Queries StatCan API for changed cubes on each unchecked day since then
- Records all changes in changed_cubes_log table (with deduplication)
- Records "no changes" dates with a special marker record (productid = -1)
- Updates cube_status.download_pending = TRUE for cubes with recent changes
- Accounts for StatCan's 8:30 AM EST release schedule in date calculations

The script is idempotent and can be run multiple times safely. It respects
StatCan's API with delays between requests and handles individual date failures
gracefully without stopping the entire process.

Last Updated: June 2025
"""

from datetime import date, datetime, time, timedelta, timezone
import requests
import psycopg2
from psycopg2.extras import execute_values
from statcan.tools.config import DB_CONFIG
from loguru import logger
from time import sleep 

logger.add("/app/logs/update_cube_status.log", rotation="10 MB", retention="7 days")

WDS_CHANGE_URL = "https://www150.statcan.gc.ca/t1/wds/rest/getChangedCubeList/{}"
SLEEP_SECONDS = 2
NO_CHANGES_MARKER = -1  # Special productid to mark dates with no changes

def get_last_checked_date(cur) -> date:
    """Get the most recent date we've checked for changes in the log"""
    cur.execute("SELECT MAX(change_date) FROM raw_files.changed_cubes_log")
    result = cur.fetchone()[0]
    
    if result is None:
        # If no changes have been logged, start from a reasonable baseline
        logger.info("No previous change checks found, starting from 2024-01-01")
        return date(2024, 1, 1)
    
    logger.info(f"Last checked date found in log: {result}")
    return result

def date_already_checked(cur, check_date: date) -> bool:
    """Check if we've already processed this date"""
    cur.execute("""
        SELECT 1 FROM raw_files.changed_cubes_log 
        WHERE change_date = %s 
        LIMIT 1
    """, (check_date,))
    return cur.fetchone() is not None

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

def fetch_changed_cubes(for_date: date) -> list[tuple[int, date]]:
    """Fetch changed cubes for a specific date from StatCan API"""
    url = WDS_CHANGE_URL.format(for_date.isoformat())
    logger.info(f"🔍 Checking changes for {for_date}")
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    
    changes = [
        (entry["productId"], for_date)
        for entry in data.get("object", [])
        if entry.get("responseStatusCode") == 0
    ]
    
    logger.info(f"📅 {for_date}: Found {len(changes)} changed cubes")
    return changes

def record_changes_for_date(cur, for_date: date, changes: list[tuple[int, date]]):
    """Record changes for a date, including a marker for dates with no changes"""
    if changes:
        # Record actual changes
        execute_values(cur, """
            INSERT INTO raw_files.changed_cubes_log (productid, change_date)
            VALUES %s
            ON CONFLICT DO NOTHING;
        """, changes)
        logger.info(f"📝 Recorded {len(changes)} changes for {for_date}")
    else:
        # Record a marker for dates with no changes
        cur.execute("""
            INSERT INTO raw_files.changed_cubes_log (productid, change_date)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING;
        """, (NO_CHANGES_MARKER, for_date))
        logger.info(f"📝 Recorded no-changes marker for {for_date}")

def update_cube_status_from_log(cur):
    """Update cube_status based on all logged changes"""
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
    logger.info(f"🔄 Marked {updated_count} cubes as pending download")

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
    logger.info("🚦 Starting change detection...")
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                last_checked = get_last_checked_date(cur)
                today = get_effective_statcan_date()

                logger.info(f"📅 Last checked date: {last_checked}")
                logger.info(f"📅 Checking through: {today}")

                # Calculate date range to check
                if last_checked >= today:
                    logger.info("✅ Already up to date - no new dates to check")
                else:
                    # Check each unchecked date from (last_checked + 1) through today
                    start_date = last_checked + timedelta(days=1)
                    current_date = start_date
                    
                    total_days = (today - start_date).days + 1
                    logger.info(f"🗓️ Checking {total_days} days from {start_date} to {today}")

                    day_count = 0
                    while current_date <= today:
                        day_count += 1
                        
                        # Skip if already checked (safety check)
                        if date_already_checked(cur, current_date):
                            logger.info(f"⏭️  Day {day_count}/{total_days}: {current_date} - already checked, skipping")
                            current_date += timedelta(days=1)
                            continue
                        
                        try:
                            changes = fetch_changed_cubes(current_date)
                            record_changes_for_date(cur, current_date, changes)
                            logger.info(f"✅ Day {day_count}/{total_days}: {current_date} - completed")
                            conn.commit()
                            sleep(SLEEP_SECONDS)
                        except Exception as e:
                            logger.warning(f"⚠️ Failed to process {current_date}: {e}")
                            conn.rollback()
                        
                        current_date += timedelta(days=1)

                # Update cube status based on all logged changes
                logger.info("🔄 Updating cube download status from change log...")
                update_cube_status_from_log(cur)
                conn.commit()
                
                # Final summary
                pending_count = count_pending_updates(cur)
                logger.success(f"✅ Change detection complete. {pending_count} cubes now pending download")

    except Exception as e:
        logger.exception(f"❌ Change detection failed: {e}")

if __name__ == "__main__":
    main()

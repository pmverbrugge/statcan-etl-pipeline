"""
StatCan Cube Change Detection and Status Update Script

This script monitors StatCan data cubes for updates by querying the getChangedCubeList API
endpoint for each day since the last successful check. It maintains a complete log of
cube changes and flags cubes for download when they've been updated.

Key Functions:
- Finds the most recent successful download date from cube_status table
- Queries StatCan API for changed cubes on each day since then
- Records all changes in changed_cubes_log table (with deduplication)
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

def get_last_checked_date(cur) -> date:
    """Get the most recent date we've successfully checked for changes"""
    # Changed from MIN to MAX - we want the most recent check, not the oldest
    cur.execute("SELECT MAX(last_download::date) FROM raw_files.cube_status WHERE last_download IS NOT NULL")
    result = cur.fetchone()[0]
    
    if result is None:
        # If no downloads have happened, start from a reasonable baseline
        logger.info("No previous downloads found, starting from 2024-01-01")
        return date(2024, 1, 1)
    
    logger.info(f"Last successful download was on: {result}")
    return result

def count_pending_updates(cur) -> int:
    cur.execute("""
        SELECT COUNT(DISTINCT log.productid)
        FROM raw_files.changed_cubes_log log
        JOIN raw_files.cube_status cs ON log.productid = cs.productid
        WHERE log.change_date > cs.last_download::date;
    """)
    return cur.fetchone()[0]

def fetch_changed_cubes(for_date: date) -> list[tuple[int, date]]:
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

def insert_changes(cur, changes: list[tuple[int, date]]):
    if not changes:
        return
    execute_values(cur, """
        INSERT INTO raw_files.changed_cubes_log (productid, change_date)
        VALUES %s
        ON CONFLICT DO NOTHING;
    """, changes)

def update_cube_status(cur):
    cur.execute("""
        UPDATE raw_files.cube_status
        SET download_pending = TRUE
        WHERE productid IN (
            SELECT DISTINCT log.productid
            FROM raw_files.changed_cubes_log log
            JOIN raw_files.cube_status cs ON log.productid = cs.productid
            WHERE log.change_date > COALESCE(cs.last_download::date, '2020-01-01'::date)
        );
    """)
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
    logger.info("🚦 Starting update checker...")
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
                    return

                total_pending = count_pending_updates(cur)
                logger.info(f"🧮 {total_pending} cubes currently pending based on change history")

                # Check each date from (last_checked + 1) through today
                start_date = last_checked + timedelta(days=1)
                current_date = start_date
                
                total_days = (today - start_date).days + 1
                logger.info(f"🗓️ Checking {total_days} days from {start_date} to {today}")

                day_count = 0
                while current_date <= today:
                    day_count += 1
                    try:
                        changes = fetch_changed_cubes(current_date)
                        insert_changes(cur, changes)
                        logger.info(f"📝 Day {day_count}/{total_days}: {current_date} - {len(changes)} changes recorded")
                        conn.commit()
                        sleep(SLEEP_SECONDS)
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to process {current_date}: {e}")
                        conn.rollback()
                    
                    current_date += timedelta(days=1)

                # Update cube status based on all recorded changes
                update_cube_status(cur)
                conn.commit()
                
                # Final summary
                final_pending = count_pending_updates(cur)
                logger.success(f"✅ Update complete. {final_pending} cubes now pending download")

    except Exception as e:
        logger.exception(f"❌ Update checker failed: {e}")

if __name__ == "__main__":
    main()

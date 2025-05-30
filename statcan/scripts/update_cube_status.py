from datetime import date, timedelta
import requests
import psycopg2
from psycopg2.extras import execute_values
from statcan.tools.config import DB_CONFIG
from loguru import logger
import time

logger.add("/app/logs/update_cube_status.log", rotation="10 MB", retention="7 days")

WDS_CHANGE_URL = "https://www150.statcan.gc.ca/t1/wds/rest/getChangedCubeList/{}"
SLEEP_SECONDS = 2

def get_last_checked_date(cur) -> date:
    cur.execute("SELECT MIN(last_download::date) FROM raw_files.cube_status WHERE last_download IS NOT NULL")
    result = cur.fetchone()[0]
    return result if result else date(2020, 1, 1)

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
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    return [
        (entry["productId"], for_date)
        for entry in data.get("object", [])
        if entry.get("responseStatusCode") == 0
    ]

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
            WHERE log.change_date > cs.last_download::date
        );
    """)

def main():
    logger.info("🚦 Starting update checker...")
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                last_checked = get_last_checked_date(cur)
                today = date.today()

                logger.info(f"📅 Last checked date: {last_checked}, checking through {today}.")

                total_pending = count_pending_updates(cur)
                logger.info(f"🧮 {total_pending} cubes may need updating based on change history.")

                for d in (last_checked + timedelta(days=i) for i in range(1, (today - last_checked).days + 1)):
                    try:
                        changes = fetch_changed_cubes(d)
                        insert_changes(cur, changes)
                        logger.info(f"📅 {d}: {len(changes)} changes recorded.")
                        conn.commit()
                        time.sleep(SLEEP_SECONDS)
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to process {d}: {e}")
                        conn.rollback()

                update_cube_status(cur)
                conn.commit()
                logger.success("✅ Cube status updated with pending downloads.")

    except Exception as e:
        logger.exception(f"❌ Update checker failed: {e}")

if __name__ == "__main__":
    main()


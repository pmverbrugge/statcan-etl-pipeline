# populate_cube_status.py

import psycopg2
from loguru import logger
from statcan.tools.config import DB_CONFIG

logger.add("/app/logs/populate_cube_status.log", rotation="1 MB", retention="7 days")

INSERT_SQL = """
INSERT INTO raw_files.cube_status (productid, download_pending)
SELECT c.productid, TRUE
FROM spine.cube c
LEFT JOIN raw_files.cube_status cs ON c.productid = cs.productid
WHERE cs.productid IS NULL;
"""

def main():
    logger.info("🟢 Starting cube_status population...")
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(INSERT_SQL)
                inserted = cur.rowcount
                conn.commit()
                logger.success(f"✅ Inserted {inserted} missing productid(s) into cube_status.")
    except Exception as e:
        logger.exception(f"❌ Failed to populate cube_status: {e}")

if __name__ == "__main__":
    main()


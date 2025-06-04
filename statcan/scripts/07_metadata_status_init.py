import psycopg2
from loguru import logger
from statcan.tools.config import DB_CONFIG

logger.add("/app/logs/populate_metadata_status.log", rotation="1 MB", retention="7 days")

INSERT_SQL = """
INSERT INTO raw_files.metadata_status (productid, download_pending)
SELECT c.productid, TRUE
FROM spine.cube c
LEFT JOIN raw_files.metadata_status ms ON c.productid = ms.productid
WHERE ms.productid IS NULL;
"""

def main():
    logger.info("🟢 Starting metadata_status population...")
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(INSERT_SQL)
                inserted = cur.rowcount
                conn.commit()
                logger.success(f"✅ Inserted {inserted} missing productid(s) into metadata_status.")
    except Exception as e:
        logger.exception(f"❌ Failed to populate metadata_status: {e}")

if __name__ == "__main__":
    main()


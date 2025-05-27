"""
bulk_sdmx_ingester.py

Downloads and archives all StatCan cubes listed in spine.cube
using the getFullTableDownloadSDMX endpoint.

- Handles duplicates (skips if already archived)
- Logs success, duplicate, not found, skipped, and error statuses
- Throttles requests to avoid overwhelming the API
- Can safely resume ingestion by checking archive.ingest_log
- Writes human-readable log to logs/bulk_ingest.log
"""

import os
import time
import requests
import psycopg2
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from lib.archive_ingester import ingest_file_from_bytes

load_dotenv()

DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRESS", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

SDMX_ENDPOINT = "https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadSDMX/{}"
DELAY_SECONDS = 5  # More conservative delay to avoid hitting StatCan too fast
MAX_BYTES = 800 * 1024 * 1024  # 800 MB safety buffer

# Setup file + console logging
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, 'bulk_ingest.log')

logger = logging.getLogger("bulk_ingest")
logger.setLevel(logging.INFO)

fh = RotatingFileHandler(log_path, maxBytes=5*1024*1024, backupCount=3)
fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(message)s'))
logger.addHandler(ch)

def log_result(cur, productid, status, notes=None, file_hash=None):
    cur.execute("""
        INSERT INTO archive.ingest_log (productid, status, notes, file_hash)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (productid) DO UPDATE
        SET status = EXCLUDED.status,
            notes = EXCLUDED.notes,
            attempt_time = now(),
            file_hash = EXCLUDED.file_hash
    """, (productid, status, notes, file_hash))

def main():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    
    with conn.cursor() as cur:
        cur.execute("""
            SELECT productid FROM spine.cube
            WHERE productid NOT IN (
                SELECT productid FROM archive.ingest_log
                WHERE status IN ('success', 'duplicate')
            )
            ORDER BY productid
        """)
        rows = cur.fetchall()
        product_ids = [r[0] for r in rows]

    logger.info(f"Found {len(product_ids)} cubes to ingest.")

    for productid in product_ids:
        logger.info(f"\n🔄 Ingesting product ID: {productid}")
        try:
            resp = requests.get(SDMX_ENDPOINT.format(productid))
            resp.raise_for_status()
            result = resp.json()

            if not isinstance(result, dict) or "object" not in result:
                with conn.cursor() as cur:
                    log_result(cur, productid, "not_found", notes="Missing 'object' in response")
                logger.warning(f"⚠️ No download URL for {productid}")
                continue

            file_url = result["object"]
            file_resp = requests.get(file_url)
            file_resp.raise_for_status()
            file_bytes = file_resp.content

            if len(file_bytes) >= MAX_BYTES:
                with conn.cursor() as cur:
                    log_result(cur, productid, "skipped", notes=f"File too large: {len(file_bytes)} bytes")
                logger.warning(f"🚫 Skipping {productid}.sdmx.zip: file too large ({len(file_bytes)} bytes)")
                conn.commit()
                continue

            file_name = f"{productid}.sdmx.zip"
            content_type = "application/zip"

            success = ingest_file_from_bytes(
                file_bytes,
                file_name,
                conn,
                source_url=file_url,
                content_type=content_type
            )

            file_hash = None
            if success:
                file_hash = __import__('hashlib').sha256(file_bytes).hexdigest()
                status = "success"
                logger.info(f"✅ Ingested: {file_name} ({len(file_bytes)} bytes)")
            else:
                status = "duplicate"
                logger.info(f"🔁 Duplicate: {file_name} already ingested")

            with conn.cursor() as cur:
                log_result(cur, productid, status, file_hash=file_hash)

        except Exception as e:
            try:
                with conn.cursor() as cur:
                    log_result(cur, productid, "error", notes=str(e))
                conn.commit()
            except Exception as log_err:
                logger.error(f"⚠️ Failed to log error for {productid}: {log_err}")
            logger.error(f"❌ Error on {productid}: {e}")

        conn.commit()
        time.sleep(DELAY_SECONDS)

    conn.close()
    logger.info("\n✅ Ingestion complete.")

if __name__ == "__main__":
    main()


"""
Spine ETL Script
Last updated: 2025-05-29

This script loads the most recent StatCan spine metadata file from disk,
stages it in DuckDB, and inserts structured data into PostgreSQL spine tables.
"""

import os
import json
import tempfile
import duckdb
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from loguru import logger
from statcan.tools.config import DB_CONFIG

# Constants
RAW_DIR = "/app/raw/statcan/metadata"
con = duckdb.connect(":memory:")

def get_active_file_path() -> str:
    """Fetch the file path of the active spine metadata file from Postgres."""
    query = """
        SELECT storage_location
        FROM raw_files.manage_spine_raw_files
        WHERE active = true
        ORDER BY date_download DESC
        LIMIT 1
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()
            if not result:
                raise RuntimeError("No active spine metadata file found.")
            return result[0]

def normalize_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Convert datetime columns to string format for PostgreSQL insertion."""
    df = df.replace({pd.NaT: None, "NaT": None})
    for col in df.select_dtypes(include=["datetime64[ns]"]):
        df[col] = df[col].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if pd.notnull(x) else None)
    return df

def stage_data(file_path: str) -> str:
    """Read the JSON metadata into DuckDB and stage views for target tables."""
    con.sql("INSTALL json; LOAD json;")

    with open(file_path, "rb") as f:
        json_bytes = f.read()

    with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as tmp:
        tmp.write(json_bytes.decode("utf-8"))
        tmp.flush()
        json_path = tmp.name

    con.execute(f"CREATE OR REPLACE VIEW base_cube AS SELECT * FROM read_json_auto('{json_path}')")

    con.sql("""
        CREATE OR REPLACE VIEW cube AS 
        SELECT DISTINCT
            productId, cansimId, cubeTitleEn, cubeTitleFr,
            CAST(cubeStartDate AS DATE) AS cubeStartDate,
            CAST(cubeEndDate AS DATE) AS cubeEndDate,
            CAST(releaseTime AS DATE) AS releaseTime,
            CAST(archived AS SMALLINT) AS archived,
            CAST(frequencyCode AS SMALLINT) AS frequencyCode,
            CAST(issueDate AS DATE) AS issueDate
        FROM base_cube;
    """)

    con.sql("CREATE OR REPLACE VIEW cube_subject AS SELECT productId, UNNEST(subjectCode) AS subjectCode FROM base_cube;")
    con.sql("CREATE OR REPLACE VIEW cube_survey AS SELECT productId, UNNEST(surveyCode) AS surveyCode FROM base_cube;")

    logger.info("✅ DuckDB views created.")
    return json_path

def insert_into_postgres():
    """Insert the staged data into the spine schema in PostgreSQL."""
    target_tables = ["cube", "cube_subject", "cube_survey"]

    with psycopg2.connect(**DB_CONFIG) as pg:
        with pg.cursor() as cur:
            cur.execute("TRUNCATE TABLE spine.cube, spine.cube_subject, spine.cube_survey")
            logger.info("🔁 Truncated spine tables.")

            for table in target_tables:
                df = con.sql(f"SELECT * FROM {table}").fetchdf()
                if df.empty:
                    logger.warning(f"Skipping {table}: no data.")
                    continue

                df = normalize_datetime(df)
                columns = ','.join(df.columns)
                values = [tuple(row) for row in df.itertuples(index=False)]
                sql = f"INSERT INTO spine.{table} ({columns}) VALUES %s"
                execute_values(cur, sql, values)

            pg.commit()
            logger.info("📥 Data inserted into spine schema.")

def main():
    json_path = None
    try:
        logger.info("🚀 Starting spine ETL...")

        file_path = get_active_file_path()
        json_path = stage_data(file_path)
        insert_into_postgres()

        logger.info("✅ Spine ETL complete.")
    except Exception as e:
        logger.exception("❌ Spine ETL failed.")
        raise
    finally:
        con.close()
        if json_path and os.path.exists(json_path):
            os.remove(json_path)

if __name__ == "__main__":
    main()


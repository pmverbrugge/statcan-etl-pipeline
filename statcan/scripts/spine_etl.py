"""
Spine ETL Script
Date: 25 May 2025

This script extracts cube metadata from StatCan’s Web Data Service,
archives the raw response to disk, stages it in DuckDB,
and loads it to the database.
"""

import os
import json
import tempfile
import requests
import duckdb
import pandas as pd
from loguru import logger
from statcan.tools.config import DB_CONFIG
from statcan.tools.file_logger import (
    compute_sha256,
    save_file_if_changed,
    log_file_ingest,
)
import psycopg2
from psycopg2.extras import execute_values

# ------------------ #
#  Config & Setup    #
# ------------------ #

WDS_ENDPOINT = "https://www150.statcan.gc.ca/t1/wds/rest/getAllCubesList"
RAW_FILENAME = "cubes_list.json"
RAW_PATH = f"/app/raw_archive/{RAW_FILENAME}"
con = duckdb.connect(":memory:")

# ------------------ #
#  Helper Functions  #
# ------------------ #

def fetch_json(url: str) -> dict:
    try:
        logger.info(f"Requesting data from {url}")
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Failed to fetch data: {e}")
        raise

def normalize_datetime(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace({pd.NaT: None, "NaT": None})
    for col in df.select_dtypes(include=["datetime64[ns]"]):
        df[col] = df[col].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if pd.notnull(x) else None)
    return df

def stage_base_data(json_bytes: bytes) -> str:
    con.sql("INSTALL json; LOAD json;")
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as f:
        f.write(json_bytes.decode("utf-8"))
        f.flush()
        json_path = f.name

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
    con.sql("CREATE OR REPLACE VIEW cube_dimension AS SELECT productId, UNNEST(dimensions, recursive := true) FROM base_cube;")
    con.sql("""
    CREATE OR REPLACE VIEW cube_correction AS
    WITH correction_unpacked AS (
        SELECT productId, UNNEST(corrections, recursive := true)
        FROM base_cube
    )
    SELECT 
        productId,
        CAST(correctionDate AS TIMESTAMP) AS correctionDate, 
        correctionNoteEn, 
        correctionNoteFr
    FROM correction_unpacked;
    """)

    logger.info("Created staging views in DuckDB.")
    return json_path

def insert_spine_tables():
    tables = ["cube", "cube_subject", "cube_survey", "cube_dimension", "cube_correction"]

    with psycopg2.connect(**DB_CONFIG) as pg:
        with pg.cursor() as cur:
            cur.execute("TRUNCATE TABLE spine.cube, spine.cube_subject, spine.cube_survey, spine.cube_dimension, spine.cube_correction")
            logger.info("Truncated spine tables.")

            for table in tables:
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
            logger.info("Inserted data into spine tables.")

# ------------------ #
#  Main Pipeline     #
# ------------------ #

def main():
    json_path = None
    try:
        logger.info("Starting StatCan spine ETL...")

        data = fetch_json(WDS_ENDPOINT)
        json_bytes = json.dumps(data, indent=2).encode("utf-8")

        file_hash = compute_sha256(json_bytes)

        if save_file_if_changed(RAW_PATH, json_bytes):
            logger.info(f"💾 Saved updated file to {RAW_PATH}")
        else:
            logger.info("✅ File already exists and is unchanged.")

        with psycopg2.connect(**DB_CONFIG) as conn:
            log_file_ingest(
                conn=conn,
                productid="cubes_list",
                file_path=RAW_PATH,
                file_hash=file_hash,
                source_url=WDS_ENDPOINT,
            )

        json_path = stage_base_data(json_bytes)
        insert_spine_tables()
        logger.info("ETL complete. Spine tables populated.")
    except Exception as e:
        logger.exception("Pipeline failed.")
        raise
    finally:
        con.close()
        if json_path and os.path.exists(json_path):
            os.remove(json_path)

if __name__ == "__main__":
    main()


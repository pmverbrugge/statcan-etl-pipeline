"""
Spine ETL Script
Author: PMV
Date: 25 May 2025

This script extracts cube metadata from StatCan’s Web Data Service,
archives the raw response in PostgreSQL, stages it in DuckDB,
and prepares it for loading into structured warehouse tables.
"""

import os
import json
import tempfile
import requests
import duckdb
from loguru import logger
from lib.archive_ingester import ingest_file_from_bytes
from lib.config import DB_CONFIG
import psycopg2

# ------------------ #
#  Config & Setup    #
# ------------------ #

WDS_ENDPOINT = "https://www150.statcan.gc.ca/t1/wds/rest/getAllCubesList"
con = duckdb.connect(":memory:")

# ------------------ #
#  Helper Functions  #
# ------------------ #

def fetch_json(url: str) -> dict:
    """Download JSON data from the given URL."""
    try:
        logger.info(f"Requesting data from {url}")
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Failed to fetch data: {e}")
        raise

def stage_base_data(json_bytes: bytes):
    """Load raw JSON into DuckDB and define views."""
    con.sql("INSTALL json; LOAD json;")

    with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as f:
        f.write(json_bytes.decode("utf-8"))
        f.flush()
        json_path = f.name

    con.execute(f"""
    CREATE OR REPLACE VIEW base_cube AS 
    SELECT * FROM read_json_auto('{json_path}')
    """)

    con.sql("""
    CREATE OR REPLACE VIEW cube AS 
    SELECT DISTINCT
        productId,     
        cansimId,      
        cubeTitleEn,   
        cubeTitleFr,   
        CAST(cubeStartDate AS DATE) AS cubeStartDate, 
        CAST(cubeEndDate AS DATE) AS cubeEndDate,   
        CAST(releaseTime AS DATE) AS releaseTime,   
        CAST(archived AS SMALLINT) AS archived,       
        CAST(frequencyCode AS SMALLINT) AS frequencyCode,  
        CAST(issueDate AS DATE) AS issueDate
    FROM base_cube;
    """)

    con.sql("""
    CREATE OR REPLACE VIEW cube_subject AS 
    SELECT productId, UNNEST(subjectCode) AS subjectCode
    FROM base_cube;
    """)

    con.sql("""
    CREATE OR REPLACE VIEW cube_survey AS 
    SELECT productId, UNNEST(surveyCode) AS surveyCode
    FROM base_cube;
    """)

    con.sql("""
    CREATE OR REPLACE VIEW cube_dimension AS
    SELECT productId,
           UNNEST(dimensions, recursive := true) AS dimension
    FROM base_cube;
    """)

    con.sql("""
    CREATE OR REPLACE VIEW cube_correction AS
    WITH correction_unpacked AS (
        SELECT productId,
               UNNEST(corrections, recursive := true) AS correction
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

# ------------------ #
#  Main Pipeline     #
# ------------------ #

def main():
    logger.info("Starting StatCan spine ETL...")

    data = fetch_json(WDS_ENDPOINT)

    json_bytes = json.dumps(data, indent=2).encode("utf-8")
    file_name = "cubes_list.json"
    source_url = WDS_ENDPOINT

    with psycopg2.connect(**DB_CONFIG) as conn:
        ingested = ingest_file_from_bytes(
            file_bytes=json_bytes,
            file_name=file_name,
            conn=conn,
            source_url=source_url,
            content_type="application/json"
        )
        if ingested:
            logger.info(f"Ingested response to archive as {file_name}")
        else:
            logger.info(f"Duplicate response skipped for {file_name}")

    stage_base_data(json_bytes)

    logger.info("ETL complete. Ready for DB insert.")


if __name__ == "__main__":
    main()


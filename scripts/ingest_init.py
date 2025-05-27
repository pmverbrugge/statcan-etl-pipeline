"""
Spine ETL Script
Author: PMV
Date: 27 May 2025

This script extracts cube data metadata from StatCan’s Web Data Service,
archives the raw response in Postgres for local retreival during project 
devlopement an initialization. Once the environment is established, 
piplines to retrieve and annex changed data will be implemented. 


"""

#import os
#import json
#import tempfile
#import requests
#import duckdb
#import pandas as pd
#from loguru import logger
#from lib.archive_ingester import ingest_file_from_bytes
#from lib.config import DB_CONFIG
#import psycopg2
#from psycopg2.extras import execute_values
import requests
import psycopg2
from lib.archive_ingester import ingest_file_from_bytes, get_file_from_archive
from dotenv import load_dotenv
import os

# ------------------ #
#  Config & Setup    #
# ------------------ #

# ------------------ #
#  Helper Functions  #
# ------------------ #

# ------------------ #
#  Main Pipeline     #
# ------------------ #

# Load credentials from .env
load_dotenv()
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRESS", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

STATCAN_URL="https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadSDMX/35100027"

FILENAME = "3510002701.sdmx.zip"

def main():
    # Connect to Postgres
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    # Download metadata JSON
    print("Downloading metadata...")
    resp = requests.get(STATCAN_URL)
    resp.raise_for_status()
    result = resp.json()

    if not isinstance(result, dict) or "object" not in result:
        raise ValueError(f"Unexpected JSON structure: {result}")

    file_url = result["object"]  # direct string, not a dict
    content_type = "application/zip"

    # Download the SDMX ZIP file
    print(f"Downloading file from: {file_url}")
    file_bytes = requests.get(file_url).content
    print(f"Downloaded {len(file_bytes)} bytes.")

    # Ingest into archive
    print("Ingesting into archive...")
    ingest_file_from_bytes(file_bytes, FILENAME, conn, source_url=file_url, content_type=content_type)

    # Retrieve from archive
    print("Retrieving from archive...")
    archive_record = get_file_from_archive(conn, file_name=FILENAME)
    if not archive_record:
        raise RuntimeError("File not found in archive")

    # Compare contents
    if file_bytes == archive_record["content"]:
        print("✅ File verified: content matches.")
    else:
        print("❌ File mismatch!")

    conn.close()

if __name__ == "__main__":
    main()
resp = requests.get(STATCAN_URL)
resp.raise_for_status()



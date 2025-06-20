"""
Enhanced Spine ETL Script - Statistics Canada ETL Pipeline
==========================================================

This script loads validated spine metadata from archived files into the PostgreSQL
data warehouse. It implements pre-load validation and safety checks to prevent
database corruption from invalid or incomplete data files.

Key Features:
- Loads most recent validated spine file from archive
- Stages data in DuckDB for validation before database changes
- Implements comprehensive pre-load validation checks
- Uses atomic transactions to prevent partial updates
- Maintains referential integrity across spine schema tables

Process Flow:
1. Retrieve path of most recent active spine file
2. Load JSON data into DuckDB memory database
3. Create normalized views (cube, cube_subject, cube_survey)
4. Validate staged data completeness and quality
5. Compare against existing spine data for sanity checks
6. Atomically replace spine tables with validated data

Protection Mechanisms:
- Pre-load validation prevents truncating good data
- Size and content checks ensure data completeness
- Atomic transactions with rollback on failure
- Comparison with existing data for sanity checks
- Detailed logging for audit trail

Dependencies:
- Requires validated spine files from 01_spine_fetch_raw.py
- Uses DuckDB for in-memory staging and validation
- Connects to PostgreSQL spine schema tables

Last Updated: June 2025
Author: Paul Verbrugge with Claude 3.5 Sonnet (v20241022)e
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

# Add file logging
logger.add("/app/logs/spine_etl.log", rotation="10 MB", retention="7 days")

# Constants
RAW_DIR = "/app/raw/statcan/metadata"
MIN_EXPECTED_CUBES = 1000
MIN_SUBJECTS_RATIO = 0.8  # Expect 80%+ of cubes to have subjects
MAX_SIZE_VARIANCE = 0.1   # Allow 10% variance from existing data

# Initialize DuckDB connection
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
                raise RuntimeError("❌ No active spine metadata file found.")
            file_path = result[0]
            logger.info(f"📁 Active spine file: {file_path}")
            return file_path


def get_existing_spine_stats(cur) -> dict:
    """Get statistics about current spine data for comparison"""
    stats = {}
    
    # Count existing cubes
    cur.execute("SELECT COUNT(*) FROM spine.cube")
    stats['cube_count'] = cur.fetchone()[0]
    
    # Count cube subjects
    cur.execute("SELECT COUNT(*) FROM spine.cube_subject")
    stats['subject_count'] = cur.fetchone()[0]
    
    # Count cube surveys
    cur.execute("SELECT COUNT(*) FROM spine.cube_survey")
    stats['survey_count'] = cur.fetchone()[0]
    
    # Count archived cubes
    cur.execute("SELECT COUNT(*) FROM spine.cube WHERE archived = 1")
    stats['archived_count'] = cur.fetchone()[0]
    
    logger.info(f"📊 Existing spine stats: {stats['cube_count']} cubes, {stats['subject_count']} subjects, {stats['survey_count']} surveys, {stats['archived_count']} archived")
    return stats


def normalize_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Convert datetime columns to string format for PostgreSQL insertion."""
    df = df.replace({pd.NaT: None, "NaT": None})
    for col in df.select_dtypes(include=["datetime64[ns]"]):
        df[col] = df[col].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if pd.notnull(x) else None)
    return df


def stage_data(file_path: str) -> str:
    """Read the JSON metadata into DuckDB and stage views for target tables."""
    logger.info(f"📥 Loading spine file into DuckDB: {file_path}")
    
    # Verify file exists and is readable
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ Spine file not found: {file_path}")
    
    file_size = os.path.getsize(file_path)
    if file_size < 100000:  # Less than 100KB is suspicious
        raise ValueError(f"❌ Spine file too small: {file_size} bytes")
    
    logger.info(f"📏 File size: {file_size:,} bytes")
    
    # Install and load JSON extension
    con.sql("INSTALL json; LOAD json;")

    # Read and validate JSON file
    try:
        with open(file_path, "rb") as f:
            json_bytes = f.read()
        
        # Verify it's valid JSON
        json_data = json.loads(json_bytes.decode("utf-8"))
        if not isinstance(json_data, list):
            raise ValueError("❌ JSON file does not contain a list")
        
        logger.info(f"✅ JSON validated: {len(json_data)} records")
    except json.JSONDecodeError as e:
        raise ValueError(f"❌ Invalid JSON in spine file: {e}")

    # Create temporary file for DuckDB
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as tmp:
        tmp.write(json_bytes.decode("utf-8"))
        tmp.flush()
        json_path = tmp.name

    try:
        # Create base view from JSON
        con.execute(f"CREATE OR REPLACE VIEW base_cube AS SELECT * FROM read_json_auto('{json_path}')")
        
        # Check that we can read the base data
        base_count = con.sql("SELECT COUNT(*) FROM base_cube").fetchone()[0]
        logger.info(f"📊 Base cube records loaded: {base_count}")

        # Create normalized views
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

        con.sql("""
            CREATE OR REPLACE VIEW cube_subject AS 
            SELECT productId, UNNEST(subjectCode) AS subjectCode 
            FROM base_cube 
            WHERE subjectCode IS NOT NULL AND len(subjectCode) > 0;
        """)

        con.sql("""
            CREATE OR REPLACE VIEW cube_survey AS 
            SELECT productId, UNNEST(surveyCode) AS surveyCode 
            FROM base_cube 
            WHERE surveyCode IS NOT NULL AND len(surveyCode) > 0;
        """)

        logger.success("✅ DuckDB views created successfully")
        return json_path
        
    except Exception as e:
        # Clean up temp file on error
        if os.path.exists(json_path):
            os.remove(json_path)
        raise RuntimeError(f"❌ Failed to create DuckDB views: {e}")


def validate_staged_data(existing_stats: dict):
    """Comprehensive validation of staged data before database update"""
    logger.info("🔍 Validating staged data...")
    
    # Get counts from staged views
    cube_count = con.sql("SELECT COUNT(*) FROM cube").fetchone()[0]
    subject_count = con.sql("SELECT COUNT(*) FROM cube_subject").fetchone()[0]
    survey_count = con.sql("SELECT COUNT(*) FROM cube_survey").fetchone()[0]
    
    logger.info(f"📊 Staged data: {cube_count} cubes, {subject_count} subjects, {survey_count} surveys")
    
    # Validation 1: Minimum cube count
    if cube_count < MIN_EXPECTED_CUBES:
        raise ValueError(f"❌ Too few cubes staged: {cube_count} < {MIN_EXPECTED_CUBES}")
    
    # Validation 2: Required fields not null
    null_product_ids = con.sql("SELECT COUNT(*) FROM cube WHERE productId IS NULL").fetchone()[0]
    if null_product_ids > 0:
        raise ValueError(f"❌ {null_product_ids} cubes have NULL productId")
    
    null_titles = con.sql("SELECT COUNT(*) FROM cube WHERE cubeTitleEn IS NULL OR trim(cubeTitleEn) = ''").fetchone()[0]
    if null_titles > 0:
        raise ValueError(f"❌ {null_titles} cubes have NULL or empty English titles")
    
    # Validation 3: Product ID uniqueness
    duplicate_ids = con.sql("SELECT COUNT(*) - COUNT(DISTINCT productId) FROM cube").fetchone()[0]
    if duplicate_ids > 0:
        raise ValueError(f"❌ {duplicate_ids} duplicate product IDs found")
    
    # Validation 4: Subject coverage (most cubes should have subjects)
    cubes_with_subjects = con.sql("SELECT COUNT(DISTINCT productId) FROM cube_subject").fetchone()[0]
    subject_ratio = cubes_with_subjects / cube_count if cube_count > 0 else 0
    if subject_ratio < MIN_SUBJECTS_RATIO:
        raise ValueError(f"❌ Too few cubes with subjects: {subject_ratio:.1%} < {MIN_SUBJECTS_RATIO:.1%}")
    
    # Validation 5: Compare with existing data (if exists)
    if existing_stats['cube_count'] > 0:
        size_ratio = cube_count / existing_stats['cube_count']
        if size_ratio < (1 - MAX_SIZE_VARIANCE) or size_ratio > (1 + MAX_SIZE_VARIANCE):
            logger.warning(f"⚠️  Large size change: {existing_stats['cube_count']} → {cube_count} ({size_ratio:.1%})")
            if size_ratio < 0.5:  # More than 50% reduction is suspicious
                raise ValueError(f"❌ Suspicious data reduction: {size_ratio:.1%} of original size")
    
    # Validation 6: Data type consistency
    invalid_archived = con.sql("SELECT COUNT(*) FROM cube WHERE archived NOT IN (0, 1, NULL)").fetchone()[0]
    if invalid_archived > 0:
        raise ValueError(f"❌ {invalid_archived} cubes have invalid archived values")
    
    logger.success("✅ Staged data validation passed")
    logger.info(f"📈 Subject coverage: {subject_ratio:.1%}")
    
    return {
        'cube_count': cube_count,
        'subject_count': subject_count,
        'survey_count': survey_count,
        'subject_ratio': subject_ratio
    }


def insert_into_postgres(staged_stats: dict):
    """Insert the validated staged data into the spine schema in PostgreSQL."""
    target_tables = ["cube", "cube_subject", "cube_survey"]

    with psycopg2.connect(**DB_CONFIG) as pg:
        with pg.cursor() as cur:
            try:
                logger.info("🔄 Starting atomic spine update...")
                
                # Begin explicit transaction
                cur.execute("BEGIN")
                
                # Truncate tables (within transaction)
                cur.execute("TRUNCATE TABLE spine.cube, spine.cube_subject, spine.cube_survey CASCADE")
                logger.info("🗑️  Spine tables truncated")

                # Insert data for each table
                for table in target_tables:
                    logger.info(f"📥 Inserting {table} data...")
                    
                    df = con.sql(f"SELECT * FROM {table}").fetchdf()
                    if df.empty:
                        logger.warning(f"⚠️  No data for {table}, skipping...")
                        continue

                    df = normalize_datetime(df)
                    columns = ','.join(df.columns)
                    values = [tuple(row) for row in df.itertuples(index=False)]
                    
                    sql = f"INSERT INTO spine.{table} ({columns}) VALUES %s"
                    execute_values(cur, sql, values)
                    
                    logger.info(f"✅ Inserted {len(values)} rows into spine.{table}")

                # Commit transaction
                cur.execute("COMMIT")
                logger.success("✅ Spine update committed successfully")
                
                # Log final summary
                logger.info("📊 Update summary:")
                logger.info(f"   Cubes: {staged_stats['cube_count']}")
                logger.info(f"   Subjects: {staged_stats['subject_count']}")
                logger.info(f"   Surveys: {staged_stats['survey_count']}")

            except Exception as e:
                # Rollback on any error
                cur.execute("ROLLBACK")
                logger.error("🔄 Transaction rolled back due to error")
                raise RuntimeError(f"❌ Failed to update spine tables: {e}")


def main():
    json_path = None
    try:
        logger.info("🚀 Starting enhanced spine ETL...")

        # Get active file and existing stats
        file_path = get_active_file_path()
        
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                existing_stats = get_existing_spine_stats(cur)

        # Stage and validate data
        json_path = stage_data(file_path)
        staged_stats = validate_staged_data(existing_stats)
        
        # Update database with validated data
        insert_into_postgres(staged_stats)

        logger.success("✅ Enhanced spine ETL completed successfully")

    except Exception as e:
        logger.exception("❌ Enhanced spine ETL failed")
        raise
    finally:
        con.close()
        if json_path and os.path.exists(json_path):
            os.remove(json_path)
            logger.info("🧹 Temporary files cleaned up")


if __name__ == "__main__":
    main()

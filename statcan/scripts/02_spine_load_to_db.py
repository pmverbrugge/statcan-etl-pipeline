"""
Statcan Public Data ETL Project - Script 02: Spine Load to Database
Script: 02_spine_load_to_db.py
Date: June 21, 2025
Author: Paul Verbrugge with Claude Sonnet 4

PURPOSE:
Loads StatCan spine metadata from raw JSON files into normalized PostgreSQL spine schema tables.
Processes cube metadata, subject relationships, and survey relationships through DuckDB staging 
then direct PostgreSQL loading for optimal performance.

DEPENDENCIES:
- Script 01: Must have active spine metadata file registered
- Database: spine schema tables (cube, cube_subject, cube_survey)
- Raw Files: Active spine metadata JSON file on disk

PROCESSING LOGIC:
1. Identify and validate active spine metadata file
2. Stage JSON data in DuckDB with type casting and validation
3. Create normalized views for cube, subject, and survey data
4. Load data directly to PostgreSQL in atomic transaction
5. Validate loaded data integrity and relationships

OUTPUTS:
- spine.cube: Core cube metadata (productId, titles, dates, etc.)
- spine.cube_subject: Many-to-many cube-subject relationships  
- spine.cube_survey: Many-to-many cube-survey relationships

PERFORMANCE NOTES:
- Uses DuckDB for efficient JSON processing
- Direct DuckDB->PostgreSQL transfer via CSV (eliminates pandas)
- Atomic transactions with rollback capability
- Memory efficient with temporary file cleanup
"""

import os
import json
import hashlib
import tempfile
import duckdb
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from loguru import logger
from datetime import datetime
from statcan.tools.config import DB_CONFIG

# Processing Constants
RAW_DIR = "/app/raw/statcan/metadata"
TEMP_DIR = "/tmp"
TARGET_TABLES = ["cube", "cube_subject", "cube_survey"]

def validate_prerequisites() -> None:
    """Validate all prerequisites before processing begins."""
    logger.info("📋 Validating prerequisites...")
    
    # Check database connectivity
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                logger.info("✅ Database connectivity confirmed")
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise RuntimeError(f"Cannot connect to database: {e}")
    
    # Verify spine schema tables exist
    required_tables = [f"spine.{table}" for table in TARGET_TABLES]
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                for table in required_tables:
                    cur.execute(f"SELECT COUNT(*) FROM {table} LIMIT 1")
                logger.info(f"✅ Verified spine schema tables: {', '.join(required_tables)}")
    except Exception as e:
        logger.error(f"❌ Spine schema validation failed: {e}")
        raise RuntimeError(f"Spine schema tables not accessible: {e}")
    
    # Check temp directory permissions
    try:
        test_file = os.path.join(TEMP_DIR, f"test_write_{datetime.now().strftime('%Y%m%d_%H%M%S')}.tmp")
        with open(test_file, 'w') as f:
            f.write("test")
        os.remove(test_file)
        logger.info(f"✅ Temporary directory writable: {TEMP_DIR}")
    except Exception as e:
        logger.error(f"❌ Temporary directory not writable: {TEMP_DIR}")
        raise RuntimeError(f"Cannot write to temporary directory: {e}")

def get_active_file_path() -> str:
    """Fetch the file path of the active spine metadata file from PostgreSQL."""
    logger.info("📁 Identifying active spine metadata file...")
    
    query = """
        SELECT storage_location
        FROM raw_files.manage_spine_raw_files
        WHERE active = true
        ORDER BY date_download DESC
        LIMIT 1
    """
    
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchone()
                
                if not result:
                    logger.error("❌ No active spine metadata file found in registry")
                    raise RuntimeError("No active spine metadata file found in database registry")
                
                file_path = result[0]
                logger.info(f"📁 Active spine file: {file_path}")
                return file_path
                
    except psycopg2.Error as e:
        logger.error(f"❌ Database error retrieving active file: {e}")
        raise RuntimeError(f"Database error: {e}")

def validate_json_file(file_path: str) -> dict:
    """Validate JSON file structure and return metadata."""
    logger.info(f"🔍 Validating JSON file: {os.path.basename(file_path)}")
    
    # Check file exists and is readable
    if not os.path.exists(file_path):
        logger.error(f"❌ File does not exist: {file_path}")
        raise FileNotFoundError(f"Spine metadata file not found: {file_path}")
    
    if not os.access(file_path, os.R_OK):
        logger.error(f"❌ File not readable: {file_path}")
        raise PermissionError(f"Cannot read spine metadata file: {file_path}")
    
    # Get file metadata
    file_size = os.path.getsize(file_path)
    logger.info(f"📊 File size: {file_size:,} bytes")
    
    # Validate JSON structure
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        if not isinstance(data, list):
            logger.error("❌ JSON file must contain array of cube objects")
            raise ValueError("Invalid JSON structure: expected array of objects")
        
        if len(data) == 0:
            logger.error("❌ JSON file contains no cube data")
            raise ValueError("Empty JSON file: no cube metadata found")
        
        # Validate required fields in first record
        required_fields = ['productId', 'cubeTitleEn', 'subjectCode']
        sample_record = data[0]
        missing_fields = [field for field in required_fields if field not in sample_record]
        
        if missing_fields:
            logger.error(f"❌ Missing required fields: {missing_fields}")
            raise ValueError(f"Invalid JSON structure: missing fields {missing_fields}")
        
        logger.info(f"✅ JSON validation passed - {len(data):,} cube records")
        
        return {
            'file_path': file_path,
            'file_size': file_size,
            'record_count': len(data),
            'sample_record': sample_record
        }
        
    except json.JSONDecodeError as e:
        logger.error(f"❌ Invalid JSON format: {e}")
        raise ValueError(f"JSON decode error: {e}")
    except Exception as e:
        logger.error(f"❌ JSON validation failed: {e}")
        raise

def stage_data_in_duckdb(file_path: str) -> duckdb.DuckDBPyConnection:
    """Stage JSON data in DuckDB with comprehensive validation and type casting."""
    logger.info("🦆 Staging data in DuckDB...")
    
    # Create in-memory DuckDB connection
    con = duckdb.connect(":memory:")
    
    try:
        # Install and load JSON extension
        con.execute("INSTALL json; LOAD json;")
        logger.info("✅ DuckDB JSON extension loaded")
        
        # Create base view from JSON file
        con.execute(f"""
            CREATE OR REPLACE VIEW base_cube AS 
            SELECT * FROM read_json_auto('{file_path}')
        """)
        
        # Validate base data loaded
        base_count = con.execute("SELECT COUNT(*) FROM base_cube").fetchone()[0]
        logger.info(f"📊 Base cube records loaded: {base_count:,}")
        
        if base_count == 0:
            raise ValueError("No records loaded from JSON file")
        
        # Create normalized cube view with robust type casting
        logger.info("🔄 Creating normalized cube view...")
        con.execute("""
            CREATE OR REPLACE VIEW cube AS 
            SELECT DISTINCT
                productId,
                cansimId,
                cubeTitleEn,
                cubeTitleFr,
                TRY_CAST(cubeStartDate AS DATE) AS cubeStartDate,
                TRY_CAST(cubeEndDate AS DATE) AS cubeEndDate,
                TRY_CAST(releaseTime AS TIMESTAMP) AS releaseTime,
                COALESCE(TRY_CAST(archived AS SMALLINT), 0) AS archived,
                COALESCE(TRY_CAST(frequencyCode AS SMALLINT), 0) AS frequencyCode,
                TRY_CAST(issueDate AS TIMESTAMP) AS issueDate
            FROM base_cube
            WHERE productId IS NOT NULL
        """)
        
        # Create subject relationship view
        logger.info("🔗 Creating cube-subject relationship view...")
        con.execute("""
            CREATE OR REPLACE VIEW cube_subject AS 
            SELECT DISTINCT
                productId,
                UNNEST(subjectCode) AS subjectCode
            FROM base_cube
            WHERE productId IS NOT NULL 
            AND subjectCode IS NOT NULL
            AND len(subjectCode) > 0
        """)
        
        # Create survey relationship view  
        logger.info("🔗 Creating cube-survey relationship view...")
        con.execute("""
            CREATE OR REPLACE VIEW cube_survey AS 
            SELECT DISTINCT
                productId,
                UNNEST(surveyCode) AS surveyCode
            FROM base_cube
            WHERE productId IS NOT NULL 
            AND surveyCode IS NOT NULL
            AND len(surveyCode) > 0
        """)
        
        # Validate all views created successfully
        for table in TARGET_TABLES:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            logger.info(f"📊 {table} view: {count:,} records")
            
            if count == 0 and table == "cube":
                raise ValueError("No cube records after processing - data validation failed")
        
        logger.info("✅ DuckDB staging complete")
        return con
        
    except Exception as e:
        logger.error(f"❌ DuckDB staging failed: {e}")
        con.close()
        raise

def capture_spine_tables_state() -> dict:
    """Capture current state of spine schema tables for testing validation."""
    logger.info("📸 Capturing spine tables state...")
    
    state = {}
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            for table in TARGET_TABLES:
                full_table = f"spine.{table}"
                df = pd.read_sql(f"SELECT * FROM {full_table} ORDER BY productId", conn)
                
                # Create content hash for comparison
                content_hash = hashlib.md5(
                    df.to_string(index=False).encode('utf-8')
                ).hexdigest()
                
                state[table] = {
                    'row_count': len(df),
                    'content_hash': content_hash,
                    'data': df
                }
                
                logger.info(f"📊 {full_table}: {len(df):,} rows, hash: {content_hash[:8]}...")
        
        return state
        
    except Exception as e:
        logger.error(f"❌ Failed to capture spine tables state: {e}")
        raise

def load_to_postgres_atomic(con: duckdb.DuckDBPyConnection) -> dict:
    """Load all spine data to PostgreSQL in single atomic transaction."""
    logger.info("📥 Starting atomic PostgreSQL load...")
    
    temp_files = []
    load_stats = {}
    
    try:
        with psycopg2.connect(**DB_CONFIG) as pg_conn:
            with pg_conn.cursor() as cur:
                logger.info("🔄 Beginning atomic transaction...")
                
                # Truncate all spine tables first
                logger.info("🗑️ Truncating existing spine tables...")
                cur.execute("TRUNCATE TABLE spine.cube, spine.cube_subject, spine.cube_survey CASCADE")
                
                # Load each table using DuckDB -> CSV -> PostgreSQL pipeline
                for table in TARGET_TABLES:
                    logger.info(f"📊 Processing {table}...")
                    
                    # Generate temporary CSV file
                    temp_csv = os.path.join(TEMP_DIR, f"spine_{table}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                    temp_files.append(temp_csv)
                    
                    # Export from DuckDB to CSV
                    con.execute(f"""
                        COPY (SELECT * FROM {table}) 
                        TO '{temp_csv}' 
                        (FORMAT CSV, HEADER true)
                    """)
                    
                    # Verify CSV file created
                    if not os.path.exists(temp_csv):
                        raise RuntimeError(f"Failed to create temporary CSV file: {temp_csv}")
                    
                    csv_size = os.path.getsize(temp_csv)
                    logger.info(f"📁 Generated CSV: {os.path.basename(temp_csv)} ({csv_size:,} bytes)")
                    
                    # Import CSV to PostgreSQL
                    with open(temp_csv, 'r') as csv_file:
                        cur.copy_expert(f"COPY spine.{table} FROM STDIN CSV HEADER", csv_file)
                    
                    # Validate load
                    cur.execute(f"SELECT COUNT(*) FROM spine.{table}")
                    loaded_count = cur.fetchone()[0]
                    
                    load_stats[table] = {
                        'records_loaded': loaded_count,
                        'csv_size': csv_size
                    }
                    
                    logger.info(f"✅ {table}: {loaded_count:,} records loaded")
                
                # Validate foreign key relationships
                logger.info("🔗 Validating relationships...")
                cur.execute("""
                    SELECT COUNT(*) FROM spine.cube_subject cs
                    LEFT JOIN spine.cube c ON cs.productId = c.productId
                    WHERE c.productId IS NULL
                """)
                orphaned_subjects = cur.fetchone()[0]
                
                cur.execute("""
                    SELECT COUNT(*) FROM spine.cube_survey cs
                    LEFT JOIN spine.cube c ON cs.productId = c.productId
                    WHERE c.productId IS NULL
                """)
                orphaned_surveys = cur.fetchone()[0]
                
                if orphaned_subjects > 0:
                    logger.warning(f"⚠️ Found {orphaned_subjects} orphaned subject relationships")
                if orphaned_surveys > 0:
                    logger.warning(f"⚠️ Found {orphaned_surveys} orphaned survey relationships")
                
                # Commit transaction
                pg_conn.commit()
                logger.info("✅ Atomic transaction committed successfully")
                
                return load_stats
                
    except Exception as e:
        logger.error(f"❌ PostgreSQL load failed: {e}")
        raise
    
    finally:
        # Clean up temporary files
        for temp_file in temp_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                    logger.info(f"🗑️ Cleaned up: {os.path.basename(temp_file)}")
            except Exception as cleanup_error:
                logger.warning(f"⚠️ Failed to clean up {temp_file}: {cleanup_error}")

def validate_processing_results(load_stats: dict) -> None:
    """Validate final processing results and data integrity."""
    logger.info("🔍 Validating processing results...")
    
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Validate all tables have data
                total_records = 0
                for table in TARGET_TABLES:
                    cur.execute(f"SELECT COUNT(*) FROM spine.{table}")
                    count = cur.fetchone()[0]
                    expected = load_stats[table]['records_loaded']
                    
                    if count != expected:
                        raise ValueError(f"Record count mismatch in {table}: expected {expected}, found {count}")
                    
                    total_records += count
                    logger.info(f"✅ {table}: {count:,} records validated")
                
                # Validate core cube data integrity
                cur.execute("SELECT COUNT(*) FROM spine.cube WHERE productId IS NULL OR cubeTitleEn = ''")
                invalid_cubes = cur.fetchone()[0]
                
                if invalid_cubes > 0:
                    logger.warning(f"⚠️ Found {invalid_cubes} cubes with missing required data")
                
                # Check for reasonable data distribution
                cur.execute("SELECT COUNT(DISTINCT productId) FROM spine.cube")
                unique_cubes = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(DISTINCT subjectCode) FROM spine.cube_subject")
                unique_subjects = cur.fetchone()[0]
                
                logger.info(f"📊 Data summary: {unique_cubes:,} unique cubes, {unique_subjects:,} unique subjects")
                logger.info(f"✅ Processing validation complete - {total_records:,} total records")
                
    except Exception as e:
        logger.error(f"❌ Processing validation failed: {e}")
        raise

def main() -> None:
    """Main processing function with comprehensive error handling."""
    con = None
    
    try:
        logger.info("🎯 SCRIPT 02: Spine Load to Database - Starting")
        start_time = datetime.now()
        
        # Phase 1: Prerequisites and validation
        validate_prerequisites()
        file_path = get_active_file_path()
        file_metadata = validate_json_file(file_path)
        
        # Phase 2: Data staging
        con = stage_data_in_duckdb(file_path)
        
        # Phase 3: Database loading
        load_stats = load_to_postgres_atomic(con)
        
        # Phase 4: Final validation
        validate_processing_results(load_stats)
        
        # Success summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        total_records = sum(stats['records_loaded'] for stats in load_stats.values())
        
        logger.info(f"✅ SCRIPT 02 COMPLETE")
        logger.info(f"⏱️ Processing time: {duration:.1f} seconds")
        logger.info(f"📊 Total records processed: {total_records:,}")
        logger.info(f"📁 Source file: {os.path.basename(file_path)} ({file_metadata['file_size']:,} bytes)")
        
    except Exception as e:
        logger.error(f"❌ SCRIPT 02 FAILED: {e}")
        logger.exception("Full error traceback:")
        raise
        
    finally:
        # Cleanup DuckDB connection
        if con:
            try:
                con.close()
                logger.info("🦆 DuckDB connection closed")
            except Exception as cleanup_error:
                logger.warning(f"⚠️ DuckDB cleanup warning: {cleanup_error}")

if __name__ == "__main__":
    main()

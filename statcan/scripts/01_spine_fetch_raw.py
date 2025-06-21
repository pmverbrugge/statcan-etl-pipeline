#!/usr/bin/env python3
"""
Statcan Public Data ETL Pipeline
Script: 01_spine_fetch_raw_v2.py
Date: 2025-06-21
Author: Paul Verbrugge with Claude Sonnet 4

Fetches the complete inventory of Statistics Canada data cubes from the getAllCubesListLite
endpoint and archives new versions using hash-based deduplication. This forms the foundation
of the ETL pipeline by maintaining a historical record of cube metadata changes.

The script implements atomic operations to ensure data consistency and uses SHA256 hashing
to detect changes in the spine metadata. Only new versions are stored, preventing
unnecessary storage bloat while maintaining complete audit trails.

Key Operations:
- Fetch complete cube inventory from StatCan WDS API
- Generate SHA256 hash for change detection  
- Store new versions with atomic database operations
- Maintain active/inactive flags for version management

Dependencies:
- Internet connectivity to StatCan WDS API
- PostgreSQL database with raw_files.manage_spine_raw_files table
- Write permissions to /app/raw/metadata directory

Processing Logic:
1. Fetch JSON from getAllCubesListLite endpoint
2. Calculate SHA256 hash of normalized JSON content
3. Check database for existing hash to prevent duplicates
4. If new: save file, deactivate old versions, insert new record
5. Commit transaction atomically or rollback on failure
"""

import os
import json
import hashlib
import requests
import psycopg2
from datetime import datetime
from loguru import logger
from pathlib import Path
from statcan.tools.config import DB_CONFIG

# Configure structured logging with rotation
logger.add("/app/logs/spine_fetch_raw_v2.log", rotation="10 MB", retention="7 days")

# StatCan API configuration
SPINE_URL = "https://www150.statcan.gc.ca/t1/wds/rest/getAllCubesListLite"
ARCHIVE_DIR = Path("/app/raw/metadata")
API_TIMEOUT = 30
MAX_RETRIES = 3


def fetch_spine_metadata() -> dict:
    """Fetch cube inventory from StatCan WDS API with retry logic
    
    Returns:
        dict: Complete cube inventory in JSON format
        
    Raises:
        requests.RequestException: If API call fails after retries
    """
    logger.info(f"🚀 Starting spine metadata fetch...")
    logger.info(f"📡 Endpoint: {SPINE_URL}")
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(SPINE_URL, timeout=API_TIMEOUT)
            response.raise_for_status()
            
            data = response.json()
            cube_count = len(data) if isinstance(data, list) else len(data.get('cubes', []))
            logger.success(f"✅ Retrieved {cube_count:,} cube records from API")
            
            return data
            
        except requests.exceptions.Timeout:
            logger.warning(f"⚠️ API timeout (attempt {attempt + 1}/{MAX_RETRIES})")
            if attempt == MAX_RETRIES - 1:
                raise
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ API request failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt == MAX_RETRIES - 1:
                raise


def calculate_content_hash(data: dict) -> str:
    """Generate SHA256 hash of JSON content for change detection
    
    Args:
        data: JSON data structure to hash
        
    Returns:
        str: SHA256 hash as hexadecimal string
        
    Note:
        Uses sorted keys to ensure consistent hashing regardless of key order
    """
    normalized_json = json.dumps(data, sort_keys=True)
    content_bytes = normalized_json.encode('utf-8')
    hash_value = hashlib.sha256(content_bytes).hexdigest()
    
    logger.info(f"🔍 Generated content hash: {hash_value[:16]}...")
    return hash_value


def save_metadata_file(data: dict, file_hash: str) -> str:
    """Save metadata to disk with hash-based filename
    
    Args:
        data: JSON metadata to save
        file_hash: SHA256 hash for filename generation
        
    Returns:
        str: Full path to saved file
        
    Raises:
        OSError: If file creation fails
    """
    # Ensure archive directory exists
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    
    # Generate filename with hash prefix for uniqueness
    filename = f"spine_{file_hash[:16]}.json"
    file_path = ARCHIVE_DIR / filename
    
    # Save with UTF-8 encoding and pretty formatting
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    logger.info(f"💾 Saved metadata file: {file_path}")
    return str(file_path)


def check_hash_exists(cur, file_hash: str) -> bool:
    """Check if hash already exists in database
    
    Args:
        cur: Database cursor
        file_hash: SHA256 hash to check
        
    Returns:
        bool: True if hash exists, False otherwise
    """
    cur.execute(
        "SELECT 1 FROM raw_files.manage_spine_raw_files WHERE file_hash = %s", 
        (file_hash,)
    )
    exists = cur.fetchone() is not None
    
    if exists:
        logger.info(f"🔍 Hash already exists in database: {file_hash[:16]}...")
    
    return exists


def deactivate_existing_files(cur):
    """Deactivate all currently active spine files
    
    Args:
        cur: Database cursor
        
    Note:
        Part of atomic operation to ensure only one active file
    """
    cur.execute(
        "UPDATE raw_files.manage_spine_raw_files SET active = FALSE WHERE active = TRUE"
    )
    deactivated_count = cur.rowcount
    
    if deactivated_count > 0:
        logger.info(f"🔄 Deactivated {deactivated_count} existing spine file(s)")


def insert_file_record(cur, file_hash: str, file_path: str):
    """Insert new file record and activate it
    
    Args:
        cur: Database cursor
        file_hash: SHA256 hash of the file
        file_path: Full path to saved file
    """
    cur.execute("""
        INSERT INTO raw_files.manage_spine_raw_files (
            file_hash, date_download, active, storage_location
        ) VALUES (%s, now(), TRUE, %s)
    """, (file_hash, file_path))
    
    logger.info(f"📝 Inserted new spine file record: {file_hash[:16]}...")


def validate_prerequisites():
    """Validate that required tables and directories exist
    
    Raises:
        Exception: If prerequisites are not met
    """
    # Test database connectivity and table existence
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_schema = 'raw_files' 
                    AND table_name = 'manage_spine_raw_files'
                """)
                
                if cur.fetchone()[0] == 0:
                    raise Exception("❌ Table raw_files.manage_spine_raw_files does not exist")
                
        logger.success("✅ Prerequisites validated")
        
    except psycopg2.Error as e:
        raise Exception(f"❌ Database connectivity failed: {e}")


def validate_output():
    """Validate that the operation completed successfully
    
    Raises:
        Exception: If validation fails
    """
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Check that exactly one file is active
                cur.execute(
                    "SELECT COUNT(*) FROM raw_files.manage_spine_raw_files WHERE active = TRUE"
                )
                active_count = cur.fetchone()[0]
                
                if active_count != 1:
                    raise Exception(f"❌ Expected 1 active spine file, found {active_count}")
                
                # Get details of active file
                cur.execute("""
                    SELECT file_hash, storage_location, date_download 
                    FROM raw_files.manage_spine_raw_files 
                    WHERE active = TRUE
                """)
                file_hash, storage_location, date_download = cur.fetchone()
                
                # Verify file exists on disk
                if not Path(storage_location).exists():
                    raise Exception(f"❌ Active file not found on disk: {storage_location}")
                
                logger.success(f"✅ Validation passed:")
                logger.info(f"📊 Active file: {file_hash[:16]}...")
                logger.info(f"📁 Location: {storage_location}")
                logger.info(f"📅 Downloaded: {date_download}")
                
    except psycopg2.Error as e:
        raise Exception(f"❌ Validation query failed: {e}")


def main():
    """Main processing function with comprehensive error handling"""
    logger.info("🚀 Starting spine metadata archival process...")
    
    try:
        # Validate prerequisites
        validate_prerequisites()
        
        # Fetch data from StatCan API
        metadata = fetch_spine_metadata()
        
        # Calculate hash for change detection
        file_hash = calculate_content_hash(metadata)
        
        # Process with atomic database operations
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Check if this version already exists
                if check_hash_exists(cur, file_hash):
                    logger.warning("⚠️ Duplicate spine metadata - no changes detected")
                    logger.info("🎉 Process completed - no action required")
                    return
                
                # Save file to disk first
                file_path = save_metadata_file(metadata, file_hash)
                
                # Atomic database update
                deactivate_existing_files(cur)
                insert_file_record(cur, file_hash, file_path)
                
                # Commit transaction
                conn.commit()
                logger.success("💾 Database transaction committed")
        
        # Validate successful completion
        validate_output()
        
        logger.success("🎉 Spine metadata archival completed successfully!")
        
    except requests.RequestException as e:
        logger.error(f"❌ API request failed: {e}")
        raise
    except psycopg2.Error as e:
        logger.error(f"❌ Database operation failed: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        raise


if __name__ == "__main__":
    main()

"""
Enhanced Spine Fetch Script - Statistics Canada ETL Pipeline
===========================================================

This script downloads and validates the complete list of available data cubes 
from Statistics Canada's Web Data Service API. It implements comprehensive 
validation to protect against data corruption and ensures only verified, 
complete responses are archived.

Key Features:
- Fetches complete cube catalog from getAllCubesListLite endpoint
- Validates response structure, cube count, and data quality
- Uses SHA-256 hashing for deduplication and change detection
- Maintains versioned archive of all spine snapshots
- Fails fast on validation errors to prevent corruption
- Preserves existing data when responses are invalid

Process Flow:
1. Download complete cube list from StatCan API
2. Validate response structure and data quality
3. Calculate content hash for deduplication
4. Save to versioned file archive if validation passes
5. Update database tracking with new active file

Protection Mechanisms:
- Response validation (structure, size, content)
- Hash-based deduplication prevents duplicate processing
- File versioning preserves historical snapshots
- Atomic database operations with rollback capability

Last Updated: June 2025
Author: Paul Verbrugge with Claude 3.5 Sonnet (v20241022)e
"""

# statcan/scripts/01_spine_fetch_raw.py

import os
import json
import hashlib
import requests
import psycopg2
from datetime import datetime
from loguru import logger
from pathlib import Path
from statcan.tools.config import DB_CONFIG

# Add file logging
logger.add("/app/logs/fetch_spine.log", rotation="10 MB", retention="7 days")

SPINE_URL = "https://www150.statcan.gc.ca/t1/wds/rest/getAllCubesListLite"
ARCHIVE_DIR = Path("/app/raw/metadata")

# Validation constants
MIN_EXPECTED_CUBES = 1000  # StatCan has thousands of cubes
REQUIRED_FIELDS = ['productId', 'cubeTitleEn', 'cubeStartDate']
OPTIONAL_FIELDS = ['cubeTitleFr', 'cubeEndDate', 'releaseTime', 'archived', 'subjectCode', 'surveyCode']


def fetch_json():
    """Fetch spine data from StatCan API with timeout and error handling"""
    logger.info(f"Requesting cube metadata from: {SPINE_URL}")
    try:
        resp = requests.get(SPINE_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        logger.info(f"✅ API response received: {len(str(resp.content))} bytes")
        return data
    except requests.exceptions.Timeout:
        logger.error("❌ API request timed out after 30 seconds")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ API request failed: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"❌ Invalid JSON response: {e}")
        raise


def validate_spine_response(data) -> bool:
    """Comprehensive validation of spine API response"""
    logger.info("🔍 Validating spine response...")
    
    # Check if response is a list
    if not isinstance(data, list):
        logger.error(f"❌ Response is not a list, got: {type(data)}")
        return False
    
    # Check minimum number of cubes
    cube_count = len(data)
    if cube_count < MIN_EXPECTED_CUBES:
        logger.error(f"❌ Only {cube_count} cubes returned - expected at least {MIN_EXPECTED_CUBES}")
        return False
    
    logger.info(f"✅ Cube count check passed: {cube_count} cubes")
    
    # Validate structure of first 10 records
    for i, cube in enumerate(data[:10]):
        if not isinstance(cube, dict):
            logger.error(f"❌ Cube {i} is not a dictionary: {type(cube)}")
            return False
        
        # Check required fields
        missing_required = [field for field in REQUIRED_FIELDS if field not in cube]
        if missing_required:
            logger.error(f"❌ Cube {i} (productId: {cube.get('productId', 'unknown')}) missing required fields: {missing_required}")
            return False
        
        # Validate productId is numeric
        try:
            product_id = int(cube['productId'])
            if product_id <= 0:
                logger.error(f"❌ Cube {i} has invalid productId: {product_id}")
                return False
        except (ValueError, TypeError):
            logger.error(f"❌ Cube {i} has non-numeric productId: {cube.get('productId')}")
            return False
        
        # Check title is not empty
        title = cube.get('cubeTitleEn', '').strip()
        if not title:
            logger.error(f"❌ Cube {i} (productId: {cube['productId']}) has empty English title")
            return False
    
    logger.info("✅ Structure validation passed for sample records")
    
    # Check for reasonable distribution of product IDs
    product_ids = [cube.get('productId') for cube in data[:100]]
    unique_ids = set(product_ids)
    if len(unique_ids) != len(product_ids):
        logger.error("❌ Duplicate product IDs detected in sample")
        return False
    
    # Check product ID ranges (StatCan uses 8-digit IDs starting with subject codes)
    valid_id_count = sum(1 for pid in product_ids if isinstance(pid, int) and 10000000 <= pid <= 99999999)
    if valid_id_count < len(product_ids) * 0.9:  # Allow 10% variance
        logger.error(f"❌ Too many invalid product ID formats: {valid_id_count}/{len(product_ids)}")
        return False
    
    logger.info("✅ Product ID validation passed")
    
    # Log summary statistics
    archived_count = sum(1 for cube in data if cube.get('archived') == 1)
    with_subjects = sum(1 for cube in data if cube.get('subjectCode'))
    
    logger.info(f"📊 Validation summary:")
    logger.info(f"   Total cubes: {cube_count}")
    logger.info(f"   Archived cubes: {archived_count}")
    logger.info(f"   Cubes with subjects: {with_subjects}")
    
    logger.success("✅ Spine response validation passed")
    return True


def hash_json(data: dict) -> str:
    """Generate consistent hash of JSON data"""
    b = json.dumps(data, sort_keys=True).encode("utf-8")
    return hashlib.sha256(b).hexdigest()


def save_file(data: dict, file_hash: str) -> str:
    """Save validated spine data to disk"""
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"spine_{file_hash[:16]}.json"
    file_path = ARCHIVE_DIR / filename
    
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"💾 Saved spine file: {file_path}")
        return str(file_path)
    except Exception as e:
        logger.error(f"❌ Failed to save file {file_path}: {e}")
        raise


def file_exists(cur, file_hash: str) -> bool:
    """Check if file hash already exists in database"""
    cur.execute("SELECT 1 FROM raw_files.manage_spine_raw_files WHERE file_hash = %s", (file_hash,))
    return cur.fetchone() is not None


def deactivate_existing(cur):
    """Deactivate all currently active spine files"""
    cur.execute("UPDATE raw_files.manage_spine_raw_files SET active = FALSE WHERE active = TRUE")
    deactivated = cur.rowcount
    if deactivated > 0:
        logger.info(f"🔄 Deactivated {deactivated} existing spine file(s)")


def insert_record(cur, file_hash: str, file_path: str):
    """Insert new spine file record and activate it"""
    deactivate_existing(cur)
    cur.execute("""
        INSERT INTO raw_files.manage_spine_raw_files (
            file_hash, date_download, active, storage_location
        ) VALUES (%s, now(), TRUE, %s)
    """, (file_hash, file_path))
    logger.info(f"📝 Registered new active spine file: {file_hash[:16]}")


def main():
    logger.info("🚀 Starting spine archive fetch...")

    try:
        # Fetch data from API
        data = fetch_json()
        
        # Validate response before proceeding
        if not validate_spine_response(data):
            logger.error("❌ Spine response validation failed - aborting")
            return
        
        # Calculate hash and check for duplicates
        file_hash = hash_json(data)
        logger.info(f"🔢 Calculated file hash: {file_hash[:16]}...")

        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                if file_exists(cur, file_hash):
                    logger.info("ℹ️  Duplicate spine file – already archived, no changes needed")
                    return

                # Save file and register in database
                file_path = save_file(data, file_hash)
                insert_record(cur, file_hash, file_path)
                conn.commit()
                
                logger.success(f"✅ Successfully archived new spine file: {file_path}")
                logger.info(f"📊 Contains {len(data)} cube definitions")

    except Exception as e:
        logger.exception(f"❌ Spine fetch pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()

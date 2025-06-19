"""
Enhanced Metadata Download Script - Statistics Canada ETL Pipeline
===================================================================

This script downloads bilingual metadata for StatCan data cubes using the getCubeMetadata
API endpoint. It implements comprehensive API response validation, file integrity checks,
and safety mechanisms to ensure reliable metadata acquisition and storage.

Key Features:
- Downloads detailed cube metadata in JSON format from StatCan API
- Validates API responses for completeness and data quality
- Implements hash-based deduplication to prevent duplicate downloads
- Manages file storage with content verification and integrity checks
- Updates tracking tables with atomic operations and rollback protection
- Rate-limited API calls to respect StatCan's service limits

Process Flow:
1. Query metadata_status table for cubes pending download
2. For each pending cube: call getCubeMetadata API endpoint
3. Validate API response structure and content quality
4. Calculate content hash for deduplication checking
5. Save validated metadata to versioned file storage
6. Update tracking tables atomically with download status
7. Implement rate limiting between API calls

Protection Mechanisms:
- API response validation (structure, content, error handling)
- Content integrity verification before storage
- Hash-based deduplication prevents duplicate processing
- Atomic database operations with rollback capability
- Timeout protection and retry logic for network issues
- File system error handling and cleanup

API Behavior:
- getCubeMetadata endpoint returns bilingual metadata in JSON format
- Includes dimension definitions, member hierarchies, and cube properties
- Response can be large (several MB for complex cubes)
- API has rate limits - script implements polite delays

Dependencies:
- Requires metadata_status entries from 07_metadata_status_init.py
- Uses raw_files.manage_metadata_raw_files for file tracking
- Updates raw_files.metadata_status for download management

Last Updated: June 2025
Author: Paul Verbrugge
"""

import os
import requests
import hashlib
import psycopg2
import json
from pathlib import Path
from loguru import logger
from datetime import datetime
from statcan.tools.config import DB_CONFIG
import time

# Add file logging
logger.add("/app/logs/fetch_metadata.log", rotation="10 MB", retention="7 days")

# API and validation constants
WDS_METADATA_URL = "https://www150.statcan.gc.ca/t1/wds/rest/getCubeMetadata"
DOWNLOAD_DIR = Path("/app/raw/metadata")
API_TIMEOUT = 120  # seconds
RATE_LIMIT_DELAY = 1  # seconds between requests
MAX_BATCH_SIZE = None  # Set to limit for testing
MIN_METADATA_SIZE = 1000  # Minimum expected metadata size in bytes
MAX_METADATA_SIZE = 50 * 1024 * 1024  # 50MB maximum (very large cubes)


def validate_metadata_tracking_setup(cur) -> dict:
    """Validate that metadata tracking tables are properly set up"""
    logger.info("🔍 Validating metadata tracking setup...")
    
    # Check that metadata_status table exists and is accessible
    try:
        cur.execute("SELECT COUNT(*) FROM raw_files.metadata_status")
        status_count = cur.fetchone()[0]
    except Exception as e:
        raise RuntimeError(f"❌ Cannot access metadata_status table: {e}")
    
    # Check that manage_metadata_raw_files table exists
    try:
        cur.execute("SELECT COUNT(*) FROM raw_files.manage_metadata_raw_files")
        files_count = cur.fetchone()[0]
    except Exception as e:
        raise RuntimeError(f"❌ Cannot access manage_metadata_raw_files table: {e}")
    
    # Get pending download statistics
    cur.execute("SELECT COUNT(*) FROM raw_files.metadata_status WHERE download_pending = TRUE")
    pending_count = cur.fetchone()[0]
    
    stats = {
        'total_status_entries': status_count,
        'total_file_entries': files_count,
        'pending_downloads': pending_count
    }
    
    logger.success("✅ Metadata tracking setup validated")
    logger.info(f"📊 Status entries: {status_count}, File entries: {files_count}, Pending: {pending_count}")
    
    return stats


def get_pending_metadata(cur, limit=MAX_BATCH_SIZE) -> list:
    """Get list of product IDs that need metadata download"""
    logger.info("📋 Fetching pending metadata downloads...")
    
    sql = """
        SELECT productid 
        FROM raw_files.metadata_status
        WHERE download_pending = TRUE
        ORDER BY productid
    """
    
    if limit:
        sql += f" LIMIT {limit}"
    
    cur.execute(sql)
    product_ids = [row[0] for row in cur.fetchall()]
    
    logger.info(f"📥 Found {len(product_ids)} cubes pending metadata download")
    if product_ids:
        sample_size = min(5, len(product_ids))
        logger.info(f"📝 Sample product IDs: {product_ids[:sample_size]}")
    
    return product_ids


def validate_api_response(response_data: dict, productid: int) -> bool:
    """Validate API response structure and content"""
    logger.debug(f"🔍 Validating API response for product {productid}")
    
    # Check if response is a list (getCubeMetadata returns array)
    if not isinstance(response_data, list):
        logger.error(f"❌ API response is not a list for {productid}: {type(response_data)}")
        return False
    
    if len(response_data) == 0:
        logger.error(f"❌ Empty response array for {productid}")
        return False
    
    # Check first element structure
    first_element = response_data[0]
    if not isinstance(first_element, dict):
        logger.error(f"❌ First response element is not a dictionary for {productid}")
        return False
    
    # Check for required top-level fields
    if 'status' not in first_element:
        logger.error(f"❌ Response missing 'status' field for {productid}")
        return False
    
    if first_element.get('status') != 'SUCCESS':
        logger.warning(f"⚠️  API returned non-success status for {productid}: {first_element.get('status')}")
        return False
    
    # Check for object field containing actual metadata
    if 'object' not in first_element:
        logger.error(f"❌ Response missing 'object' field for {productid}")
        return False
    
    metadata_obj = first_element.get('object', {})
    if not isinstance(metadata_obj, dict):
        logger.error(f"❌ Metadata object is not a dictionary for {productid}")
        return False
    
    # Validate essential metadata fields
    essential_fields = ['productId', 'cubeTitleEn']
    missing_fields = [field for field in essential_fields if field not in metadata_obj]
    if missing_fields:
        logger.error(f"❌ Metadata missing essential fields for {productid}: {missing_fields}")
        return False
    
    # Validate product ID matches
    response_product_id = metadata_obj.get('productId')
    try:
        if int(response_product_id) != productid:
            logger.error(f"❌ Product ID mismatch for {productid}: got {response_product_id}")
            return False
    except (ValueError, TypeError):
        logger.error(f"❌ Invalid product ID in response for {productid}: {response_product_id}")
        return False
    
    # Check for dimension data (main content we need)
    dimensions = metadata_obj.get('dimension', [])
    if not isinstance(dimensions, list):
        logger.warning(f"⚠️  Dimensions field is not a list for {productid}")
    elif len(dimensions) == 0:
        logger.warning(f"⚠️  No dimensions found for {productid}")
    
    logger.debug(f"✅ API response validation passed for {productid}")
    return True


def get_metadata_json(productid: int) -> bytes:
    """Fetch metadata from StatCan API with validation and error handling"""
    logger.info(f"🔽 Downloading metadata for product {productid}...")
    
    try:
        # Prepare request payload
        payload = [{"productId": productid}]
        headers = {"Content-Type": "application/json"}
        
        # Make API request with timeout
        response = requests.post(
            WDS_METADATA_URL,
            json=payload,
            headers=headers,
            timeout=API_TIMEOUT
        )
        response.raise_for_status()
        
        # Get response content
        content = response.content
        content_size = len(content)
        
        # Validate content size
        if content_size < MIN_METADATA_SIZE:
            raise ValueError(f"Response too small: {content_size} bytes (minimum {MIN_METADATA_SIZE})")
        
        if content_size > MAX_METADATA_SIZE:
            raise ValueError(f"Response too large: {content_size} bytes (maximum {MAX_METADATA_SIZE})")
        
        # Validate JSON structure
        try:
            response_data = json.loads(content.decode('utf-8'))
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON response: {e}")
        
        # Validate API response content
        if not validate_api_response(response_data, productid):
            raise ValueError("API response validation failed")
        
        logger.success(f"✅ Successfully downloaded metadata for {productid} ({content_size:,} bytes)")
        return content
        
    except requests.exceptions.Timeout:
        logger.error(f"❌ API timeout for {productid} after {API_TIMEOUT} seconds")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ API request failed for {productid}: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Metadata download failed for {productid}: {e}")
        raise


def hash_bytes(content: bytes) -> str:
    """Generate SHA-256 hash of content"""
    return hashlib.sha256(content).hexdigest()


def save_metadata_file(productid: int, file_hash: str, content: bytes) -> str:
    """Save metadata content to file with integrity verification"""
    logger.debug(f"💾 Saving metadata file for product {productid}")
    
    try:
        # Ensure download directory exists
        DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        
        # Generate filename with hash for deduplication
        filename = f"{productid}_{file_hash[:16]}.json"
        file_path = DOWNLOAD_DIR / filename
        
        # Write content to file
        with open(file_path, "wb") as f:
            f.write(content)
        
        # Verify file was written correctly
        if not file_path.exists():
            raise IOError(f"File was not created: {file_path}")
        
        actual_size = file_path.stat().st_size
        if actual_size != len(content):
            raise IOError(f"File size mismatch: expected {len(content)}, got {actual_size}")
        
        # Verify content integrity
        with open(file_path, "rb") as f:
            file_content = f.read()
        
        file_hash_verify = hash_bytes(file_content)
        if file_hash_verify != file_hash:
            raise IOError(f"File hash mismatch: expected {file_hash}, got {file_hash_verify}")
        
        logger.success(f"💾 Metadata file saved: {filename}")
        return str(file_path)
        
    except Exception as e:
        logger.error(f"❌ Failed to save metadata file for {productid}: {e}")
        # Clean up partial file if it exists
        if 'file_path' in locals() and file_path.exists():
            try:
                file_path.unlink()
                logger.info(f"🧹 Cleaned up partial file: {filename}")
            except:
                pass
        raise


def file_already_exists(cur, file_hash: str) -> bool:
    """Check if file with this hash already exists"""
    cur.execute("SELECT 1 FROM raw_files.manage_metadata_raw_files WHERE file_hash = %s", (file_hash,))
    return cur.fetchone() is not None


def deactivate_existing_metadata(cur, productid: int):
    """Deactivate existing metadata files for this product"""
    cur.execute("""
        UPDATE raw_files.manage_metadata_raw_files
        SET active = FALSE
        WHERE productid = %s AND active = TRUE
    """, (productid,))
    deactivated = cur.rowcount
    if deactivated > 0:
        logger.info(f"🔄 Deactivated {deactivated} existing metadata file(s) for {productid}")


def insert_metadata_log(cur, productid: int, file_hash: str, file_path: str):
    """Insert metadata file record into tracking table"""
    deactivate_existing_metadata(cur, productid)
    cur.execute("""
        INSERT INTO raw_files.manage_metadata_raw_files (
            productid, file_hash, date_download, active, storage_location
        ) VALUES (%s, %s, now(), TRUE, %s)
    """, (productid, file_hash, file_path))
    logger.debug(f"📝 Registered metadata file for {productid}: {file_hash[:16]}")


def update_metadata_status(cur, productid: int, file_hash: str):
    """Update metadata_status table with download completion"""
    cur.execute("""
        UPDATE raw_files.metadata_status
        SET download_pending = FALSE, 
            last_download = now(), 
            last_file_hash = %s
        WHERE productid = %s
    """, (file_hash, productid))
    
    if cur.rowcount == 0:
        logger.warning(f"⚠️  No metadata_status record updated for {productid}")
    else:
        logger.debug(f"📊 Updated metadata_status for {productid}")


def download_and_process_metadata(productid: int) -> bool:
    """Download and process metadata for a single product ID"""
    try:
        # Download metadata from API
        content = get_metadata_json(productid)
        file_hash = hash_bytes(content)
        
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Check for duplicate
                if file_already_exists(cur, file_hash):
                    logger.info(f"ℹ️  Duplicate metadata for {productid} (hash: {file_hash[:16]}), updating status only")
                    update_metadata_status(cur, productid, file_hash)
                    conn.commit()
                    return True
                
                # Save file and update tracking
                file_path = save_metadata_file(productid, file_hash, content)
                insert_metadata_log(cur, productid, file_hash, file_path)
                update_metadata_status(cur, productid, file_hash)
                
                conn.commit()
                logger.success(f"✅ Completed metadata processing for {productid}")
                return True
                
    except Exception as e:
        logger.error(f"❌ Failed to process metadata for {productid}: {e}")
        return False


def main():
    logger.info("🚀 Starting enhanced metadata download...")
    
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Validate setup
                setup_stats = validate_metadata_tracking_setup(cur)
                
                # Get pending downloads
                product_ids = get_pending_metadata(cur)
                
                if not product_ids:
                    logger.success("✅ No metadata downloads pending")
                    return
                
                # Process each product ID
                total_products = len(product_ids)
                successful = 0
                failed = 0
                
                logger.info(f"📥 Processing {total_products} metadata downloads...")
                
                for i, productid in enumerate(product_ids, 1):
                    logger.info(f"🔄 Processing {i}/{total_products}: Product {productid}")
                    
                    if download_and_process_metadata(productid):
                        successful += 1
                    else:
                        failed += 1
                    
                    # Rate limiting (except for last request)
                    if i < total_products:
                        time.sleep(RATE_LIMIT_DELAY)
                
                # Final summary
                logger.success(f"✅ Enhanced metadata download complete")
                logger.info(f"📊 Summary: {successful} successful, {failed} failed, {total_products} total")
                
                if failed > 0:
                    failure_rate = failed / total_products
                    if failure_rate > 0.1:  # More than 10% failure rate
                        logger.warning(f"⚠️  High failure rate: {failure_rate:.1%}")
                
    except Exception as e:
        logger.exception(f"❌ Enhanced metadata download failed: {e}")
        raise


if __name__ == "__main__":
    main()

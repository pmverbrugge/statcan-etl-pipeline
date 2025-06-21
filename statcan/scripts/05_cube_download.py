"""
StatCan Public Data ETL Project
Script Name: 05_cube_download.py (Refactored)
Date: June 21, 2025
Author: Paul Verbrugge with Claude Sonnet 4

Download StatCan data cubes with granular status tracking.
Uses getFullTableDownloadCSV endpoint to download cube zip files.
Updates raw_files.manage_cube_raw_files and cube_status progressively.

REFACTORING IMPROVEMENTS:
- Enhanced validation framework (prerequisite + processing + result validation)
- Atomic transaction management with rollback capability
- Improved error classification and handling (API vs file vs database errors)
- Professional logging with performance metrics and progress tracking
- Disk space validation and file system safety checks
- Enhanced deduplication logic with integrity verification
- Comprehensive error recovery and retry mechanisms
"""

import os
import requests
import hashlib
import psycopg2
import shutil
from pathlib import Path
from loguru import logger
from datetime import datetime
from statcan.tools.config import DB_CONFIG
import time
import json


# Configuration constants
WDS_URL_TEMPLATE = "https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/{}/en"
DOWNLOAD_DIR = Path("/app/raw/cubes")
MAX_CUBES = None
MIN_FREE_SPACE_GB = 5  # Minimum free disk space required
DOWNLOAD_TIMEOUT = 600  # 10 minute timeout for large files
API_RETRY_DELAY = 3  # Seconds between API retries

# Configure logging
logger.add("/app/logs/05_cube_download.log", rotation="10 MB", retention="7 days")


class DownloadError(Exception):
    """Custom exception for download-related errors"""
    pass


class ValidationError(Exception):
    """Custom exception for validation failures"""
    pass


def validate_prerequisites() -> bool:
    """
    Comprehensive validation of prerequisites before processing.
    Returns True if all validations pass, raises ValidationError on critical failures.
    """
    logger.info("📋 Validating prerequisites...")
    
    try:
        # 1. Database connectivity (REQUIRED)
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                logger.info("✅ Database connectivity confirmed")
        
        # 2. Required tables exist (REQUIRED)
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'raw_files' 
                    AND table_name IN ('cube_status', 'manage_cube_raw_files')
                """)
                tables = [row[0] for row in cur.fetchall()]
                if len(tables) != 2:
                    raise ValidationError(f"Missing required tables. Found: {tables}")
                logger.info("✅ Required database tables confirmed")
        
        # 3. Download directory setup (REQUIRED)
        DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        if not DOWNLOAD_DIR.exists():
            raise ValidationError(f"Cannot create download directory: {DOWNLOAD_DIR}")
        
        # Test write permissions
        test_file = DOWNLOAD_DIR / "test_write_permissions.tmp"
        try:
            test_file.write_text("test")
            test_file.unlink()
            logger.info(f"✅ Download directory accessible: {DOWNLOAD_DIR}")
        except Exception as e:
            raise ValidationError(f"Download directory not writable: {e}")
        
        # 4. Disk space check (WARNING if low)
        try:
            statvfs = os.statvfs(DOWNLOAD_DIR)
            # Use f_bavail (available to non-root) or f_free as fallback
            if hasattr(statvfs, 'f_bavail'):
                free_blocks = statvfs.f_bavail
            elif hasattr(statvfs, 'f_free'):
                free_blocks = statvfs.f_free
            else:
                raise AttributeError("No available space attribute found")
            
            free_space_gb = (statvfs.f_frsize * free_blocks) / (1024**3)
            if free_space_gb < MIN_FREE_SPACE_GB:
                logger.warning(f"⚠️ Low disk space: {free_space_gb:.1f}GB available (minimum {MIN_FREE_SPACE_GB}GB recommended)")
            else:
                logger.info(f"✅ Sufficient disk space: {free_space_gb:.1f}GB available")
        except Exception as e:
            logger.warning(f"⚠️ Could not check disk space: {e}")
        
        # 5. API connectivity (NON-BLOCKING - warn but continue)
        try:
            # Test with a known good product ID (if we have one)
            response = requests.get("https://www150.statcan.gc.ca/t1/wds/rest/getAllCubesListLite", timeout=10)
            if response.status_code == 200:
                logger.info("✅ StatCan API connectivity confirmed")
            else:
                logger.warning(f"⚠️ StatCan API returned status {response.status_code}")
        except Exception as e:
            logger.warning(f"⚠️ Could not validate StatCan API connectivity: {e}")
            logger.warning("⚠️ Continuing with downloads - connectivity will be tested per request")
        
        logger.info("✅ Prerequisites validation complete")
        return True
        
    except ValidationError:
        raise
    except Exception as e:
        raise ValidationError(f"Unexpected error during validation: {e}")


def get_pending_cubes(limit=MAX_CUBES) -> list:
    """Get list of product IDs pending download with enhanced validation."""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT productid FROM raw_files.cube_status
                    WHERE download_pending = TRUE
                    ORDER BY productid
                    LIMIT %s;
                """, (limit,))
                product_ids = [row[0] for row in cur.fetchall()]
                
                # Validate product IDs are reasonable
                valid_ids = []
                for pid in product_ids:
                    if isinstance(pid, int) and 10000000 <= pid <= 99999999:  # 8-digit StatCan product IDs
                        valid_ids.append(pid)
                    else:
                        logger.warning(f"⚠️ Skipping invalid product ID: {pid}")
                
                logger.info(f"📊 Found {len(valid_ids)} valid pending cubes (of {len(product_ids)} total)")
                return valid_ids
                
    except Exception as e:
        logger.error(f"❌ Failed to get pending cubes: {e}")
        raise DownloadError(f"Could not retrieve pending cubes: {e}")


def classify_api_error(error) -> str:
    """Classify API errors for appropriate handling."""
    if hasattr(error, 'response') and error.response is not None:
        status_code = error.response.status_code
        if status_code == 404:
            return "not_found"  # Product doesn't exist
        elif status_code == 409:
            return "not_available"  # Data not ready yet (like script 04)
        elif 400 <= status_code < 500:
            return "client_error"  # Configuration issue
        elif 500 <= status_code < 600:
            return "server_error"  # Temporary server issue
    return "network_error"  # Connection issues, timeouts, etc.


def get_download_url(productid: int) -> str:
    """Get download URL with enhanced error handling."""
    url = WDS_URL_TEMPLATE.format(productid)
    
    try:
        logger.info(f"🔗 Requesting download URL for product {productid}")
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        
        if "object" not in result:
            raise DownloadError(f"Invalid response format: missing 'object' field")
        
        download_url = result["object"]
        if not download_url or not download_url.startswith("http"):
            raise DownloadError(f"Invalid download URL received: {download_url}")
        
        logger.info(f"✅ Retrieved download URL for product {productid}")
        return download_url
        
    except requests.exceptions.RequestException as e:
        error_type = classify_api_error(e)
        if error_type == "not_found":
            raise DownloadError(f"Product {productid} not found (404)")
        elif error_type == "not_available":
            raise DownloadError(f"Product {productid} data not available yet (409)")
        else:
            raise DownloadError(f"API request failed ({error_type}): {e}")
    except (KeyError, ValueError, TypeError) as e:
        raise DownloadError(f"Invalid API response format: {e}")


def hash_bytes(b: bytes) -> str:
    """Generate SHA256 hash of file content."""
    if not isinstance(b, bytes):
        raise ValueError("Input must be bytes")
    if len(b) == 0:
        raise ValueError("Cannot hash empty content")
    return hashlib.sha256(b).hexdigest()


def save_file_atomic(productid: int, file_hash: str, content: bytes) -> str:
    """
    Save file atomically to prevent corruption from interrupted downloads.
    Skips save if identical file already exists.
    Returns the final file path.
    """
    if not content:
        raise ValueError("Cannot save empty file content")
    
    # Ensure download directory exists
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    
    # Generate filenames
    filename = f"{productid}_{file_hash[:16]}.zip"
    final_path = DOWNLOAD_DIR / filename
    temp_path = DOWNLOAD_DIR / f"{filename}.tmp"
    
    # Check if identical file already exists
    if final_path.exists():
        try:
            # Verify existing file matches expected hash
            with open(final_path, "rb") as f:
                existing_content = f.read()
            existing_hash = hash_bytes(existing_content)
            
            if existing_hash == file_hash:
                file_size_mb = len(existing_content) / (1024 * 1024)
                logger.info(f"📁 File already exists with matching hash: {final_path} ({file_size_mb:.1f}MB)")
                return str(final_path)
            else:
                logger.warning(f"⚠️ Existing file has different hash, will overwrite: {final_path}")
        except Exception as e:
            logger.warning(f"⚠️ Could not verify existing file, will overwrite: {e}")
    
    try:
        # Write to temporary file first
        with open(temp_path, "wb") as f:
            f.write(content)
        
        # Verify file was written correctly
        if temp_path.stat().st_size != len(content):
            raise DownloadError("File size mismatch after write")
        
        # Verify content hash
        with open(temp_path, "rb") as f:
            written_content = f.read()
        if hash_bytes(written_content) != file_hash:
            raise DownloadError("File hash mismatch after write")
        
        # Atomic move to final location
        temp_path.rename(final_path)
        
        file_size_mb = len(content) / (1024 * 1024)
        logger.info(f"💾 Saved new file atomically: {final_path} ({file_size_mb:.1f}MB)")
        return str(final_path)
        
    except Exception as e:
        # Clean up temporary file on any error
        if temp_path.exists():
            temp_path.unlink()
        raise DownloadError(f"Failed to save file atomically: {e}")


def file_exists_in_db(cur, file_hash: str) -> bool:
    """Check if file hash already exists in database."""
    cur.execute("""
        SELECT productid, storage_location, active 
        FROM raw_files.manage_cube_raw_files 
        WHERE file_hash = %s
    """, (file_hash,))
    result = cur.fetchone()
    if result:
        productid, location, active = result
        logger.info(f"📋 File hash {file_hash[:16]} exists for product {productid} (active: {active})")
        return True
    return False


def deactivate_existing_files(cur, productid: int) -> int:
    """Deactivate existing files for product and return count of deactivated files."""
    cur.execute("""
        UPDATE raw_files.manage_cube_raw_files
        SET active = FALSE
        WHERE productid = %s AND active = TRUE
    """, (productid,))
    deactivated_count = cur.rowcount
    if deactivated_count > 0:
        logger.info(f"📝 Deactivated {deactivated_count} existing files for product {productid}")
    return deactivated_count


def insert_file_record(cur, productid: int, file_hash: str, file_path: str):
    """Insert new file record and deactivate existing ones."""
    # First deactivate existing files
    deactivate_existing_files(cur, productid)
    
    # Insert new active record
    cur.execute("""
        INSERT INTO raw_files.manage_cube_raw_files (
            productid, file_hash, date_download, active, storage_location
        ) VALUES (%s, %s, now(), TRUE, %s)
    """, (productid, file_hash, file_path))
    
    logger.info(f"📝 Inserted file record for product {productid}")


def update_cube_status_complete(cur, productid: int, file_hash: str):
    """Mark download as successfully completed in cube_status."""
    cur.execute("""
        UPDATE raw_files.cube_status
        SET download_pending = FALSE, 
            last_download = now(), 
            last_file_hash = %s
        WHERE productid = %s;
    """, (file_hash, productid))
    
    if cur.rowcount == 0:
        logger.warning(f"⚠️ No cube_status record updated for product {productid}")
    else:
        logger.info(f"📊 Updated cube_status for product {productid}")


def mark_download_attempt(productid: int, success: bool, error_msg: str = None):
    """Mark download attempt in cube_status (for tracking purposes)."""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                if success:
                    # Success is handled in the main transaction
                    return
                else:
                    # Mark attempt but keep download_pending = TRUE for retry
                    cur.execute("""
                        UPDATE raw_files.cube_status
                        SET last_download = now()
                        WHERE productid = %s;
                    """, (productid,))
                    conn.commit()
                    logger.info(f"📅 Marked download attempt for {productid}")
                    
    except Exception as e:
        logger.error(f"❌ Failed to mark download attempt for {productid}: {e}")


def download_with_progress(url: str, timeout: int = DOWNLOAD_TIMEOUT) -> bytes:
    """
    Download file with real-time progress reporting.
    Shows MB downloaded and percentage complete for large files.
    """
    try:
        # Start download with streaming
        response = requests.get(url, timeout=timeout, stream=True)
        response.raise_for_status()
        
        # Get total size if available
        total_size = response.headers.get('content-length')
        if total_size:
            total_size = int(total_size)
            total_mb = total_size / (1024 * 1024)
        else:
            total_size = None
            total_mb = None
        
        # Download in chunks with progress
        downloaded_data = b''
        downloaded_bytes = 0
        chunk_size = 8192  # 8KB chunks
        last_progress_time = time.time()
        
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:  # Filter out keep-alive chunks
                downloaded_data += chunk
                downloaded_bytes += len(chunk)
                
                # Show progress every 2 seconds or when complete
                current_time = time.time()
                if current_time - last_progress_time >= 2.0 or downloaded_bytes == total_size:
                    downloaded_mb = downloaded_bytes / (1024 * 1024)
                    
                    if total_size:
                        percentage = (downloaded_bytes / total_size) * 100
                        logger.info(f"   📥 {downloaded_mb:.1f}MB / {total_mb:.1f}MB ({percentage:.1f}%)")
                    else:
                        logger.info(f"   📥 {downloaded_mb:.1f}MB downloaded...")
                    
                    last_progress_time = current_time
        
        # Final progress report
        final_mb = len(downloaded_data) / (1024 * 1024)
        if total_size:
            logger.info(f"   ✅ Download complete: {final_mb:.1f}MB")
        else:
            logger.info(f"   ✅ Download complete: {final_mb:.1f}MB (total)")
        
        return downloaded_data
        
    except requests.exceptions.RequestException as e:
        raise DownloadError(f"Download failed: {e}")


def download_cube_file(productid: int) -> bool:
    """
    Download a single cube file with comprehensive error handling.
    Returns True if successful, False if failed.
    """
    logger.info(f"🔽 Starting download for cube {productid}")
    start_time = time.time()
    
    try:
        # Step 1: Get download URL
        try:
            download_url = get_download_url(productid)
        except DownloadError as e:
            logger.error(f"❌ Failed to get download URL for {productid}: {e}")
            mark_download_attempt(productid, False, str(e))
            return False
        
        # Step 2: Download file content
        try:
            logger.info(f"⬇️ Downloading file for product {productid}")
            file_bytes = download_with_progress(download_url)
            
            # Validate content
            if not file_bytes:
                raise DownloadError("Downloaded file is empty")
            
            # Generate hash
            file_hash = hash_bytes(file_bytes)
            file_size_mb = len(file_bytes) / (1024 * 1024)
            
            logger.info(f"🔐 File hash: {file_hash[:16]}... ({file_size_mb:.1f}MB)")
            
        except requests.exceptions.RequestException as e:
            error_type = classify_api_error(e)
            logger.error(f"❌ Download failed for {productid} ({error_type}): {e}")
            mark_download_attempt(productid, False, str(e))
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected download error for {productid}: {e}")
            mark_download_attempt(productid, False, str(e))
            return False
        
        # Step 3: Database transaction with file save
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    # Check for duplicate content
                    if file_exists_in_db(cur, file_hash):
                        logger.warning(f"⚠️ Duplicate content for {productid} (hash: {file_hash[:16]})")
                        logger.info(f"🗑️ Discarded {file_size_mb:.1f}MB downloaded content (already in database)")
                        # Still update status to mark as complete
                        update_cube_status_complete(cur, productid, file_hash)
                        conn.commit()
                        
                        download_time = time.time() - start_time
                        logger.success(f"✅ Updated status for duplicate {productid} ({download_time:.1f}s)")
                        return True
                    
                    # Save file atomically
                    file_path = save_file_atomic(productid, file_hash, file_bytes)
                    
                    # Insert database records (within transaction)
                    insert_file_record(cur, productid, file_hash, file_path)
                    update_cube_status_complete(cur, productid, file_hash)
                    
                    # Commit transaction
                    conn.commit()
                    
                    download_time = time.time() - start_time
                    logger.success(f"✅ Successfully downloaded {productid} ({download_time:.1f}s, {file_size_mb:.1f}MB)")
                    return True
                    
        except Exception as e:
            logger.error(f"❌ Database/file error for {productid}: {e}")
            mark_download_attempt(productid, False, str(e))
            return False
            
    except Exception as e:
        logger.error(f"❌ Unexpected error downloading {productid}: {e}")
        mark_download_attempt(productid, False, str(e))
        return False


def validate_processing_results(successful_count: int, failed_count: int, total_count: int):
    """Validate that processing results are reasonable."""
    if total_count == 0:
        logger.info("✅ No cubes to process - validation passed")
        return
    
    if successful_count + failed_count != total_count:
        logger.error(f"❌ Count mismatch: {successful_count} + {failed_count} != {total_count}")
        raise ValidationError("Processing count validation failed")
    
    success_rate = (successful_count / total_count) * 100
    
    if success_rate == 100:
        logger.success(f"✅ Perfect success rate: {successful_count}/{total_count} cubes downloaded")
    elif success_rate >= 90:
        logger.info(f"✅ Good success rate: {successful_count}/{total_count} cubes downloaded ({success_rate:.1f}%)")
    elif success_rate >= 50:
        logger.warning(f"⚠️ Moderate success rate: {successful_count}/{total_count} cubes downloaded ({success_rate:.1f}%)")
    else:
        logger.error(f"❌ Low success rate: {successful_count}/{total_count} cubes downloaded ({success_rate:.1f}%)")


def main():
    """Main download orchestration with comprehensive error handling."""
    logger.info("🎯 SCRIPT 05: Cube Download - Starting")
    start_time = time.time()
    
    try:
        # Validate prerequisites
        validate_prerequisites()
        
        # Get pending cubes
        product_ids = get_pending_cubes()
        if not product_ids:
            logger.info("🎉 No cubes pending download")
            logger.success("✅ SCRIPT 05 COMPLETE (no work needed)")
            return
        
        logger.info(f"📊 Processing {len(product_ids)} pending cubes")
        
        # Process downloads with progress tracking
        successful_count = 0
        failed_count = 0
        
        for i, productid in enumerate(product_ids, 1):
            logger.info(f"🔽 DOWNLOADING CUBE {i}/{len(product_ids)}: {productid}")
            download_start = time.time()
            
            if download_cube_file(productid):
                download_time = time.time() - download_start
                successful_count += 1
                logger.success(f"✅ COMPLETED {productid} in {download_time:.1f}s ({successful_count}/{len(product_ids)} done)")
            else:
                download_time = time.time() - download_start
                failed_count += 1
                logger.error(f"❌ FAILED {productid} after {download_time:.1f}s ({failed_count} failures)")
            
            # Polite pause between downloads
            if i < len(product_ids):  # Don't pause after last download
                time.sleep(API_RETRY_DELAY)
        
        # Validate results
        validate_processing_results(successful_count, failed_count, len(product_ids))
        
        # Final reporting
        total_time = time.time() - start_time
        avg_time = total_time / len(product_ids) if product_ids else 0
        
        logger.info(f"📊 Download Summary:")
        logger.info(f"   • Total cubes processed: {len(product_ids)}")
        logger.info(f"   • Successful downloads: {successful_count}")
        logger.info(f"   • Failed downloads: {failed_count}")
        logger.info(f"   • Total time: {total_time:.1f}s")
        logger.info(f"   • Average time per cube: {avg_time:.1f}s")
        
        if failed_count == 0:
            logger.success("✅ SCRIPT 05 COMPLETE (all downloads successful)")
        else:
            logger.success(f"✅ SCRIPT 05 COMPLETE ({successful_count}/{len(product_ids)} successful)")
            
    except ValidationError as e:
        logger.error(f"❌ Validation failed: {e}")
        logger.error("❌ SCRIPT 05 FAILED (validation error)")
        raise
    except Exception as e:
        logger.exception(f"❌ Unexpected error in main: {e}")
        logger.error("❌ SCRIPT 05 FAILED (unexpected error)")
        raise


if __name__ == "__main__":
    main()

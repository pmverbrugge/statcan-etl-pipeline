"""
Statcan Public Data ETL Project
Script: 06_cube_verify_files.py
Date: June 21, 2025
Author: Paul Verbrugge with Claude Sonnet 4

Verify raw cube files against database log and reset download flags if missing or corrupted.
Enhanced with chunked hashing, progress reporting, and error resilience.
"""

import os
import hashlib
import psycopg2
import time
from pathlib import Path
from statcan.tools.config import DB_CONFIG
from loguru import logger

DOWNLOAD_DIR = Path("/app/raw/cubes")
HASH_CHUNK_SIZE = 8192  # 8KB chunks for memory efficiency

logger.add("/app/logs/verify_raw_files.log", rotation="10 MB", retention="7 days")


def hash_file_chunked(file_path: Path) -> str:
    """Calculate SHA-256 hash using chunked reading for memory efficiency."""
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(HASH_CHUNK_SIZE):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()


def verify_files():
    """Verify all files with progress reporting and error resilience."""
    start_time = time.time()
    
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT productid, file_hash, storage_location
                FROM raw_files.manage_cube_raw_files
                WHERE active = TRUE
                ORDER BY productid
            """)
            rows = cur.fetchall()
            total_files = len(rows)
            
            if total_files == 0:
                logger.info("📭 No active files found to verify")
                return
                
            logger.info(f"🔍 Verifying {total_files} files...")
            
            verified_count = 0
            missing_count = 0
            corrupted_count = 0
            error_count = 0

            for i, (productid, file_hash, file_path) in enumerate(rows, 1):
                try:
                    # Progress reporting every 50 files or at end
                    if i % 50 == 0 or i == total_files:
                        logger.info(f"📊 Progress: {i}/{total_files} files processed")
                    
                    p = Path(file_path)
                    
                    # Handle missing files
                    if not p.exists():
                        logger.warning(f"❌ File missing for {productid}: {file_path}")
                        
                        # Atomic cleanup for missing file
                        cur.execute("DELETE FROM raw_files.manage_cube_raw_files WHERE productid = %s AND file_hash = %s", 
                                  (productid, file_hash))
                        cur.execute("UPDATE raw_files.cube_status SET download_pending = TRUE WHERE productid = %s", 
                                  (productid,))
                        conn.commit()
                        missing_count += 1
                        continue

                    # Verify hash with chunked reading
                    actual_hash = hash_file_chunked(p)
                    
                    if actual_hash != file_hash:
                        logger.error(f"⚠️ Hash mismatch for {productid}: expected {file_hash[:16]}..., got {actual_hash[:16]}...")
                        
                        # Remove corrupted file and cleanup database
                        try:
                            p.unlink()
                            logger.warning(f"🗑️ Deleted corrupted file: {file_path}")
                        except Exception as e:
                            logger.error(f"💥 Failed to delete corrupted file: {file_path} - {e}")
                        
                        # Atomic cleanup for corrupted file  
                        cur.execute("DELETE FROM raw_files.manage_cube_raw_files WHERE productid = %s AND file_hash = %s", 
                                  (productid, file_hash))
                        cur.execute("UPDATE raw_files.cube_status SET download_pending = TRUE WHERE productid = %s", 
                                  (productid,))
                        conn.commit()
                        corrupted_count += 1
                    else:
                        logger.debug(f"✅ Verified {productid}: {p.name}")
                        verified_count += 1
                        
                except Exception as e:
                    logger.error(f"💥 Error processing {productid}: {e}")
                    error_count += 1
                    # Continue with next file instead of failing entire script

            # Summary statistics
            elapsed_time = time.time() - start_time
            logger.success(f"🎯 Verification complete in {elapsed_time:.1f}s")
            logger.info(f"📊 Results: {verified_count} verified, {missing_count} missing, {corrupted_count} corrupted, {error_count} errors")
            
            if missing_count > 0 or corrupted_count > 0:
                logger.info(f"🔧 {missing_count + corrupted_count} files marked for re-download")


def main():
    """Main entry point for file verification script."""
    logger.info("🔍 SCRIPT 06: Cube File Verification - Starting")
    
    try:
        verify_files()
        logger.success("✅ SCRIPT 06 COMPLETE")
    except Exception as e:
        logger.exception(f"💥 Verification failed: {e}")
        return False
    
    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

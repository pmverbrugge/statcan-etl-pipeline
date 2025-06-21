"""
Statcan Public Data ETL Project
Script: 05_cube_download.py  
Date: June 21, 2025
Author: Paul Verbrugge with Claude Sonnet 4

Download StatCan data cubes with granular status tracking and hard link backups.
Enhanced with zero-cost hard link backup system for file protection.
Uses getFullTableDownloadCSV endpoint to download cube zip files.
Updates raw_files.manage_cube_raw_files and cube_status progressively.
"""

import os
import requests
import hashlib
import psycopg2
from pathlib import Path
from loguru import logger
from datetime import datetime
from statcan.tools.config import DB_CONFIG
import time


WDS_URL_TEMPLATE = "https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/{}/en"
DOWNLOAD_DIR = Path("/app/raw/cubes")
BACKUP_DIR = Path("/app/raw/cubes/.hardlink_backups")
MAX_CUBES = None

logger.add("/app/logs/fetch_cubes.log", rotation="10 MB", retention="7 days")


def get_pending_cubes(limit=MAX_CUBES):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT productid FROM raw_files.cube_status
                WHERE download_pending = TRUE
                ORDER BY productid
                LIMIT %s;
            """, (limit,))
            return [row[0] for row in cur.fetchall()]


def mark_download_started(productid: int):
    """Mark that download has started for a productid"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE raw_files.cube_status
                SET last_download = now()
                WHERE productid = %s;
            """, (productid,))
            conn.commit()
            logger.info(f"🚀 Marked download started for {productid}")


def mark_download_failed(productid: int, error_msg: str):
    """Mark that download failed for a productid"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Keep download_pending = TRUE so it gets retried
            # Update last_download to track the attempt
            cur.execute("""
                UPDATE raw_files.cube_status
                SET last_download = now()
                WHERE productid = %s;
            """, (productid,))
            conn.commit()
            logger.error(f"❌ Marked download failed for {productid}: {error_msg}")


def get_download_url(productid: int) -> str:
    url = WDS_URL_TEMPLATE.format(productid)
    resp = requests.get(url)
    resp.raise_for_status()
    result = resp.json()
    return result["object"]


def hash_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def create_hard_link_backup(primary_path: Path, productid: int, file_hash: str) -> str:
    """Create hard link backup and return backup path."""
    try:
        # Ensure backup directory exists
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        
        # Create backup filename with hash prefix for uniqueness
        backup_filename = f"{productid}_{file_hash[:16]}.zip"
        backup_path = BACKUP_DIR / backup_filename
        
        # Remove any existing backup for this product
        cleanup_old_backups(productid)
        
        # Create hard link (zero storage cost)
        backup_path.hardlink_to(primary_path)
        
        logger.info(f"🔗 Created hard link backup: {backup_path}")
        return str(backup_path)
        
    except Exception as e:
        logger.warning(f"⚠️ Failed to create hard link backup for {productid}: {e}")
        return None


def cleanup_old_backups(productid: int):
    """Remove old backup files for this product from filesystem."""
    try:
        if not BACKUP_DIR.exists():
            return
            
        # Find and remove old backup files for this productid
        pattern = f"{productid}_*.zip"
        for old_backup in BACKUP_DIR.glob(pattern):
            try:
                old_backup.unlink()
                logger.debug(f"🗑️ Removed old backup: {old_backup}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to remove old backup {old_backup}: {e}")
                
    except Exception as e:
        logger.warning(f"⚠️ Failed to cleanup old backups for {productid}: {e}")


def save_file_with_backup(productid: int, file_hash: str, content: bytes) -> tuple[str, str]:
    """Save file and create hard link backup. Returns (primary_path, backup_path)."""
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    
    # Save primary file
    filename = f"{productid}_{file_hash[:16]}.zip"
    primary_path = DOWNLOAD_DIR / filename
    
    with open(primary_path, "wb") as f:
        f.write(content)
    
    logger.info(f"💾 Saved primary file: {primary_path}")
    
    # Create hard link backup
    backup_path = create_hard_link_backup(primary_path, productid, file_hash)
    
    return str(primary_path), backup_path


def file_exists(cur, file_hash: str) -> bool:
    cur.execute("SELECT 1 FROM raw_files.manage_cube_raw_files WHERE file_hash = %s", (file_hash,))
    return cur.fetchone() is not None


def deactivate_existing(cur, productid: int):
    """Deactivate existing records and cleanup old backup references."""
    cur.execute("""
        UPDATE raw_files.manage_cube_raw_files
        SET active = FALSE
        WHERE productid = %s AND active = TRUE
    """, (productid,))


def insert_log_with_backup(cur, productid: int, file_hash: str, file_path: str, backup_path: str):
    """Insert new file record with backup location."""
    deactivate_existing(cur, productid)
    cur.execute("""
        INSERT INTO raw_files.manage_cube_raw_files (
            productid, file_hash, date_download, active, storage_location, backup_storage_location
        ) VALUES (%s, %s, now(), TRUE, %s, %s)
    """, (productid, file_hash, file_path, backup_path))


def update_status_complete(cur, productid: int):
    """Mark download as successfully completed"""
    cur.execute("""
        UPDATE raw_files.cube_status
        SET download_pending = FALSE, 
            last_download = now(), 
            last_file_hash = (
                SELECT file_hash FROM raw_files.manage_cube_raw_files
                WHERE productid = %s AND active = TRUE
            )
        WHERE productid = %s;
    """, (productid, productid))


def download_and_log(productid: int):
    logger.info(f"🔽 Starting download for cube {productid}...")
    
    # Step 1: Mark download started
    try:
        mark_download_started(productid)
    except Exception as e:
        logger.error(f"❌ Failed to mark download started for {productid}: {e}")
        return
    
    # Step 2: Get download URL
    try:
        url = get_download_url(productid)
        logger.info(f"📡 Got download URL for {productid}")
    except Exception as e:
        mark_download_failed(productid, f"Failed to get download URL: {e}")
        return
    
    # Step 3: Download file content
    try:
        resp = requests.get(url, timeout=300)  # 5 minute timeout
        resp.raise_for_status()
        file_bytes = resp.content
        file_hash = hash_bytes(file_bytes)
        logger.info(f"⬇️ Downloaded {len(file_bytes)} bytes for {productid}, hash: {file_hash[:16]}")
    except Exception as e:
        mark_download_failed(productid, f"Failed to download file: {e}")
        return
    
    # Step 4: Check for duplicates and save/log
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                if file_exists(cur, file_hash):
                    logger.warning(f"⚠️ Duplicate file for {productid} (hash: {file_hash[:16]}), updating status only")
                    update_status_complete(cur, productid)
                    conn.commit()
                    logger.success(f"✅ Updated status for duplicate {productid}")
                    return
                
                # Save file to disk with hard link backup
                file_path, backup_path = save_file_with_backup(productid, file_hash, file_bytes)
                
                # Log file in database with backup location
                insert_log_with_backup(cur, productid, file_hash, file_path, backup_path)
                logger.info(f"📝 Logged file for {productid} in database with backup reference")
                
                # Mark download complete
                update_status_complete(cur, productid)
                conn.commit()
                
                backup_status = "with backup" if backup_path else "without backup"
                logger.success(f"✅ Completed download and logging for {productid} {backup_status}")
                
    except Exception as e:
        mark_download_failed(productid, f"Failed to save/log file: {e}")
        return


def validate_backup_system():
    """Validate that hard link backup system is working."""
    logger.info("🔍 Validating hard link backup system...")
    
    # Check if backup directory is writable
    try:
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        test_file = BACKUP_DIR / ".test_hardlink"
        test_file.write_text("test")
        
        # Test hard link creation
        test_link = BACKUP_DIR / ".test_hardlink_link"
        test_link.hardlink_to(test_file)
        
        # Verify both files have same inode (true hard link)
        if test_file.stat().st_ino == test_link.stat().st_ino:
            logger.success("✅ Hard link backup system validated")
        else:
            logger.warning("⚠️ Hard link creation failed - files have different inodes")
            
        # Cleanup test files
        test_file.unlink(missing_ok=True)
        test_link.unlink(missing_ok=True)
        
    except Exception as e:
        logger.warning(f"⚠️ Hard link backup validation failed: {e}")
        logger.info("💡 Continuing without hard link backups")


def main():
    logger.info("🚀 SCRIPT 05: Cube Download with Hard Link Backups - Starting")
    
    # Validate backup system
    validate_backup_system()
    
    try:
        product_ids = get_pending_cubes()
        if not product_ids:
            logger.info("🎉 No cubes pending download.")
            return
        
        logger.info(f"📋 Found {len(product_ids)} cubes pending download")
        
        for i, pid in enumerate(product_ids, 1):
            logger.info(f"🔄 Processing cube {i}/{len(product_ids)}: {pid}")
            try:
                download_and_log(pid)
                time.sleep(2)  # polite pause
            except Exception as e:
                logger.error(f"❌ Unexpected error processing cube {pid}: {e}")
                # Continue with next cube rather than stopping entire batch
                continue
                
        logger.success("✅ SCRIPT 05 COMPLETE: Batch download complete with hard link backups")
    except Exception as e:
        logger.exception(f"❌ Download pipeline failed: {e}")


if __name__ == "__main__":
    main()

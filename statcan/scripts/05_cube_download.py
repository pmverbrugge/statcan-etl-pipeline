#!/usr/bin/env python3
"""
Statistics Canada Cube Data Download Manager
===========================================

Script:     05_cube_download.py
Purpose:    Automated download and tracking of StatCan data cube ZIP files
Author:     Paul Verbrugge with Claude Sonnet 4 (Anthropic)
Created:    2025
Updated:    June 2025

Overview:
--------
This script manages the systematic download of Statistics Canada data cubes using 
the Web Data Service (WDS) API. It processes cubes marked as 'download_pending' in 
the cube_status table, downloads compressed CSV files via the getFullTableDownloadCSV 
endpoint, and maintains comprehensive file tracking with hash-based deduplication.

Key Features:
------------
• Granular status tracking with progressive updates during download process
• Content-based deduplication using SHA-256 file hashing
• Robust error handling with per-cube failure isolation
• Polite API usage with 2-second delays between requests
• Comprehensive logging with emoji indicators for easy monitoring
• Atomic database operations with rollback capability
• File versioning and archival of superseded downloads

Data Flow:
---------
1. Query raw_files.cube_status for cubes with download_pending = TRUE
2. For each pending cube:
   a. Mark download initiation timestamp
   b. Retrieve download URL from WDS API
   c. Download and hash file content
   d. Check for duplicate files using hash comparison
   e. Save file to /app/raw/cubes/ with hash-based naming
   f. Update raw_files.manage_cube_raw_files with file metadata
   g. Mark cube_status as complete (download_pending = FALSE)

Database Tables Modified:
------------------------
• raw_files.cube_status - Download completion tracking
• raw_files.manage_cube_raw_files - File inventory and metadata

API Endpoints Used:
------------------
• GET /t1/wds/rest/getFullTableDownloadCSV/{productid}/en

File Storage:
------------
• Location: /app/raw/cubes/
• Naming: {productid}_{hash_prefix}.zip
• Retention: Managed by file verification script (06_cube_verify_files.py)

Error Handling:
--------------
• Network timeouts: 5-minute download timeout per file
• API failures: Logged with retry capability maintained
• Disk I/O errors: Isolated per-cube with batch continuation
• Database errors: Transactional rollback with error logging

Integration:
-----------
• Preceded by: 04_cube_status_update.py (change detection)
• Followed by: 06_cube_verify_files.py (integrity verification)
• Monitored via: /app/logs/fetch_cubes.log

Performance Notes:
-----------------
• Processes all pending cubes in single batch
• 2-second delay between downloads for API politeness
• Duplicate detection prevents redundant storage
• Progressive status updates enable restart capability

Usage:
------
python 05_cube_download.py

Environment Requirements:
------------------------
• Network access to Statistics Canada WDS API
• Write permissions to /app/raw/cubes/ directory
• PostgreSQL connection via statcan.tools.config.DB_CONFIG
• Sufficient disk space for compressed cube files (typically 1KB-100MB each)
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


def save_file(productid: int, file_hash: str, content: bytes) -> str:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"{productid}_{file_hash[:16]}.zip"
    file_path = DOWNLOAD_DIR / filename
    with open(file_path, "wb") as f:
        f.write(content)
    return str(file_path)


def file_exists(cur, file_hash: str) -> bool:
    cur.execute("SELECT 1 FROM raw_files.manage_cube_raw_files WHERE file_hash = %s", (file_hash,))
    return cur.fetchone() is not None


def deactivate_existing(cur, productid: int):
    cur.execute("""
        UPDATE raw_files.manage_cube_raw_files
        SET active = FALSE
        WHERE productid = %s AND active = TRUE
    """, (productid,))


def insert_log(cur, productid: int, file_hash: str, file_path: str):
    deactivate_existing(cur, productid)
    cur.execute("""
        INSERT INTO raw_files.manage_cube_raw_files (
            productid, file_hash, date_download, active, storage_location
        ) VALUES (%s, %s, now(), TRUE, %s)
    """, (productid, file_hash, file_path))


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
                
                # Save file to disk
                file_path = save_file(productid, file_hash, file_bytes)
                logger.info(f"💾 Saved file for {productid} to {file_path}")
                
                # Log file in database
                insert_log(cur, productid, file_hash, file_path)
                logger.info(f"📝 Logged file for {productid} in database")
                
                # Mark download complete
                update_status_complete(cur, productid)
                conn.commit()
                logger.success(f"✅ Completed download and logging for {productid}")
                
    except Exception as e:
        mark_download_failed(productid, f"Failed to save/log file: {e}")
        return


def main():
    logger.info("🚀 Starting cube fetch script...")
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
                
        logger.info("✅ Batch download complete.")
    except Exception as e:
        logger.exception(f"❌ Download pipeline failed: {e}")


if __name__ == "__main__":
    main()

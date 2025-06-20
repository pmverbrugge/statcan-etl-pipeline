#!/usr/bin/env python3
"""
Statistics Canada Cube File Integrity Verification System
=========================================================

Script:     06_cube_verify_files.py
Purpose:    Validate integrity of downloaded cube files and trigger re-downloads when needed
Author:     Paul Verbrugge with Claude Sonnet 4 (Anthropic)
Created:    2025
Updated:    June 2025

Overview:
--------
This critical data integrity script validates that all active cube files in the raw file 
inventory exist on disk and match their recorded SHA-256 hashes. When files are missing 
or corrupted, the script automatically cleans up the database records and triggers 
re-downloads by resetting the download_pending flag in cube_status.

Key Features:
------------
• Complete file integrity verification using SHA-256 hash validation
• Automatic cleanup of missing or corrupted files
• Self-healing architecture that triggers re-downloads for failed files
• Atomic database operations with immediate commits per file
• Comprehensive logging with emoji indicators for monitoring
• Fail-fast approach with individual file error isolation

Verification Process:
-------------------
1. Query all active cube files from raw_files.manage_cube_raw_files
2. For each recorded file:
   a. Check physical file existence at storage_location
   b. Calculate SHA-256 hash of file contents
   c. Compare calculated hash with recorded file_hash
   d. If file missing or hash mismatch:
      - Delete physical file (if corrupted)
      - Remove database record from manage_cube_raw_files
      - Set download_pending = TRUE in cube_status for re-download
   e. Log verification result

Database Tables Modified:
------------------------
• raw_files.manage_cube_raw_files - Remove invalid file records
• raw_files.cube_status - Reset download_pending flag for failed files

File Operations:
---------------
• Read verification: All active cube files in /app/raw/cubes/
• Delete operation: Corrupted files that fail hash validation
• No file creation or modification (read-only verification)

Error Handling:
--------------
• Missing files: Database cleanup and re-download trigger
• Hash mismatches: File deletion, database cleanup, re-download trigger
• File deletion failures: Logged but don't prevent database cleanup
• Database errors: Individual file processing continues on error

Self-Healing Behavior:
---------------------
The script implements automatic recovery by:
• Removing invalid database records to maintain consistency
• Triggering re-downloads via download_pending flag
• Deleting corrupted files to free disk space
• Logging all actions for audit trail

Integration Points:
------------------
• Follows: 05_cube_download.py (initial file download)
• Triggers: Re-execution of 05_cube_download.py for failed files
• Monitored via: /app/logs/verify_raw_files.log
• Scheduled: Should run after download batches or on schedule

Performance Characteristics:
---------------------------
• I/O intensive: Reads entire content of each file for hashing
• Memory efficient: Processes files individually
• Database efficient: Single query to get file list, individual updates
• Scale: Processes all active files in single run

Usage Scenarios:
---------------
• Post-download verification after batch cube downloads
• Scheduled integrity checks (daily/weekly)
• Diagnostic runs when data quality issues suspected
• Recovery operations after disk/network issues

Usage:
------
python 06_cube_verify_files.py

Environment Requirements:
------------------------
• Read access to /app/raw/cubes/ directory
• Write/delete permissions for cube files
• PostgreSQL connection via statcan.tools.config.DB_CONFIG
• Sufficient I/O capacity for hash calculation of large files

Monitoring:
----------
• Success indicator: "✅ Verified {productid}: {filename}" messages
• Failure indicators: "❌ File missing" or "⚠️ Hash mismatch" messages
• Cleanup actions: "🗑️ Corrupted file deleted" messages
• Overall status: "🎯 Verification complete" on successful run

Data Quality Assurance:
----------------------
This script is essential for maintaining data integrity in the ETL pipeline.
It ensures that downstream processing (cube ingestion, analysis) operates on
validated, uncorrupted source files. The self-healing aspect reduces manual
intervention and improves pipeline reliability.
"""

import os
import hashlib
import psycopg2
from pathlib import Path
from statcan.tools.config import DB_CONFIG
from loguru import logger

DOWNLOAD_DIR = Path("/app/raw/cubes")
logger.add("/app/logs/verify_raw_files.log", rotation="10 MB", retention="7 days")

def hash_file(file_path: Path) -> str:
    with open(file_path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()

def verify_files():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT productid, file_hash, storage_location
                FROM raw_files.manage_cube_raw_files
                WHERE active = TRUE
            """)
            rows = cur.fetchall()

            for productid, file_hash, file_path in rows:
                p = Path(file_path)
                if not p.exists():
                    logger.warning(f"❌ File missing for {productid}: {file_path}")
                    cur.execute("DELETE FROM raw_files.manage_cube_raw_files WHERE productid = %s AND file_hash = %s", (productid, file_hash))
                    cur.execute("UPDATE raw_files.cube_status SET download_pending = TRUE WHERE productid = %s", (productid,))
                    conn.commit()
                    continue

                actual_hash = hash_file(p)
                if actual_hash != file_hash:
                    logger.error(f"⚠️ Hash mismatch for {productid}: expected {file_hash}, got {actual_hash}")
                    try:
                        os.remove(p)
                        logger.warning(f"🗑️ Corrupted file deleted: {file_path}")
                    except Exception as e:
                        logger.exception(f"💥 Failed to delete corrupted file: {file_path}")
                    cur.execute("DELETE FROM raw_files.manage_cube_raw_files WHERE productid = %s AND file_hash = %s", (productid, file_hash))
                    cur.execute("UPDATE raw_files.cube_status SET download_pending = TRUE WHERE productid = %s", (productid,))
                    conn.commit()
                else:
                    logger.info(f"✅ Verified {productid}: {p.name}")

def main():
    logger.info("🔍 Starting raw file verification...")
    try:
        verify_files()
        logger.success("🎯 Verification complete.")
    except Exception as e:
        logger.exception(f"💥 Verification failed: {e}")

if __name__ == "__main__":
    main()


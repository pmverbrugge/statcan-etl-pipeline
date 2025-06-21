"""
Statcan Public Data ETL Project
Script: 06_cube_verify_files.py
Date: June 21, 2025
Author: Paul Verbrugge with Claude Sonnet 4

Verify raw cube files against database log and reset download flags if missing or corrupted.
Enhanced with hard link recovery system - attempts restoration before re-download.
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


def attempt_hardlink_recovery(productid: str, file_hash: str, primary_path: Path, backup_path: str) -> bool:
    """Attempt to restore primary file from hard link backup."""
    try:
        backup_file = Path(backup_path)
        
        # Check if backup exists
        if not backup_file.exists():
            logger.warning(f"⚠️ Backup file missing for {productid}: {backup_path}")
            return False
        
        # Verify backup has correct hash
        backup_hash = hash_file_chunked(backup_file)
        if backup_hash != file_hash:
            logger.error(f"⚠️ Backup file corrupted for {productid}: hash mismatch")
            logger.info(f"   Expected: {file_hash[:16]}..., Got: {backup_hash[:16]}...")
            
            # Remove corrupted backup
            try:
                backup_file.unlink()
                logger.warning(f"🗑️ Removed corrupted backup: {backup_path}")
            except Exception as e:
                logger.error(f"❌ Failed to remove corrupted backup: {e}")
            
            return False
        
        # Restore primary file from backup (create new hard link)
        if primary_path.exists():
            primary_path.unlink()  # Remove corrupted primary
            
        primary_path.hardlink_to(backup_file)
        logger.success(f"🔗 Restored {productid} from hard link backup")
        
        # Verify restored file
        restored_hash = hash_file_chunked(primary_path)
        if restored_hash == file_hash:
            logger.info(f"✅ Restored file verified for {productid}")
            return True
        else:
            logger.error(f"❌ Restored file verification failed for {productid}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Hard link recovery failed for {productid}: {e}")
        return False


def cleanup_backup_reference(cur, productid: str, file_hash: str):
    """Remove backup_storage_location from database record."""
    try:
        cur.execute("""
            UPDATE raw_files.manage_cube_raw_files 
            SET backup_storage_location = NULL
            WHERE productid = %s AND file_hash = %s
        """, (productid, file_hash))
        logger.info(f"🗑️ Cleared backup reference for {productid}")
    except Exception as e:
        logger.error(f"❌ Failed to clear backup reference for {productid}: {e}")


def handle_missing_file(productid: str, file_hash: str, file_path: str, backup_path: str = None) -> bool:
    """Handle missing file - try hard link recovery, then mark for re-download."""
    primary_path = Path(file_path)
    
    # Attempt hard link recovery if backup exists
    if backup_path:
        logger.info(f"🔗 Attempting hard link recovery for missing file {productid}")
        if attempt_hardlink_recovery(productid, file_hash, primary_path, backup_path):
            logger.success(f"✅ Successfully recovered {productid} from backup")
            return True
    
    # Recovery failed or no backup - mark for re-download
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Atomic transaction for missing file cleanup
                cur.execute(
                    "DELETE FROM raw_files.manage_cube_raw_files WHERE productid = %s AND file_hash = %s",
                    (productid, file_hash)
                )
                deleted_count = cur.rowcount
                
                cur.execute(
                    "UPDATE raw_files.cube_status SET download_pending = TRUE WHERE productid = %s",
                    (productid,)
                )
                updated_count = cur.rowcount
                
                conn.commit()
                
                if deleted_count > 0 and updated_count > 0:
                    logger.warning(f"🗑️ Missing file cleanup completed for {productid}")
                    return True
                else:
                    logger.error(f"❌ Unexpected database state for {productid} (deleted: {deleted_count}, updated: {updated_count})")
                    return False
                    
    except Exception as e:
        logger.error(f"❌ Failed to handle missing file {productid}: {e}")
        return False


def handle_corrupted_file(productid: str, file_hash: str, file_path: Path, actual_hash: str, backup_path: str = None) -> bool:
    """Handle corrupted file - try hard link recovery, then mark for re-download."""
    
    # Attempt hard link recovery if backup exists
    if backup_path:
        logger.info(f"🔗 Attempting hard link recovery for corrupted file {productid}")
        if attempt_hardlink_recovery(productid, file_hash, file_path, backup_path):
            logger.success(f"✅ Successfully recovered {productid} from backup")
            return True
    
    # Recovery failed or no backup - delete file and mark for re-download
    try:
        # First, attempt to delete the corrupted file
        file_deleted = False
        if file_path.exists():
            try:
                file_path.unlink()
                file_deleted = True
                logger.warning(f"🗑️ Deleted corrupted file: {file_path}")
            except Exception as e:
                logger.error(f"❌ Failed to delete corrupted file {file_path}: {e}")
                # Continue with database cleanup even if file deletion fails
        
        # Database cleanup (atomic transaction)
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM raw_files.manage_cube_raw_files WHERE productid = %s AND file_hash = %s",
                    (productid, file_hash)
                )
                deleted_count = cur.rowcount
                
                cur.execute(
                    "UPDATE raw_files.cube_status SET download_pending = TRUE WHERE productid = %s",
                    (productid,)
                )
                updated_count = cur.rowcount
                
                conn.commit()
                
                if deleted_count > 0 and updated_count > 0:
                    logger.warning(f"🔧 Corrupted file cleanup completed for {productid}")
                    logger.info(f"📊 Hash mismatch details - Expected: {file_hash[:16]}..., Actual: {actual_hash[:16]}...")
                    return True
                else:
                    logger.error(f"❌ Unexpected database state for {productid} (deleted: {deleted_count}, updated: {updated_count})")
                    return False
                    
    except Exception as e:
        logger.error(f"❌ Failed to handle corrupted file {productid}: {e}")
        return False


def create_missing_backup(cur, productid: str, file_hash: str, primary_path: Path) -> str:
    """Create hard link backup for existing file that lacks one."""
    try:
        backup_dir = Path("/app/raw/cubes/.hardlink_backups")
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Create backup filename
        backup_filename = f"{productid}_{file_hash[:16]}.zip"
        backup_path = backup_dir / backup_filename
        
        # Create hard link
        backup_path.hardlink_to(primary_path)
        
        # Update database with backup location
        cur.execute("""
            UPDATE raw_files.manage_cube_raw_files 
            SET backup_storage_location = %s
            WHERE productid = %s AND file_hash = %s AND active = TRUE
        """, (str(backup_path), productid, file_hash))
        
        logger.info(f"🔗 Created missing backup for {productid}: {backup_path}")
        return str(backup_path)
        
    except Exception as e:
        logger.warning(f"⚠️ Failed to create backup for {productid}: {e}")
        return None


def verify_files():
    """Verify all files with progress reporting, hard link recovery, and error resilience."""
    start_time = time.time()
    
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT productid, file_hash, storage_location, backup_storage_location
                FROM raw_files.manage_cube_raw_files
                WHERE active = TRUE
                ORDER BY productid
            """)
            rows = cur.fetchall()
            total_files = len(rows)
            
            if total_files == 0:
                logger.info("📭 No active files found to verify")
                return
                
            logger.info(f"🔍 Verifying {total_files} files with hard link recovery and backup creation...")
            
            verified_count = 0
            missing_count = 0
            corrupted_count = 0
            recovered_count = 0
            backups_created = 0
            error_count = 0

            for i, (productid, file_hash, file_path, backup_path) in enumerate(rows, 1):
                try:
                    # Progress reporting every 50 files or at end
                    if i % 50 == 0 or i == total_files:
                        logger.info(f"📊 Progress: {i}/{total_files} files processed")
                    
                    p = Path(file_path)
                    
                    # Handle missing files
                    if not p.exists():
                        logger.warning(f"❌ File missing for {productid}: {file_path}")
                        
                        if handle_missing_file(productid, file_hash, file_path, backup_path):
                            if p.exists():  # Successfully recovered
                                recovered_count += 1
                                logger.debug(f"✅ Verified recovered {productid}")
                            else:  # Marked for re-download
                                missing_count += 1
                        else:
                            error_count += 1
                        continue

                    # Verify hash with chunked reading
                    actual_hash = hash_file_chunked(p)
                    
                    if actual_hash != file_hash:
                        logger.error(f"⚠️ Hash mismatch for {productid}: expected {file_hash[:16]}..., got {actual_hash[:16]}...")
                        
                        if handle_corrupted_file(productid, file_hash, p, actual_hash, backup_path):
                            # Check if file was recovered or marked for re-download
                            if p.exists() and hash_file_chunked(p) == file_hash:
                                recovered_count += 1
                                logger.debug(f"✅ Verified recovered {productid}")
                            else:
                                corrupted_count += 1
                        else:
                            error_count += 1
                    else:
                        # File is valid - create backup if missing
                        if not backup_path:
                            new_backup = create_missing_backup(cur, productid, file_hash, p)
                            if new_backup:
                                backups_created += 1
                                conn.commit()  # Commit backup creation
                        
                        logger.debug(f"✅ Verified {productid}: {p.name}")
                        verified_count += 1
                        
                except Exception as e:
                    logger.error(f"💥 Error processing {productid}: {e}")
                    error_count += 1
                    # Continue with next file instead of failing entire script

            # Summary statistics
            elapsed_time = time.time() - start_time
            logger.success(f"🎯 Verification complete in {elapsed_time:.1f}s")
            logger.info(f"📊 Results: {verified_count} verified, {recovered_count} recovered, {missing_count} missing, {corrupted_count} corrupted, {error_count} errors")
            
            if backups_created > 0:
                logger.success(f"🔗 Created {backups_created} missing hard link backups")
            
            if missing_count > 0 or corrupted_count > 0:
                logger.info(f"🔧 {missing_count + corrupted_count} files marked for re-download")
            
            if recovered_count > 0:
                logger.success(f"🔗 Hard link system recovered {recovered_count} files (avoided re-downloads)")


def main():
    """Main entry point for file verification script."""
    logger.info("🔍 SCRIPT 06: Cube File Verification with Hard Link Recovery - Starting")
    
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

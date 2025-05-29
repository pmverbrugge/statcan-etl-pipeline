import os
import hashlib
import datetime

def compute_sha256(data: bytes) -> str:
    """Compute SHA-256 hash of byte content."""
    return hashlib.sha256(data).hexdigest()

def file_exists_with_same_hash(path: str, new_bytes: bytes) -> bool:
    """Check if a file exists at `path` and matches the hash of `new_bytes`."""
    if not os.path.exists(path):
        return False
    with open(path, "rb") as f:
        existing_bytes = f.read()
    return hashlib.sha256(existing_bytes).hexdigest() == hashlib.sha256(new_bytes).hexdigest()

def save_file_if_changed(path: str, file_bytes: bytes) -> bool:
    """
    Save file only if it's missing or has changed.
    Returns True if file was saved or updated.
    """
    if file_exists_with_same_hash(path, file_bytes):
        return False
    with open(path, "wb") as f:
        f.write(file_bytes)
    return True

def log_file_ingest(conn, productid: str, file_path: str, file_hash: str, source_url: str = None):
    """Insert a log record into statcan.ingest_log (skip if already exists)."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO archive.ingest_log (productid, status, file_path, file_hash, notes, attempt_time)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (productid) DO NOTHING
        """, (
            productid,
            "success",
            file_path,
            file_hash,
            source_url,
            datetime.datetime.now()
        ))
    conn.commit()


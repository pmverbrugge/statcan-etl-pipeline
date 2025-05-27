"""
archive_ingester.py

Handles ingestion of raw files into the `archive.raw_file` table.
Files are hashed using SHA-256 to prevent duplicates and are stored as binary (BYTEA),
along with metadata: file name, content type, size, and source URL.

Supports ingestion from in-memory bytes, suitable for raw downloads from APIs.
"""

import hashlib
import mimetypes

def infer_content_type(file_name):
    mime, _ = mimetypes.guess_type(file_name)
    return mime or "application/octet-stream"


def ingest_file_from_bytes(file_bytes, file_name, conn, source_url=None, content_type=None):
    """
    Ingest a file into the archive.raw_file table.

    Args:
        file_bytes (bytes): Raw file content
        file_name (str): File name to store
        conn: psycopg2 connection
        source_url (str): Optional URL of origin
        content_type (str): Optional MIME type (guessed if not provided)

    Returns:
        bool: True if inserted, False if duplicate
    """
    file_hash = hashlib.sha256(file_bytes).hexdigest()
    size = len(file_bytes)
    content_type = content_type or infer_content_type(file_name)

    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM archive.raw_file WHERE file_hash = %s", (file_hash,))
        if cur.fetchone():
            print(f"🔁 Duplicate: {file_name} already ingested.")
            return False

        cur.execute("""
            INSERT INTO archive.raw_file (
                file_hash, file_name, content, content_type,
                size_bytes, source_url
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (file_hash, file_name, file_bytes, content_type, size, source_url))

    conn.commit()
    print(f"✅ Ingested: {file_name} ({content_type}, {size} bytes)")
    return True


def get_file_from_archive(conn, *, file_hash=None, file_name=None):
    """
    Retrieve a file's raw content and metadata from the archive.

    Args:
        conn: psycopg2 connection
        file_hash (str): Optional SHA-256 hash
        file_name (str): Optional name (ignored if hash is given)

    Returns:
        dict or None
    """
    with conn.cursor() as cur:
        if file_hash:
            cur.execute("""
                SELECT file_name, file_hash, content, content_type,
                       size_bytes, source_url, created_at
                FROM archive.raw_file
                WHERE file_hash = %s
            """, (file_hash,))
        elif file_name:
            cur.execute("""
                SELECT file_name, file_hash, content, content_type,
                       size_bytes, source_url, created_at
                FROM archive.raw_file
                WHERE file_name = %s
                ORDER BY id DESC LIMIT 1
            """, (file_name,))
        else:
            raise ValueError("Must provide either file_hash or file_name")

        row = cur.fetchone()
        if row:
            return {
                "file_name": row[0],
                "file_hash": row[1],
                "content": bytes(row[2]),  # decode memoryview
                "content_type": row[3],
                "size_bytes": row[4],
                "source_url": row[5],
                "created_at": row[6],
            }

    return None


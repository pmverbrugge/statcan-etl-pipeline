
"""
archive_ingester.py

This module handles ingestion of raw files into the `archive.raw_file` table
in the PostgreSQL database. Files are hashed using SHA-256 to prevent duplicates
and are stored as binary (BYTEA), along with metadata such as file name,
content type, size, and source URL.

Core features:
- Hash-based deduplication of files
- Ingest from local disk or in-memory bytes (e.g., API responses)
- Store metadata alongside content for traceability

Intended for use within ETL scripts to ensure consistent, centralized storage
of raw source files prior to processing.

Planned extensions:
- Retrieval functions by hash, ID, or file name
- Decompression / staging hooks
- Validation or annotation tools for archived data
"""

def ingest_file_from_bytes(file_bytes, file_name, conn, source_url=None, content_type="application/octet-stream"):
    import hashlib

    h = hashlib.sha256(file_bytes).hexdigest()
    size = len(file_bytes)

    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM archive.raw_file WHERE file_hash = %s", (h,))
        if cur.fetchone():
            print(f"Duplicate: {file_name} already ingested.")
            return False

        cur.execute("""
            INSERT INTO archive.raw_file (
                file_hash, file_name, content, content_type,
                size_bytes, source_url
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (h, file_name, file_bytes, content_type, size, source_url))

    conn.commit()
    print(f"Ingested from bytes: {file_name}")
    return True

def get_file_from_archive(conn, *, file_hash=None, file_name=None):
    """
    Retrieve a file's raw content and metadata from the archive.

    Args:
        conn: psycopg2 connection object
        file_hash (str): Optional SHA-256 hash of the file to retrieve
        file_name (str): Optional file name to retrieve (ignored if hash is given)

    Returns:
        dict: {
            'file_name': str,
            'file_hash': str,
            'content': bytes,
            'content_type': str,
            'size_bytes': int,
            'source_url': str,
            'created_at': datetime
        }
        or None if no match found
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
                "content": bytes(row[2]),  # 🔥 decode memoryview here
                "content_type": row[3],
                "size_bytes": row[4],
                "source_url": row[5],
                "created_at": row[6],
            }

    return None


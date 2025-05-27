import psycopg2
import zipfile
import io
from dotenv import load_dotenv
import os
from lib.archive_ingester import get_file_from_archive

load_dotenv()

DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRESS", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

PRODUCT_ID = 10100001  # change as needed

def main():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    file_name = f"{PRODUCT_ID}.sdmx.zip"
    archive = get_file_from_archive(conn, file_name=file_name)

    if not archive:
        print(f"❌ No file found for product {PRODUCT_ID}")
        return

    print(f"✅ Retrieved: {file_name} ({archive['size_bytes']} bytes)")

    # Read ZIP content
    with zipfile.ZipFile(io.BytesIO(archive["content"])) as z:
        for name in z.namelist():
            print(f"\n📦 File inside ZIP: {name}")
            with z.open(name) as f:
                lines = f.read().decode("utf-8", errors="ignore").splitlines()
                print("📝 First 10 lines:\n" + "\n".join(lines[:10]))

    conn.close()

if __name__ == "__main__":
    main()


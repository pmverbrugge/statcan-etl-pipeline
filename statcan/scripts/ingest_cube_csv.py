import os
import zipfile
import pandas as pd
import psycopg2
import csv
from io import StringIO
from statcan.tools.config import DB_CONFIG

def get_zip_path_from_db(conn, product_id):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT storage_location
            FROM raw_files.manage_cube_raw_files
            WHERE productid = %s AND active = true;
        """, (product_id,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"No active file for productId {product_id}")
        return row[0]

def get_csv_from_zip(zip_path):
    with zipfile.ZipFile(zip_path, 'r') as z:
        csv_file = [f for f in z.namelist() if f.endswith('.csv')][0]
        with z.open(csv_file) as f:
            df = pd.read_csv(f)
    return df

def infer_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    elif pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    else:
        return "TEXT"

def create_table(conn, df, product_id):
    cols = df.columns
    dtypes = [infer_sql_type(df[col]) for col in cols]
    col_defs = ",\n  ".join(f'"{col}" {dtype}' for col, dtype in zip(cols, dtypes))
    ddl = f'''
    DROP TABLE IF EXISTS cube_data."{product_id}";
    CREATE TABLE cube_data."{product_id}" (
      {col_defs}
    );
    '''
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()

def load_data(conn, df, product_id):
    with conn.cursor() as cur:
        output = StringIO()
        df.to_csv(output, index=False, header=False, quoting=csv.QUOTE_MINIMAL)
        output.seek(0)
        cols = ','.join(f'"{c}"' for c in df.columns)
        cur.copy_expert(f'COPY cube_data."{product_id}" ({cols}) FROM STDIN WITH CSV', output)
    conn.commit()

def ingest_by_product_id(product_id):
    conn = psycopg2.connect(**DB_CONFIG)
    zip_path = get_zip_path_from_db(conn, product_id)
    df = get_csv_from_zip(zip_path)
    create_table(conn, df, product_id)
    load_data(conn, df, product_id)
    conn.close()
    print(f"✅ Loaded cube {product_id} from {zip_path} with {len(df)} rows.")

# Example usage
if __name__ == "__main__":
    ingest_by_product_id("13100653")


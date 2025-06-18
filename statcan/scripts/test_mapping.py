import os
import zipfile
import random
import pandas as pd
import psycopg2
from psycopg2.extras import DictCursor
from statcan.tools.config import DB_CONFIG
from pathlib import Path
import time

RAW_CUBE_DIR = '/app/raw/cubes'
TEST_SAMPLE_SIZE = 10

# Logging setup
from loguru import logger
logger.add("/app/logs/test_mapping.log", rotation="5 MB", retention="7 days")

WDS_URL_TEMPLATE = "https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/{}/en"
DOWNLOAD_DIR = Path("/app/raw/cubes")
MAX_CUBES = None


def get_dimension_columns(productid: int) -> list[str]:
    # Query cube.cube_dimension_map for this productid and return
    # the list of dimension_name_en ordered by dimension_position (which corresponds to the index within the split COORDINATE).
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                """
                SELECT dimension_name_en
                FROM cube.cube_dimension_map
                WHERE productid = %s
                ORDER BY dimension_position
                """,
                (productid,)
            )
            cols = [row['dimension_name_en'] for row in cur.fetchall()]
    return cols


def list_columns_in_zip(zip_path, productid):
    with zipfile.ZipFile(zip_path, 'r') as z:
        target_csv = f"{productid}.csv"
        if target_csv in z.namelist():
            with z.open(target_csv) as f:
                df = pd.read_csv(f, nrows=0, dtype=str, engine='python')
            return list(df.columns)
    return None


def process_single_cube(zip_path: str):
    # productid is the prefix before the first underscore
    base = os.path.basename(zip_path)
    productid = int(base.split('_', 1)[0])

    with zipfile.ZipFile(zip_path, 'r') as z:
        target_csv = f"{productid}.csv"
        if target_csv not in z.namelist():
            raise FileNotFoundError(f"{target_csv} not found inside {zip_path}")
        with z.open(target_csv) as f:
            df = pd.read_csv(f, dtype=str, engine='python')

    # Strip whitespace from column names
    df.columns = df.columns.str.strip()

    # Locate the COORDINATE column (case-insensitive)
    coord_col = next((c for c in df.columns if c.lower() == 'coordinate'), None)
    if coord_col is None:
        raise KeyError("'COORDINATE' missing")

    # Split COORDINATE into code1..codeN
    coords = df[coord_col].astype(str).str.split('.', expand=True)
    num_codes = coords.shape[1]
    coords.columns = [f"code{i+1}" for i in range(num_codes)]
    df = pd.concat([df, coords], axis=1)

    # Fetch the dimension columns from cube_dimension_map
    dim_cols = get_dimension_columns(productid)
    if len(dim_cols) != num_codes:
        raise ValueError(
            f"{productid}: cubemap has {len(dim_cols)} dimensions but COORDINATE has {num_codes} segments"
        )

    # Verify that each code maps consistently to its label column
    mappings = [{} for _ in range(num_codes)]
    mismatches = []

    for idx, row in df.iterrows():
        for i in range(num_codes):
            code = row[f"code{i+1}"]
            label = row[dim_cols[i]]

            # Skip blank code/label
            if pd.isna(code) or str(code).strip()=='' or pd.isna(label) or str(label).strip()=='':
                mismatches.append((idx, i+1, code, label, "Missing code or label"))
                continue

            if code not in mappings[i]:
                mappings[i][code] = label
            elif mappings[i][code] != label:
                mismatches.append((
                    idx,
                    i+1,
                    code,
                    label,
                    f"Expected '{mappings[i][code]}' for code {code}"
                ))

    if mismatches:
        logger.warning(f"{productid}: {len(mismatches)} inconsistencies found (showing first 5)")
        for m in mismatches[:5]:
            row_i, dim_i, code, label, msg = m
            logger.warning(f"Row {row_i}, dim #{dim_i} ({dim_cols[dim_i-1]}): code={code!r}, label={label!r} -> {msg}")
    else:
        logger.info(f"{productid}: all {len(df)} rows have consistent code->label mappings across {num_codes} dims")

    return df


def ingest_random_cubes(raw_dir=RAW_CUBE_DIR, sample_size=TEST_SAMPLE_SIZE):
    all_zips = [f for f in os.listdir(raw_dir) if f.lower().endswith('.zip')]
    sample = random.sample(all_zips, min(sample_size, len(all_zips)))

    for fname in sample:
        path = os.path.join(raw_dir, fname)
        pid = int(fname.split('_', 1)[0])
        try:
            process_single_cube(path)
        except KeyError:
            cols = list_columns_in_zip(path, pid)
            if cols:
                logger.warning(f"{pid}: no 'COORDINATE'. Columns = {cols}")
            else:
                logger.warning(f"{pid}: CSV not found to list columns.")
        except Exception as e:
            logger.error(f"Skipped {fname}: {e}")

if __name__ == "__main__":
    random.seed(42)
    ingest_random_cubes()


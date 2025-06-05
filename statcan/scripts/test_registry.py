import zipfile
import pandas as pd
import hashlib
import psycopg2
from statcan.tools.config import DB_CONFIG

def normalize(text):
    return str(text or "").strip().lower()

def hash_key_value(code, label_en):
    return hashlib.sha256(f"{normalize(code)}|{normalize(label_en)}".encode("utf-8")).hexdigest()

def get_csv_from_zip(product_id, conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT storage_location
            FROM raw_files.manage_cube_raw_files
            WHERE productid = %s AND active = true;
        """, (product_id,))
        path = cur.fetchone()[0]

    with zipfile.ZipFile(path, 'r') as z:
        csv_file = [f for f in z.namelist() if f.endswith('.csv')][0]
        with z.open(csv_file) as f:
            df = pd.read_csv(f)
    return df


def get_dimension_order(product_id, conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.dimension_position, s.base_name, c.dimension_hash
            FROM dictionary.cube_dimension_map c
            JOIN dictionary.dimension_set s ON c.dimension_hash = s.dimension_hash
            WHERE c.productid = %s
            ORDER BY c.dimension_position;
        """, (product_id,))
        return cur.fetchall()


def get_members_by_hash(conn, dimension_hash):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT member_id, member_name_en, member_hash
            FROM dictionary.dimension_set_member
            WHERE dimension_hash = %s;
        """, (dimension_hash,))
        return { (member_id, normalize(label)): h for member_id, label, h in cur.fetchall() }

def validate_row(row, coordinate_parts, dim_order, member_lookup):
    mismatches = []
    for i, (pos, dim_name, dim_hash) in enumerate(dim_order):
        label = row[dim_name]
        label_n = normalize(label)
        coord_idx = int(coordinate_parts[i])
        member_id = coord_idx  # assumes 1-based indexing = member_id
        actual_hash = hash_key_value(member_id, label_n)
        expected_hash = member_lookup[dim_hash].get((member_id, label_n))
        if actual_hash != expected_hash:
            mismatches.append({
                "dimension": dim_name,
                "member_id": member_id,
                "label": label,
                "computed_hash": actual_hash,
                "expected_hash": expected_hash
            })
    return mismatches

def run_validation(product_id="13100653", max_rows=50):
    conn = psycopg2.connect(**DB_CONFIG)
    df = get_csv_from_zip(product_id, conn)
    dim_order = get_dimension_order(product_id, conn)
    member_lookup = {
        dim_hash: get_members_by_hash(conn, dim_hash)
        for _, _, dim_hash in dim_order
    }

    fails = []
    for _, row in df.head(max_rows).iterrows():
        coord = row["COORDINATE"]
        parts = coord.split(".")
        if len(parts) != len(dim_order):
            continue
        mismatches = validate_row(row, parts, dim_order, member_lookup)
        if mismatches:
            fails.append({"row_coord": coord, "issues": mismatches})

    conn.close()
    if not fails:
        print("✅ All checked rows matched dimension registry.")
    else:
        for fail in fails:
            print(f"❌ COORDINATE: {fail['row_coord']}")
            for issue in fail["issues"]:
                print(f"  - Dimension: {issue['dimension']}")
                print(f"    Label: '{issue['label']}', ID: {issue['member_id']}")
                print(f"    Computed: {issue['computed_hash']}")
                print(f"    Expected: {issue['expected_hash']}")
        print(f"\n{len(fails)} rows failed out of {max_rows} checked.")

if __name__ == "__main__":
    run_validation()


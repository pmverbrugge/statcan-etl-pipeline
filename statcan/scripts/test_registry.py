import zipfile
import pandas as pd
import psycopg2
from statcan.tools.config import DB_CONFIG
from loguru import logger
from collections import defaultdict, Counter

logger.add("/app/logs/validate_registry.log", rotation="1 MB", retention="7 days")

def normalize(text):
    return str(text or "").strip().lower().replace("\u00a0", " ")  # handle non-breaking spaces

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

def get_dimension_map(product_id, conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT dimension_position, dimension_name_slug, dimension_hash
            FROM cube.cube_dimension_map
            WHERE productid = %s
            ORDER BY dimension_position ASC;
        """, (product_id,))
        return cur.fetchall()

def get_member_lookup(conn, dimension_hash):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT member_id, member_label_norm
            FROM dictionary.dimension_set_member
            WHERE dimension_hash = %s;
        """, (dimension_hash,))
        return {normalize(label): (member_id, label) for member_id, label in cur.fetchall()}

def resolve_label_columns(df, dim_order):
    label_columns = [col for col in df.columns if col not in ('REF_DATE', 'COORDINATE')]
    coord_parts_list = df['COORDINATE'].dropna().apply(lambda x: str(x).split("."))
    candidate_mappings = {}

    for i, (_, slug, _) in enumerate(dim_order):
        codes = coord_parts_list.map(lambda parts: parts[i] if i < len(parts) else None)
        for col in label_columns:
            pairs = list(zip(codes, df[col]))
            code_to_label = defaultdict(set)
            label_to_code = defaultdict(set)
            for code, label in pairs:
                code_to_label[code].add(normalize(label))
                label_to_code[normalize(label)].add(code)

            if all(len(v) == 1 for v in code_to_label.values()) and all(len(v) == 1 for v in label_to_code.values()):
                candidate_mappings[i] = col
                break

    return candidate_mappings

def validate_row(row, dim_order, member_lookups, label_map):
    mismatches = []
    coord_parts = str(row["COORDINATE"]).split(".")

    for i, (pos, slug, dim_hash) in enumerate(dim_order):
        if i not in label_map:
            continue
        label_col = label_map[i]
        raw_label = row[label_col]
        label_norm = normalize(raw_label)
        lookup = member_lookups.get(dim_hash, {})
        matched = lookup.get(label_norm)

        if not matched:
            mismatches.append({
                "dimension": slug,
                "label": raw_label,
                "norm": label_norm,
                "expected": next(iter(lookup.values()), (None, ""))[1],
                "member_id": next(iter(lookup.values()), (None,))[0],
            })

    return mismatches

def run_validation(conn, product_id, max_rows=50):
    logger.info(f"🔍 Validating productId: {product_id}")
    try:
        df = get_csv_from_zip(product_id, conn)
        dim_order = get_dimension_map(product_id, conn)
        member_lookups = {
            dim_hash: get_member_lookup(conn, dim_hash)
            for _, _, dim_hash in dim_order
        }
        label_map = resolve_label_columns(df, dim_order)

        failures = []
        for _, row in df.head(max_rows).iterrows():
            mismatches = validate_row(row, dim_order, member_lookups, label_map)
            if mismatches:
                failures.append({"coord": row["COORDINATE"], "issues": mismatches})

        if not failures:
            print("✅ All rows matched.")
        else:
            for fail in failures:
                print(f"❌ COORDINATE: {fail['coord']}")
                for issue in fail["issues"]:
                    print(f"  - Dimension: {issue['dimension']}")
                    print(f"    Label: '{issue['label']}' → Norm: '{issue['norm']}'")
                    print(f"    Expected: '{issue['expected']}'")
                    print(f"    ID: {issue['member_id']}")

    except Exception as e:
        logger.error(f"❌ Error validating {product_id}: {e}")

if __name__ == "__main__":
    conn = psycopg2.connect(**DB_CONFIG)
    sample_ids = [
        13100653, 13100667, 27100024, 27100157, 33100701,
        35100007, 36100208, 37100261, 46100053, 98100060
    ]
    for pid in sample_ids:
        run_validation(conn, pid)
    conn.close()


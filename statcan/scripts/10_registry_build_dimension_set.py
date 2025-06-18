import hashlib
import pandas as pd
import psycopg2
from slugify import slugify
from collections import Counter
from loguru import logger
from statcan.tools.config import DB_CONFIG

logger.add("/app/logs/build_dim_registry.log", rotation="1 MB", retention="7 days")

def normalize(text):
    return str(text or "").strip().lower()

def hash_key_value(code, label_en, parent_id=None, uom_code=None):
    key = f"{normalize(code)}|{normalize(label_en)}|{normalize(parent_id)}|{normalize(uom_code)}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()

def hash_dimension(member_hashes):
    sorted_hashes = sorted(member_hashes)
    return hashlib.sha256("|".join(sorted_hashes).encode("utf-8")).hexdigest()

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

def build_dimension_registry():
    logger.info("🚀 Starting dimension registry build...")

    with get_db_conn() as conn:
        cur = conn.cursor()

        raw_member = pd.read_sql("SELECT * FROM dictionary.raw_member", conn)
        raw_dim = pd.read_sql("SELECT * FROM dictionary.raw_dimension", conn)

        # Step 1: Normalize and hash code-label pairs
        raw_member["key_value_hash"] = raw_member.apply(
            lambda row: hash_key_value(
                row["member_id"],
                row["member_name_en"],
                row["parent_member_id"],
                row["member_uom_code"]
            ), axis=1
        )

        # Step 2: Create dimension-level hash
        grouped = raw_member.groupby(["productid", "dimension_position"])["key_value_hash"].apply(list).reset_index()
        grouped["dimension_hash"] = grouped["key_value_hash"].apply(hash_dimension)

        # Step 3: Join dimension_hash back to raw_member
        raw_member = raw_member.merge(grouped[["productid", "dimension_position", "dimension_hash"]],
                                      on=["productid", "dimension_position"], how="left")

        # Step 4: Select most common English/French label for each code
        label_counts = (
            raw_member.groupby(["dimension_hash", "member_id", "member_name_en", "member_name_fr"])
            .size().reset_index(name="count")
            .sort_values(["dimension_hash", "member_id", "count"], ascending=[True, True, False])
        )
        core_members = label_counts.drop_duplicates(subset=["dimension_hash", "member_id"])

        # Add member_hash column (reusing earlier value)
        core_members = core_members.merge(
            raw_member[["dimension_hash", "member_id", "key_value_hash"]],
            on=["dimension_hash", "member_id"], how="left"
        ).rename(columns={"key_value_hash": "member_hash"})

        # Add normalized label for validation
        core_members["member_label_norm"] = core_members["member_name_en"].apply(normalize)

        # Step 5: Select most common English dimension name
        raw_dim = raw_dim.merge(grouped, on=["productid", "dimension_position"], how="left")
        core_dims = (
            raw_dim.groupby(["dimension_hash", "dimension_name_en", "dimension_name_fr"])
            .size().reset_index(name="count")
            .sort_values(["dimension_hash", "count"], ascending=[True, False])
            .drop_duplicates(subset=["dimension_hash"])
        )
        core_dims["dimension_name_slug"] = core_dims["dimension_name_en"].apply(slugify, separator="_")

        # Step 6: Flagging logic
        core_members.loc[:, "is_total"] = core_members["member_name_en"].str.contains("total", case=False, na=False)
        core_dims["has_total"] = core_members.groupby("dimension_hash")["is_total"].any().values
        core_dims["is_exclusive"] = False  # placeholder
        core_dims["is_grabbag"] = core_dims["dimension_name_en"].str.contains("characteristics|other", case=False, na=False)
        core_dims["is_tree"] = raw_member.groupby("dimension_hash")["parent_member_id"].apply(lambda x: x.notna().any()).values

        # Step 7: Insert data
        logger.info("🧩 Inserting into dictionary.dimension_set")
        for _, row in core_dims.iterrows():
            cur.execute("""
                INSERT INTO dictionary.dimension_set (
                    dimension_hash, dimension_name_en, dimension_name_fr, dimension_name_slug,
                    has_total, is_exclusive, is_grabbag, is_tree
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dimension_hash) DO NOTHING
            """, (
                row["dimension_hash"],
                row["dimension_name_en"],
                row["dimension_name_fr"],
                row["dimension_name_slug"],
                row["has_total"],
                row["is_exclusive"],
                row["is_grabbag"],
                row["is_tree"]
            ))

        logger.info("🧩 Inserting into dictionary.dimension_set_member")
        for _, row in core_members.iterrows():
            cur.execute("""
                INSERT INTO dictionary.dimension_set_member (
                    dimension_hash, member_id, member_hash,
                    member_name_en, member_name_fr, is_total, member_label_norm
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dimension_hash, member_id) DO NOTHING
            """, (
                row["dimension_hash"],
                row["member_id"],
                row["member_hash"],
                row["member_name_en"],
                row["member_name_fr"],
                row["is_total"],
                row["member_label_norm"]
            ))

        logger.info("🧩 Inserting into cube.cube_dimension_map")
        cube_dim_lookup = raw_dim[
            ["productid", "dimension_position", "dimension_hash", "dimension_name_en", "dimension_name_fr"]
        ].drop_duplicates()
        cube_dim_lookup["dimension_name_slug"] = cube_dim_lookup["dimension_name_en"].apply(slugify, separator="_")

        for _, row in cube_dim_lookup.iterrows():
            cur.execute("""
                INSERT INTO cube.cube_dimension_map (
                    productid, dimension_position, dimension_hash,
                    dimension_name_en, dimension_name_fr, dimension_name_slug
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (productid, dimension_position) DO NOTHING
            """, (
                row["productid"],
                row["dimension_position"],
                row["dimension_hash"],
                row["dimension_name_en"],
                row["dimension_name_fr"],
                row["dimension_name_slug"]
            ))

        conn.commit()
        logger.info("✅ Dimension registry inserted into DB.")

if __name__ == "__main__":
    try:
        build_dimension_registry()
    except Exception as e:
        logger.error(f"❌ Failed to build dimension registry: {e}")


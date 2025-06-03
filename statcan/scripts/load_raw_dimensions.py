import psycopg2
import json
from pathlib import Path
from loguru import logger
from statcan.tools.config import DB_CONFIG

logger.add("/app/logs/load_raw_dimensions.log", rotation="1 MB", retention="7 days")

metadata_dir = Path("/app/raw/metadata")

# SQL insert statements
INSERT_DIM_SQL = """
INSERT INTO dictionary.raw_dimension (
    productid, dimension_position, dimension_name_en, dimension_name_fr, has_uom
) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
"""

INSERT_MEMBER_SQL = """
INSERT INTO dictionary.raw_member (
    productid, dimension_position, member_id, parent_member_id, classification_code,
    classification_type_code, member_name_en, member_name_fr, member_uom_code,
    geo_level, vintage, terminated
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
"""

def safe_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def main():
    logger.info("🟢 Starting metadata ingestion based on metadata_status...")

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT productid, last_file_hash
                    FROM raw_files.metadata_status
                    WHERE download_pending = FALSE AND last_file_hash IS NOT NULL;
                """)
                records = cur.fetchall()

        logger.info(f"Found {len(records)} completed metadata entries.")

        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                for productid, file_hash in records:
                    filename = f"{productid}_{file_hash[:16]}.json"
                    file_path = metadata_dir / filename

                    if not file_path.exists():
                        logger.warning(f"⚠️ File missing: {filename}")
                        continue

                    try:
                        with open(file_path, "r", encoding="utf-8") as f:
                            data = json.load(f)

                        obj = data[0].get("object", {})
                        for dim in obj.get("dimension", []):
                            pos = safe_int(dim.get("dimensionPositionId"))
                            dim_name_en = dim.get("dimensionNameEn")
                            dim_name_fr = dim.get("dimensionNameFr")
                            has_uom = dim.get("hasUom")

                            cur.execute(INSERT_DIM_SQL, (
                                productid, pos, dim_name_en, dim_name_fr, has_uom
                            ))

                            for m in dim.get("member", []):
                                cur.execute(INSERT_MEMBER_SQL, (
                                    productid,
                                    pos,
                                    safe_int(m.get("memberId")),
                                    safe_int(m.get("parentMemberId")),
                                    m.get("classificationCode"),
                                    m.get("classificationTypeCode"),
                                    m.get("memberNameEn"),
                                    m.get("memberNameFr"),
                                    m.get("memberUomCode"),
                                    safe_int(m.get("geoLevel")),
                                    safe_int(m.get("vintage")),
                                    safe_int(m.get("terminated"))
                                ))

                    except Exception as e:
                        logger.exception(f"❌ Error processing {filename}: {e}")

            conn.commit()
            logger.success("✅ Metadata ingestion complete.")

    except Exception as e:
        logger.exception(f"🚨 Database connection or query failed: {e}")

if __name__ == "__main__":
    main()


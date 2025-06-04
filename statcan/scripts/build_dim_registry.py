import psycopg2
from collections import defaultdict, Counter
from loguru import logger
import hashlib
from statcan.tools.config import DB_CONFIG

logger.add("/app/logs/build_dim_registry.log", rotation="1 MB", retention="7 days")

def normalize(text):
    return str(text or "").strip().lower()

def hash_key_value(code, label_en):
    """Create a reproducible hash for a key-value pair."""
    return hashlib.sha256(f"{normalize(code)}|{normalize(label_en)}".encode("utf-8")).hexdigest()

def hash_dimension(member_hashes):
    """Hash a sorted list of member hashes to define the dimension signature."""
    sorted_hashes = sorted(member_hashes)
    return hashlib.sha256("|".join(sorted_hashes).encode("utf-8")).hexdigest()

def detect_total(label):
    return normalize(label) in {"total", "all", "overall", "toutes", "tous", "ensemble"}

def main():
    logger.info("🚀 Starting dimension registry build...")

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # 1. Extract raw member data
                cur.execute("""
                    SELECT productid, dimension_position, classification_code,
                           member_name_en, member_name_fr
                    FROM dictionary.raw_member
                """)
                raw_rows = cur.fetchall()

                # 2. Normalize and hash key-value pairs, group by (productid, dimension_position)
                member_map = defaultdict(list)
                kv_map = defaultdict(list)  # For summarizing most common labels
                for row in raw_rows:
                    pid, dpos, code, en, fr = row
                    kv_hash = hash_key_value(code, en)
                    member_map[(pid, dpos)].append(kv_hash)
                    kv_map[(kv_hash, 'en')].append(en)
                    kv_map[(kv_hash, 'fr')].append(fr)

                # 3. Build dimension hash from sorted member hashes
                dim_hash_map = {}  # Maps (productid, dimension_position) to dimension_hash
                for key, hashes in member_map.items():
                    dim_hash_map[key] = hash_dimension(hashes)

                # 4. Insert unique dimension_set entries first
                inserted_dims = set()
                for dh in set(dim_hash_map.values()):
                    if dh not in inserted_dims:
                        cur.execute("""
                            INSERT INTO dictionary.dimension_set (dimension_hash)
                            VALUES (%s)
                            ON CONFLICT DO NOTHING
                        """, (dh,))
                        inserted_dims.add(dh)

                # 5. Build and insert dimension_set_member entries
                member_seen = set()
                for row in raw_rows:
                    pid, dpos, code, en, fr = row
                    kv_hash = hash_key_value(code, en)
                    dim_hash = dim_hash_map[(pid, dpos)]

                    if kv_hash in member_seen:
                        continue
                    member_seen.add(kv_hash)

                    best_en = Counter(kv_map[(kv_hash, 'en')]).most_common(1)[0][0]
                    best_fr = Counter(kv_map[(kv_hash, 'fr')]).most_common(1)[0][0]
                    is_total = detect_total(best_en)

                    cur.execute("""
                        INSERT INTO dictionary.dimension_set_member (
                            dimension_hash, member_hash,
                            classification_code, member_name_en, member_name_fr, is_total
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        dim_hash, kv_hash,
                        code, best_en, best_fr, is_total
                    ))

        logger.success("✅ Dimension registry build complete.")

    except Exception as e:
        logger.exception(f"❌ Failed to build dimension registry: {e}")

if __name__ == "__main__":
    main()


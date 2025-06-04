import psycopg2
from collections import defaultdict, Counter
from loguru import logger
import hashlib
from statcan.tools.config import DB_CONFIG

logger.add("/app/logs/build_dimension_registry.log", rotation="1 MB", retention="7 days")

def normalize(text):
    return str(text or '').strip().lower()

def hash_member(code, label_en):
    return hashlib.sha256(f"{normalize(code)}|{normalize(label_en)}".encode("utf-8")).hexdigest()

def hash_dimension(member_list):
    sorted_items = sorted((hash_member(m[0], m[1]) for m in member_list))
    return hashlib.sha256("|".join(sorted_items).encode("utf-8")).hexdigest()

def detect_total(label):
    return normalize(label) in {"total", "all", "overall", "toutes", "tous", "ensemble"}

def main():
    logger.info("🚀 Starting dimension registry build...")
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT productid, dimension_position, classification_code,
                           classification_type_code, member_id, member_name_en,
                           member_name_fr, member_uom_code, parent_member_id,
                           geo_level, vintage, terminated
                    FROM dictionary.raw_member
                """)
                raw_members = cur.fetchall()

                member_dict = defaultdict(list)
                for row in raw_members:
                    pid, dpos = row[0], row[1]
                    member_dict[(pid, dpos)].append(row[2:])

                seen_dimensions = {}
                seen_members = set()
                label_counter_en = defaultdict(Counter)
                label_counter_fr = defaultdict(Counter)
                pending_member_inserts = {}

                for (pid, dpos), members in member_dict.items():
                    dim_hash = hash_dimension(members)

                    if dim_hash not in seen_dimensions:
                        cur.execute("""
                            INSERT INTO dictionary.dimension_set (
                                dimension_hash
                            ) VALUES (%s)
                            ON CONFLICT DO NOTHING
                        """, (dim_hash,))
                        seen_dimensions[dim_hash] = True

                    for m in members:
                        if len(m) < 5:
                            logger.warning(f"Skipping incomplete member row: {m}")
                            continue

                        code, label_en, label_fr = m[0], m[2], m[3]
                        mem_hash = hash_member(code, label_en)

                        label_counter_en[mem_hash][label_en] += 1
                        label_counter_fr[mem_hash][label_fr] += 1

                        if mem_hash not in seen_members:
                            seen_members.add(mem_hash)
                            pending_member_inserts[mem_hash] = (dim_hash, mem_hash, m)
 mem_hash, (dim_hash, mem_hash, m) in pending_member_inserts.items():
                    if len(m) < 9:
                        logger.warning(f"Skipping malformed member tuple (len={len(m)}): {m}")
                        continue

                    best_en = label_counter_en[mem_hash].most_common(1)[0][0]
                    best_fr = label_counter_fr[mem_hash].most_common(1)[0][0]
                    is_total = detect_total(best_en)

                    cur.execute("""
                        INSERT INTO dictionary.dimension_member (
                            dimension_hash, member_hash, member_id, classification_code,
                            classification_type_code, member_name_en, member_name_fr,
                            member_uom_code, parent_member_id, geo_level, vintage,
                            terminated, is_total
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        dim_hash, mem_hash,
                        m[2] if len(m) > 2 else None,
                        m[0] if len(m) > 0 else None,
                        m[1] if len(m) > 1 else None,
                        best_en, best_fr,
                        m[4] if len(m) > 4 else None,
                        m[5] if len(m) > 5 else None,
                        m[6] if len(m) > 6 else None,
                        m[7] if len(m) > 7 else None,
                       bool(m[8]) if len(m) > 8 else None,
                        is_total
                    ))

        logger.success("✅ Dimension registry build complete.")

    except Exception as e:
        logger.exception(f"❌ Failed to build dimension registry: {e}")

if __name__ == "__main__":
    main()


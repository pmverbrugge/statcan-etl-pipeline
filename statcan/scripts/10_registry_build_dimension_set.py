#!/usr/bin/env python3
"""
#!/usr/bin/env python3

Build dimension registry from raw metadata - Simplified version with proper cleanup

This script normalizes and deduplicates Statistics Canada dimension metadata into a 
harmonized registry that enables cross-cube integration and consistent analytics.

Process Overview:
1. **Data Loading**: Reads raw dimension and member metadata from dictionary.raw_* tables
2. **Hashing**: Creates unique hashes for members (based on ID, label, parent, UOM) and 
   dimensions (based on constituent member hashes)
3. **Canonical Labels**: Selects most frequently occurring English/French labels for 
   each unique member and dimension across all cubes
4. **Metadata Flags**: Computes dimension-level characteristics:
   - has_total: Contains members with "total" in the name
   - is_tree: Has parent-child hierarchical relationships
   - is_grabbag: Name contains "characteristics" or "other" (catch-all dimensions)
   - is_statistics: Placeholder for statistical measure detection
5. **Registry Population**: Inserts normalized data into three target tables:
   - dictionary.dimension_set: Unique dimension definitions with metadata flags
   - dictionary.dimension_set_member: Canonical member definitions linked to dimensions
   - cube.cube_dimension_map: Maps each cube's dimensions to registry entries

Key Benefits:
- **Deduplication**: Identical dimensions across cubes share the same dimension_hash
- **Harmonization**: Consistent labels and metadata enable cross-cube queries
- **Traceability**: Hash-based system maintains referential integrity
- **Scalability**: Registry grows incrementally as new cubes are added

Data Quality Features:
- Validates member_id data types and filters NULL values
- Handles data type conversions for numeric fields
- Uses most common labels to resolve inconsistencies
- Periodic commits during large insertions for reliability
- Comprehensive error logging and progress reporting

Dependencies: Raw metadata must be loaded via scripts 07-09 before running this script.
Next Step: Run script 11 to compute normalized base names for semantic grouping.
"""

import hashlib
import pandas as pd
import psycopg2
from slugify import slugify
from loguru import logger
from statcan.tools.config import DB_CONFIG

logger.add("/app/logs/build_dim_registry.log", rotation="1 MB", retention="7 days")

def normalize(text):
    """Normalize text for consistent hashing"""
    return str(text or "").strip().lower()

def hash_key_value(code, label_en, parent_id=None, uom_code=None):
    """Create hash from member attributes"""
    key = f"{normalize(code)}|{normalize(label_en)}|{normalize(parent_id)}|{normalize(uom_code)}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()

def hash_dimension(member_hashes):
    """Create hash from sorted member hashes"""
    sorted_hashes = sorted(member_hashes)
    return hashlib.sha256("|".join(sorted_hashes).encode("utf-8")).hexdigest()

def get_db_conn():
    """Get database connection"""
    return psycopg2.connect(**DB_CONFIG)

def build_dimension_registry():
    """Build normalized dimension registry from raw metadata"""
    logger.info("🚀 Starting dimension registry build...")
    
    with get_db_conn() as conn:
        cur = conn.cursor()
        
        # ALWAYS start fresh - truncate in correct order due to foreign keys
        logger.info("🧹 Truncating existing registry data...")
        try:
            cur.execute("TRUNCATE TABLE cube.cube_dimension_map CASCADE")
            cur.execute("TRUNCATE TABLE dictionary.dimension_set_member CASCADE")
            cur.execute("TRUNCATE TABLE dictionary.dimension_set CASCADE")
            conn.commit()
            logger.info("✅ Tables truncated successfully")
        except Exception as e:
            logger.error(f"Failed to truncate tables: {e}")
            conn.rollback()
            raise
        
        # Load raw data
        logger.info("📊 Loading raw data...")
        raw_member = pd.read_sql("""
            SELECT productid, dimension_position, member_id, 
                   parent_member_id, classification_code, classification_type_code,
                   member_name_en, member_name_fr, member_uom_code,
                   geo_level, vintage, terminated
            FROM dictionary.raw_member
            WHERE member_id IS NOT NULL
        """, conn)
        
        raw_dim = pd.read_sql("""
            SELECT productid, dimension_position, 
                   dimension_name_en, dimension_name_fr, has_uom
            FROM dictionary.raw_dimension
        """, conn)
        
        logger.info(f"Loaded {len(raw_member)} raw members and {len(raw_dim)} raw dimensions")
        
        # Data validation
        if raw_member['member_id'].isna().any():
            logger.warning("Found NULL member_ids, filtering them out")
            raw_member = raw_member[raw_member['member_id'].notna()]
        
        # Convert member_id to int, handling any float values
        raw_member['member_id'] = raw_member['member_id'].astype('int64')
        raw_member['parent_member_id'] = raw_member['parent_member_id'].fillna(0).astype('int64')
        raw_member.loc[raw_member['parent_member_id'] == 0, 'parent_member_id'] = None
        
        logger.info(f"Max member_id: {raw_member['member_id'].max()}")
        logger.info(f"Min member_id: {raw_member['member_id'].min()}")
        
        # Step 1: Create member hashes
        logger.info("🔑 Creating member hashes...")
        raw_member["member_hash"] = raw_member.apply(
            lambda row: hash_key_value(
                row["member_id"],
                row["member_name_en"],
                row["parent_member_id"],
                row["member_uom_code"]
            ), axis=1
        )
        
        # Step 2: Create dimension hashes
        logger.info("🔑 Creating dimension hashes...")
        grouped = raw_member.groupby(["productid", "dimension_position"])["member_hash"].apply(list).reset_index()
        grouped["dimension_hash"] = grouped["member_hash"].apply(hash_dimension)
        
        # Step 3: Join dimension_hash back to raw data
        logger.info("🔗 Joining dimension hashes...")
        raw_member = raw_member.merge(
            grouped[["productid", "dimension_position", "dimension_hash"]],
            on=["productid", "dimension_position"], 
            how="left"
        )
        
        raw_dim = raw_dim.merge(
            grouped[["productid", "dimension_position", "dimension_hash"]], 
            on=["productid", "dimension_position"], 
            how="left"
        )
        
        # Step 4: Get canonical labels (most common)
        logger.info("📝 Selecting canonical member labels...")
        member_canonical = (
            raw_member.groupby(["dimension_hash", "member_id"])
            .agg({
                'member_name_en': lambda x: x.value_counts().index[0] if len(x) > 0 else None,
                'member_name_fr': lambda x: x.value_counts().index[0] if len(x) > 0 else None,
                'member_hash': 'first',
                'classification_code': 'first',
                'classification_type_code': 'first',
                'member_uom_code': 'first',
                'parent_member_id': 'first',
                'geo_level': 'first',
                'vintage': 'first',
                'terminated': 'first'
            })
            .reset_index()
        )
        
        member_canonical["is_total"] = member_canonical["member_name_en"].str.contains(
            "total", case=False, na=False
        )
        member_canonical["member_label_norm"] = member_canonical["member_name_en"].apply(normalize)
        
        # Step 5: Get canonical dimension names
        logger.info("📝 Selecting canonical dimension names...")
        dim_canonical = (
            raw_dim.groupby("dimension_hash")
            .agg({
                'dimension_name_en': lambda x: x.value_counts().index[0] if len(x) > 0 else None,
                'dimension_name_fr': lambda x: x.value_counts().index[0] if len(x) > 0 else None,
            })
            .reset_index()
        )
        
        # Calculate dimension flags
        dim_flags = member_canonical.groupby("dimension_hash").agg({
            'is_total': 'any',
            'parent_member_id': lambda x: x.notna().any()
        }).rename(columns={'parent_member_id': 'is_tree'})
        
        dim_canonical = dim_canonical.merge(dim_flags, on="dimension_hash", how="left")
        dim_canonical["has_total"] = dim_canonical["is_total"].fillna(False)
        dim_canonical["is_tree"] = dim_canonical["is_tree"].fillna(False)
        dim_canonical["is_exclusive"] = False
        dim_canonical["is_grabbag"] = dim_canonical["dimension_name_en"].str.contains(
            "characteristics|other", case=False, na=False
        )
        dim_canonical["is_statistics"] = False
        dim_canonical["dimension_name_slug"] = dim_canonical["dimension_name_en"].apply(
            lambda x: slugify(x, separator="_") if pd.notna(x) else None
        )
        
        # Step 6: Insert dimension sets
        logger.info("💾 Inserting dimension sets...")
        for _, row in dim_canonical.iterrows():
            try:
                cur.execute("""
                    INSERT INTO dictionary.dimension_set (
                        dimension_hash, dimension_name_en, dimension_name_fr, 
                        dimension_name_slug, has_total, is_exclusive, 
                        is_grabbag, is_tree, is_statistics
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row["dimension_hash"],
                    row["dimension_name_en"],
                    row["dimension_name_fr"],
                    row["dimension_name_slug"],
                    bool(row["has_total"]),
                    bool(row["is_exclusive"]),
                    bool(row["is_grabbag"]),
                    bool(row["is_tree"]),
                    bool(row["is_statistics"])
                ))
            except Exception as e:
                logger.error(f"Error inserting dimension set: {e}")
                logger.error(f"Row data: {row.to_dict()}")
                raise
        
        logger.info(f"Inserted {len(dim_canonical)} dimension sets")
        
        # Step 7: Insert dimension set members
        logger.info("💾 Inserting dimension set members...")
        error_count = 0
        success_count = 0
        
        for idx, row in member_canonical.iterrows():
            try:
                # Ensure proper data types
                member_id = int(row["member_id"])
                parent_id = int(row["parent_member_id"]) if pd.notna(row["parent_member_id"]) else None
                geo_level = int(row["geo_level"]) if pd.notna(row["geo_level"]) else None
                vintage = int(row["vintage"]) if pd.notna(row["vintage"]) else None
                terminated = bool(row["terminated"]) if pd.notna(row["terminated"]) else None
                
                cur.execute("""
                    INSERT INTO dictionary.dimension_set_member (
                        dimension_hash, member_hash, member_id,
                        classification_code, classification_type_code,
                        member_name_en, member_name_fr, member_uom_code,
                        parent_member_id, geo_level, vintage, terminated,
                        is_total, base_name, member_label_norm
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row["dimension_hash"],
                    row["member_hash"],
                    member_id,
                    row["classification_code"],
                    row["classification_type_code"],
                    row["member_name_en"],
                    row["member_name_fr"],
                    row["member_uom_code"],
                    parent_id,
                    geo_level,
                    vintage,
                    terminated,
                    bool(row["is_total"]),
                    None,  # base_name will be set by script 11
                    row["member_label_norm"]
                ))
                success_count += 1
                
                if success_count % 10000 == 0:
                    logger.info(f"  Progress: {success_count} members inserted...")
                    conn.commit()  # Commit periodically
                    
            except Exception as e:
                error_count += 1
                if error_count <= 5:  # Log first 5 errors
                    logger.error(f"Error inserting member at index {idx}: {e}")
                    logger.error(f"member_id: {row['member_id']}, type: {type(row['member_id'])}")
                    logger.error(f"dimension_hash: {row['dimension_hash']}")
                if error_count == 100:
                    logger.error("Too many errors, aborting...")
                    raise
        
        logger.info(f"Inserted {success_count} dimension set members ({error_count} errors)")
        
        # Step 8: Insert cube dimension map
        logger.info("🗺️ Inserting cube dimension map...")
        cube_dim_map = raw_dim[["productid", "dimension_position", "dimension_hash", 
                                "dimension_name_en", "dimension_name_fr"]].drop_duplicates()
        cube_dim_map["dimension_name_slug"] = cube_dim_map["dimension_name_en"].apply(
            lambda x: slugify(x, separator="_") if pd.notna(x) else None
        )
        
        for _, row in cube_dim_map.iterrows():
            cur.execute("""
                INSERT INTO cube.cube_dimension_map (
                    productid, dimension_position, dimension_hash,
                    dimension_name_en, dimension_name_fr, dimension_name_slug
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                int(row["productid"]),
                int(row["dimension_position"]),
                row["dimension_hash"],
                row["dimension_name_en"],
                row["dimension_name_fr"],
                row["dimension_name_slug"]
            ))
        
        logger.info(f"Inserted {len(cube_dim_map)} cube dimension mappings")
        
        # Final commit
        conn.commit()
        
        # Summary statistics
        cur.execute("SELECT COUNT(DISTINCT dimension_hash) FROM dictionary.dimension_set")
        dim_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM dictionary.dimension_set_member")
        member_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM cube.cube_dimension_map")
        map_count = cur.fetchone()[0]
        
        logger.info(f"""
        ✅ Dimension registry build completed!
        📊 Summary:
        - Unique dimension sets: {dim_count}
        - Total dimension members: {member_count}
        - Cube dimension mappings: {map_count}
        """)

if __name__ == "__main__":
    try:
        build_dimension_registry()
    except Exception as e:
        logger.error(f"❌ Failed to build dimension registry: {e}")
        raise

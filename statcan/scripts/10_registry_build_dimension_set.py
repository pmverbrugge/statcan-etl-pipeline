"""
Enhanced Dimension Registry Build Script - Statistics Canada ETL Pipeline
==========================================================================

This is the CORE HARMONIZATION ENGINE that transforms raw StatCan metadata into 
a unified dimension registry. It implements sophisticated deduplication algorithms,
label standardization, and metadata enrichment to enable cross-cube analytics and
consistent dimension definitions across the entire StatCan catalog.

Key Features:
- Hash-based dimension deduplication across all cubes
- Intelligent label standardization and conflict resolution
- Metadata flag detection (totals, hierarchies, grab-bags)
- Cross-cube dimension mapping and harmonization
- Statistical validation of harmonization quality
- Comprehensive audit trail of registry construction

Process Flow:
1. Load and validate raw dimension/member data from dictionary tables
2. Generate member-level hashes for identical code-label combinations
3. Create dimension-level hashes from aggregated member signatures
4. Resolve label conflicts using frequency-based selection
5. Detect metadata patterns (totals, trees, exclusive dimensions)
6. Build harmonized registry tables with cross-references
7. Validate registry quality and coverage statistics

Harmonization Algorithm:
- Member Hash: SHA-256(code|label_en|parent_id|uom_code)
- Dimension Hash: SHA-256(sorted_member_hashes)
- Label Selection: Most frequent English/French labels per code
- Conflict Resolution: Statistical frequency-based selection

Protection Mechanisms:
- Input data validation and quality checks
- Memory-efficient processing for large datasets
- Statistical validation of harmonization results
- Comprehensive error handling for data quality issues
- Atomic database operations with rollback protection

Registry Outputs:
- dictionary.dimension_set: Unified dimension definitions
- dictionary.dimension_set_member: Harmonized member registry  
- cube.cube_dimension_map: Cube-to-dimension cross-reference

This script is the innovation that enables StatCan's inconsistent metadata
to be harmonized into a coherent, queryable dimension registry.

Dependencies:
- Requires raw_dimension and raw_member data from 09_dimension_raw_load.py
- Uses pandas for efficient data processing and aggregation
- Requires slugify library for URL-safe dimension names

Last Updated: June 2025
Author: Paul Verbrugge
"""

import hashlib
import pandas as pd
import psycopg2
from slugify import slugify
from collections import Counter
from loguru import logger
from statcan.tools.config import DB_CONFIG

# Add file logging
logger.add("/app/logs/build_dim_registry.log", rotation="1 MB", retention="7 days")

# Processing constants
MIN_RAW_DIMENSIONS = 100  # Minimum dimensions expected for processing
MIN_RAW_MEMBERS = 1000   # Minimum members expected for processing
MAX_LABEL_LENGTH = 500   # Maximum length for dimension/member labels
CONFLICT_THRESHOLD = 0.1  # Warn if >10% of labels have conflicts


def validate_registry_build_setup(cur) -> dict:
    """Validate that raw data is available and ready for registry building"""
    logger.info("🔍 Validating dimension registry build setup...")
    
    # Check raw_dimension table
    try:
        cur.execute("SELECT COUNT(*) FROM dictionary.raw_dimension")
        raw_dim_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT productid) FROM dictionary.raw_dimension")
        dim_product_count = cur.fetchone()[0]
    except Exception as e:
        raise RuntimeError(f"❌ Cannot access raw_dimension table: {e}")
    
    # Check raw_member table  
    try:
        cur.execute("SELECT COUNT(*) FROM dictionary.raw_member")
        raw_member_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT productid) FROM dictionary.raw_member")
        member_product_count = cur.fetchone()[0]
    except Exception as e:
        raise RuntimeError(f"❌ Cannot access raw_member table: {e}")
    
    # Validate minimum data requirements
    if raw_dim_count < MIN_RAW_DIMENSIONS:
        raise ValueError(f"❌ Insufficient dimensions: {raw_dim_count} < {MIN_RAW_DIMENSIONS}")
    
    if raw_member_count < MIN_RAW_MEMBERS:
        raise ValueError(f"❌ Insufficient members: {raw_member_count} < {MIN_RAW_MEMBERS}")
    
    # Check data consistency
    if dim_product_count != member_product_count:
        logger.warning(f"⚠️  Product count mismatch: {dim_product_count} dims vs {member_product_count} members")
    
    # Get current registry state
    try:
        cur.execute("SELECT COUNT(*) FROM dictionary.dimension_set")
        existing_dim_sets = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM dictionary.dimension_set_member")
        existing_members = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM cube.cube_dimension_map")
        existing_mappings = cur.fetchone()[0]
    except Exception as e:
        logger.warning(f"⚠️  Cannot check existing registry tables: {e}")
        existing_dim_sets = existing_members = existing_mappings = 0
    
    stats = {
        'raw_dimensions': raw_dim_count,
        'raw_members': raw_member_count,
        'dimension_products': dim_product_count,
        'member_products': member_product_count,
        'existing_dimension_sets': existing_dim_sets,
        'existing_members': existing_members,
        'existing_mappings': existing_mappings
    }
    
    logger.success("✅ Registry build setup validated")
    logger.info(f"📊 Raw data: {raw_dim_count} dims, {raw_member_count} members from {dim_product_count} products")
    logger.info(f"📊 Existing registry: {existing_dim_sets} dim sets, {existing_members} members, {existing_mappings} mappings")
    
    return stats


def normalize_text(text) -> str:
    """Normalize text for consistent comparison"""
    if text is None:
        return ""
    return str(text).strip().lower()


def validate_text_field(text: str, field_name: str, max_length: int = MAX_LABEL_LENGTH) -> str:
    """Validate and clean text fields"""
    if text is None:
        return None
    
    text = str(text).strip()
    
    if len(text) > max_length:
        logger.warning(f"⚠️  {field_name} too long ({len(text)} chars), truncating: {text[:50]}...")
        text = text[:max_length]
    
    return text if text else ""


def hash_member_signature(code, label_en, parent_id=None, uom_code=None) -> str:
    """Generate consistent hash for member signature"""
    # Normalize inputs for consistent hashing
    code_norm = normalize_text(code)
    label_norm = normalize_text(label_en)
    parent_norm = normalize_text(parent_id)
    uom_norm = normalize_text(uom_code)
    
    # Create signature string
    signature = f"{code_norm}|{label_norm}|{parent_norm}|{uom_norm}"
    
    # Generate hash
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()


def hash_dimension_signature(member_hashes: list) -> str:
    """Generate consistent hash for dimension from member hashes"""
    if not member_hashes:
        return hashlib.sha256(b"empty").hexdigest()
    
    # Sort hashes for consistent ordering
    sorted_hashes = sorted(set(member_hashes))  # Remove duplicates and sort
    signature = "|".join(sorted_hashes)
    
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()


def load_and_validate_raw_data(conn) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load raw data from database with validation"""
    logger.info("📥 Loading raw dimension and member data...")
    
    try:
        # Load raw member data
        raw_member = pd.read_sql("""
            SELECT productid, dimension_position, member_id, parent_member_id,
                   classification_code, classification_type_code,
                   member_name_en, member_name_fr, member_uom_code,
                   geo_level, vintage, terminated
            FROM dictionary.raw_member
        """, conn)
        
        # Load raw dimension data
        raw_dim = pd.read_sql("""
            SELECT productid, dimension_position, dimension_name_en, dimension_name_fr, has_uom
            FROM dictionary.raw_dimension
        """, conn)
        
    except Exception as e:
        raise RuntimeError(f"❌ Failed to load raw data: {e}")
    
    logger.info(f"📊 Loaded {len(raw_member)} members and {len(raw_dim)} dimensions")
    
    # Validate data quality
    null_member_ids = raw_member['member_id'].isnull().sum()
    if null_member_ids > 0:
        logger.warning(f"⚠️  {null_member_ids} members have NULL member_id")
    
    null_dim_positions = raw_dim['dimension_position'].isnull().sum()
    if null_dim_positions > 0:
        logger.warning(f"⚠️  {null_dim_positions} dimensions have NULL position")
    
    # Clean and validate text fields
    raw_member['member_name_en'] = raw_member['member_name_en'].apply(
        lambda x: validate_text_field(x, 'member_name_en')
    )
    raw_member['member_name_fr'] = raw_member['member_name_fr'].apply(
        lambda x: validate_text_field(x, 'member_name_fr')
    )
    raw_dim['dimension_name_en'] = raw_dim['dimension_name_en'].apply(
        lambda x: validate_text_field(x, 'dimension_name_en')
    )
    raw_dim['dimension_name_fr'] = raw_dim['dimension_name_fr'].apply(
        lambda x: validate_text_field(x, 'dimension_name_fr')
    )
    
    return raw_member, raw_dim


def generate_member_hashes(raw_member: pd.DataFrame) -> pd.DataFrame:
    """Generate member hashes and detect duplicates"""
    logger.info("🔢 Generating member signature hashes...")
    
    # Generate member hashes
    raw_member['member_hash'] = raw_member.apply(
        lambda row: hash_member_signature(
            row['member_id'],
            row['member_name_en'],
            row['parent_member_id'],
            row['member_uom_code']
        ), axis=1
    )
    
    # Detect and report hash collisions
    total_members = len(raw_member)
    unique_hashes = raw_member['member_hash'].nunique()
    collision_rate = (total_members - unique_hashes) / total_members
    
    logger.info(f"📊 Member hashing: {total_members} members → {unique_hashes} unique hashes")
    if collision_rate > 0:
        logger.info(f"🔄 Hash collisions: {collision_rate:.1%} (expected for identical members)")
    
    return raw_member


def generate_dimension_hashes(raw_member: pd.DataFrame) -> pd.DataFrame:
    """Generate dimension hashes from aggregated member hashes"""
    logger.info("🏗️  Building dimension signatures from member aggregation...")
    
    # Group by dimension and aggregate member hashes
    dimension_groups = raw_member.groupby(['productid', 'dimension_position'])['member_hash'].apply(list).reset_index()
    dimension_groups['dimension_hash'] = dimension_groups['member_hash'].apply(hash_dimension_signature)
    
    # Add dimension hash back to raw_member
    raw_member = raw_member.merge(
        dimension_groups[['productid', 'dimension_position', 'dimension_hash']],
        on=['productid', 'dimension_position'], 
        how='left'
    )
    
    total_dimensions = len(dimension_groups)
    unique_dim_hashes = dimension_groups['dimension_hash'].nunique()
    deduplication_rate = (total_dimensions - unique_dim_hashes) / total_dimensions
    
    logger.info(f"📊 Dimension hashing: {total_dimensions} dimensions → {unique_dim_hashes} unique signatures")
    logger.success(f"🎯 Deduplication achieved: {deduplication_rate:.1%} reduction")
    
    return raw_member, dimension_groups


def resolve_label_conflicts(raw_member: pd.DataFrame, raw_dim: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Resolve label conflicts using frequency-based selection"""
    logger.info("🏷️  Resolving label conflicts with frequency-based selection...")
    
    # Resolve member label conflicts
    member_label_counts = (
        raw_member.groupby(['dimension_hash', 'member_id', 'member_name_en', 'member_name_fr'])
        .size().reset_index(name='count')
        .sort_values(['dimension_hash', 'member_id', 'count'], ascending=[True, True, False])
    )
    
    # Select most frequent labels for each member
    core_members = member_label_counts.drop_duplicates(subset=['dimension_hash', 'member_id'])
    
    # Add additional member metadata
    core_members = core_members.merge(
        raw_member[['dimension_hash', 'member_id', 'member_hash', 'classification_code', 
                   'classification_type_code', 'member_uom_code', 'parent_member_id',
                   'geo_level', 'vintage', 'terminated']].drop_duplicates(),
        on=['dimension_hash', 'member_id'], 
        how='left'
    )
    
    # Create normalized labels for validation
    core_members['member_label_norm'] = core_members['member_name_en'].apply(normalize_text)
    
    # Detect member label conflicts
    total_member_combinations = len(member_label_counts)
    unique_member_keys = len(core_members)
    member_conflict_rate = (total_member_combinations - unique_member_keys) / total_member_combinations if total_member_combinations > 0 else 0
    
    # Resolve dimension label conflicts
    raw_dim_with_hash = raw_dim.merge(
        raw_member[['productid', 'dimension_position', 'dimension_hash']].drop_duplicates(),
        on=['productid', 'dimension_position'], 
        how='left'
    )
    
    dim_label_counts = (
        raw_dim_with_hash.groupby(['dimension_hash', 'dimension_name_en', 'dimension_name_fr'])
        .size().reset_index(name='count')
        .sort_values(['dimension_hash', 'count'], ascending=[True, False])
    )
    
    core_dims = dim_label_counts.drop_duplicates(subset=['dimension_hash'])
    
    # Generate URL-safe slugs
    core_dims['dimension_name_slug'] = core_dims['dimension_name_en'].apply(
        lambda x: slugify(str(x) if x else 'unknown', separator='_')
    )
    
    # Detect dimension label conflicts
    total_dim_combinations = len(dim_label_counts)
    unique_dim_keys = len(core_dims)
    dim_conflict_rate = (total_dim_combinations - unique_dim_keys) / total_dim_combinations if total_dim_combinations > 0 else 0
    
    logger.info(f"📊 Label resolution:")
    logger.info(f"   Members: {member_conflict_rate:.1%} conflict rate")
    logger.info(f"   Dimensions: {dim_conflict_rate:.1%} conflict rate")
    
    if member_conflict_rate > CONFLICT_THRESHOLD:
        logger.warning(f"⚠️  High member label conflict rate: {member_conflict_rate:.1%}")
    if dim_conflict_rate > CONFLICT_THRESHOLD:
        logger.warning(f"⚠️  High dimension label conflict rate: {dim_conflict_rate:.1%}")
    
    return core_members, core_dims


def detect_metadata_flags(core_members: pd.DataFrame, core_dims: pd.DataFrame, raw_member: pd.DataFrame) -> pd.DataFrame:
    """Detect metadata flags for dimensions and members"""
    logger.info("🏴 Detecting metadata flags and patterns...")
    
    # Member-level flags
    core_members['is_total'] = core_members['member_name_en'].str.contains(
        'total', case=False, na=False
    )
    
    # Dimension-level flags
    dimension_flags = []
    
    for _, dim in core_dims.iterrows():
        dim_hash = dim['dimension_hash']
        dim_members = core_members[core_members['dimension_hash'] == dim_hash]
        raw_dim_members = raw_member[raw_member['dimension_hash'] == dim_hash]
        
        # Calculate flags
        has_total = dim_members['is_total'].any() if len(dim_members) > 0 else False
        is_tree = raw_dim_members['parent_member_id'].notna().any() if len(raw_dim_members) > 0 else False
        is_grabbag = any(term in str(dim['dimension_name_en']).lower() 
                        for term in ['characteristics', 'other', 'miscellaneous'])
        is_exclusive = False  # Placeholder for future logic
        
        dimension_flags.append({
            'dimension_hash': dim_hash,
            'has_total': has_total,
            'is_tree': is_tree,
            'is_grabbag': is_grabbag,
            'is_exclusive': is_exclusive
        })
    
    # Add flags to core_dims
    flags_df = pd.DataFrame(dimension_flags)
    core_dims = core_dims.merge(flags_df, on='dimension_hash', how='left')
    
    # Log flag statistics
    total_dims = len(core_dims)
    total_flag = core_dims['has_total'].sum()
    tree_flag = core_dims['is_tree'].sum()
    grabbag_flag = core_dims['is_grabbag'].sum()
    
    logger.info(f"📊 Metadata flags detected:")
    logger.info(f"   Has totals: {total_flag}/{total_dims} ({total_flag/total_dims:.1%})")
    logger.info(f"   Has hierarchy: {tree_flag}/{total_dims} ({tree_flag/total_dims:.1%})")
    logger.info(f"   Grab-bag dims: {grabbag_flag}/{total_dims} ({grabbag_flag/total_dims:.1%})")
    
    return core_dims


def insert_registry_data(cur, core_dims: pd.DataFrame, core_members: pd.DataFrame, raw_dim: pd.DataFrame, raw_member: pd.DataFrame):
    """Insert harmonized data into registry tables"""
    logger.info("📥 Inserting harmonized data into registry tables...")
    
    # Insert dimension_set
    logger.info("🏗️  Inserting dimension_set...")
    for _, row in core_dims.iterrows():
        cur.execute("""
            INSERT INTO dictionary.dimension_set (
                dimension_hash, dimension_name_en, dimension_name_fr, dimension_name_slug,
                has_total, is_exclusive, is_grabbag, is_tree
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (dimension_hash) DO UPDATE SET
                dimension_name_en = EXCLUDED.dimension_name_en,
                dimension_name_fr = EXCLUDED.dimension_name_fr,
                dimension_name_slug = EXCLUDED.dimension_name_slug,
                has_total = EXCLUDED.has_total,
                is_exclusive = EXCLUDED.is_exclusive,
                is_grabbag = EXCLUDED.is_grabbag,
                is_tree = EXCLUDED.is_tree
        """, (
            row['dimension_hash'],
            row['dimension_name_en'],
            row['dimension_name_fr'],
            row['dimension_name_slug'],
            row['has_total'],
            row['is_exclusive'],
            row['is_grabbag'],
            row['is_tree']
        ))
    
    # Insert dimension_set_member
    logger.info("👥 Inserting dimension_set_member...")
    for _, row in core_members.iterrows():
        # Convert terminated field safely
        terminated_val = row.get('terminated')
        if pd.isna(terminated_val) or terminated_val is None:
            terminated_val = None
        else:
            try:
                terminated_val = bool(int(terminated_val))
            except (ValueError, TypeError):
                terminated_val = None
        
        cur.execute("""
            INSERT INTO dictionary.dimension_set_member (
                dimension_hash, member_id, member_hash,
                classification_code, classification_type_code,
                member_name_en, member_name_fr, member_uom_code,
                parent_member_id, geo_level, vintage, terminated,
                is_total, member_label_norm
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (dimension_hash, member_id) DO UPDATE SET
                member_hash = EXCLUDED.member_hash,
                classification_code = EXCLUDED.classification_code,
                classification_type_code = EXCLUDED.classification_type_code,
                member_name_en = EXCLUDED.member_name_en,
                member_name_fr = EXCLUDED.member_name_fr,
                member_uom_code = EXCLUDED.member_uom_code,
                parent_member_id = EXCLUDED.parent_member_id,
                geo_level = EXCLUDED.geo_level,
                vintage = EXCLUDED.vintage,
                terminated = EXCLUDED.terminated,
                is_total = EXCLUDED.is_total,
                member_label_norm = EXCLUDED.member_label_norm
        """, (
            row['dimension_hash'],
            row['member_id'],
            row['member_hash'],
            row.get('classification_code'),
            row.get('classification_type_code'),
            row['member_name_en'],
            row['member_name_fr'],
            row.get('member_uom_code'),
            row.get('parent_member_id'),
            row.get('geo_level'),
            row.get('vintage'),
            terminated_val,
            row['is_total'],
            row['member_label_norm']
        ))
    
    # Insert cube_dimension_map
    logger.info("🔗 Inserting cube_dimension_map...")
    raw_dim_with_hash = raw_dim.merge(
        raw_member[['productid', 'dimension_position', 'dimension_hash']].drop_duplicates(),
        on=['productid', 'dimension_position'], 
        how='left'
    )
    
    cube_dim_lookup = raw_dim_with_hash[
        ['productid', 'dimension_position', 'dimension_hash', 'dimension_name_en', 'dimension_name_fr']
    ].drop_duplicates()
    
    cube_dim_lookup['dimension_name_slug'] = cube_dim_lookup['dimension_name_en'].apply(
        lambda x: slugify(str(x) if x else 'unknown', separator='_')
    )
    
    for _, row in cube_dim_lookup.iterrows():
        cur.execute("""
            INSERT INTO cube.cube_dimension_map (
                productid, dimension_position, dimension_hash,
                dimension_name_en, dimension_name_fr, dimension_name_slug
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (productid, dimension_position) DO UPDATE SET
                dimension_hash = EXCLUDED.dimension_hash,
                dimension_name_en = EXCLUDED.dimension_name_en,
                dimension_name_fr = EXCLUDED.dimension_name_fr,
                dimension_name_slug = EXCLUDED.dimension_name_slug
        """, (
            row['productid'],
            row['dimension_position'],
            row['dimension_hash'],
            row['dimension_name_en'],
            row['dimension_name_fr'],
            row['dimension_name_slug']
        ))
    
    logger.success(f"✅ Registry data inserted: {len(core_dims)} dimensions, {len(core_members)} members, {len(cube_dim_lookup)} mappings")


def validate_registry_quality(cur) -> dict:
    """Validate the quality of the built registry"""
    logger.info("🔍 Validating registry quality and coverage...")
    
    # Get registry statistics
    cur.execute("SELECT COUNT(*) FROM dictionary.dimension_set")
    final_dimensions = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM dictionary.dimension_set_member")
    final_members = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM cube.cube_dimension_map")
    final_mappings = cur.fetchone()[0]
    
    # Check coverage
    cur.execute("""
        SELECT COUNT(*) FROM dictionary.raw_dimension rd
        LEFT JOIN cube.cube_dimension_map cdm ON rd.productid = cdm.productid AND rd.dimension_position = cdm.dimension_position
        WHERE cdm.productid IS NULL
    """)
    unmapped_dimensions = cur.fetchone()[0]
    
    # Get deduplication stats
    cur.execute("SELECT COUNT(DISTINCT productid, dimension_position) FROM dictionary.raw_dimension")
    raw_dimension_count = cur.fetchone()[0]
    
    deduplication_ratio = (raw_dimension_count - final_dimensions) / raw_dimension_count if raw_dimension_count > 0 else 0
    coverage_ratio = (raw_dimension_count - unmapped_dimensions) / raw_dimension_count if raw_dimension_count > 0 else 0
    
    stats = {
        'final_dimensions': final_dimensions,
        'final_members': final_members,
        'final_mappings': final_mappings,
        'unmapped_dimensions': unmapped_dimensions,
        'deduplication_ratio': deduplication_ratio,
        'coverage_ratio': coverage_ratio
    }
    
    logger.success("✅ Registry quality validation complete")
    logger.info(f"📊 Final registry: {final_dimensions} dimensions, {final_members} members, {final_mappings} mappings")
    logger.info(f"🎯 Deduplication: {deduplication_ratio:.1%}, Coverage: {coverage_ratio:.1%}")
    
    if unmapped_dimensions > 0:
        logger.warning(f"⚠️  {unmapped_dimensions} dimensions remain unmapped")
    
    return stats


def main():
    logger.info("🚀 Starting enhanced dimension registry build...")
    
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Validate setup
                setup_stats = validate_registry_build_setup(cur)
                
                # Load and validate raw data
                raw_member, raw_dim = load_and_validate_raw_data(conn)
                
                # Generate member and dimension hashes
                raw_member = generate_member_hashes(raw_member)
                raw_member, dimension_groups = generate_dimension_hashes(raw_member)
                
                # Resolve label conflicts
                core_members, core_dims = resolve_label_conflicts(raw_member, raw_dim)
                
                # Detect metadata flags
                core_dims = detect_metadata_flags(core_members, core_dims, raw_member)
                
                # Insert registry data
                insert_registry_data(cur, core_dims, core_members, raw_dim, raw_member)
                
                # Commit transaction
                conn.commit()
                
                # Validate registry quality
                quality_stats = validate_registry_quality(cur)
                
                logger.success("✅ Enhanced dimension registry build completed successfully")
                logger.info("📋 Summary:")
                logger.info(f"   Input: {setup_stats['raw_dimensions']} raw dimensions, {setup_stats['raw_members']} raw members")
                logger.info(f"   Output: {quality_stats['final_dimensions']} dimension sets, {quality_stats['final_members']} members")
                logger.info(f"   Deduplication: {quality_stats['deduplication_ratio']:.1%}")
                logger.info(f"   Coverage: {quality_stats['coverage_ratio']:.1%}")

    except Exception as e:
        logger.exception(f"❌ Enhanced dimension registry build failed: {e}")
        raise


if __name__ == "__main__":
    main()

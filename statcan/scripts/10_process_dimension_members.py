#!/usr/bin/env python3
"""
Statcan Public Data ETL Pipeline
Script: 10_process_dimension_members.py
Date: 2025-06-21
Author: Paul Verbrugge with Claude Sonnet 4 (Anthropic)

Process and normalize raw dimension members with hash-based deduplication for registry building.

This script transforms raw member data from Statistics Canada metadata into normalized,
hash-based identifiers for cross-cube deduplication and harmonization. It generates
consistent member-level hashes based on structural identity (ID, label, parent, UOM)
while preserving all original metadata for downstream processing.

Key Operations:
- Generate deterministic member hashes from core identity attributes
- Normalize labels for consistent processing across languages
- Validate data types and handle edge cases (integer overflow, null values)
- Store processed member data with computed hashes for registry building
- Comprehensive data quality validation and statistics

Processing Logic:
1. Load raw member data from processing.raw_member (populated by script 09)
2. Apply minimal label normalization (lowercase, trim) for hash consistency
3. Generate member_hash from (member_id, label_en, parent_id, uom_code)
4. Validate data types and handle PostgreSQL integer range constraints
5. Insert processed data using PostgreSQL-first approach with conflict resolution
6. Generate deduplication statistics and data quality metrics

Dependencies:
- Requires processing.raw_member table from 09_dimension_raw_load.py
- Uses processing.processed_members table for output storage
- Feeds into 11_process_dimension.py for dimension-level hash generation
"""

import hashlib
import psycopg2
from loguru import logger
from statcan.tools.config import DB_CONFIG

# Configure logging with minimal approach
logger.add("/app/logs/process_dimension_members.log", rotation="5 MB", retention="7 days")

# Processing constants
BATCH_SIZE = 10000  # Process records in batches for memory efficiency
PG_INT_MIN = -2147483648  # PostgreSQL integer range constraints
PG_INT_MAX = 2147483647

def normalize_text(text):
    """Normalize text for consistent hashing across different inputs"""
    return str(text or "").strip().lower()

def hash_member_identity(member_id, label_en, parent_id=None, uom_code=None):
    """Generate deterministic hash for member identity
    
    Creates 12-character hash based on core structural identity:
    - member_id: Core member identifier
    - label_en: Normalized English label 
    - parent_id: Hierarchical parent relationship
    - uom_code: Unit of measure code
    
    Excludes classification codes, geo_level, vintage, terminated to focus
    on structural identity for cross-cube harmonization.
    """
    key_components = [
        normalize_text(member_id),
        normalize_text(label_en), 
        normalize_text(parent_id),
        normalize_text(uom_code)
    ]
    key = "|".join(key_components)
    full_hash = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return full_hash[:12]  # 12-char truncation for practical use

def validate_integer_ranges(cur):
    """Check for integer values that exceed PostgreSQL constraints"""
    int_columns = [
        'productid', 'dimension_position', 'member_id', 
        'parent_member_id', 'geo_level', 'vintage'
    ]
    
    overflow_issues = []
    
    for col in int_columns:
        cur.execute(f"""
            SELECT 
                COUNT(*) FILTER (WHERE {col} > %s) as over_max,
                COUNT(*) FILTER (WHERE {col} < %s) as under_min,
                MAX({col}) as max_val,
                MIN({col}) as min_val
            FROM processing.raw_member
            WHERE {col} IS NOT NULL
        """, (PG_INT_MAX, PG_INT_MIN))
        
        over_max, under_min, max_val, min_val = cur.fetchone()
        
        if over_max > 0 or under_min > 0:
            overflow_issues.append({
                'column': col,
                'over_max': over_max,
                'under_min': under_min,
                'range': f"{min_val} to {max_val}"
            })
    
    return overflow_issues

def get_raw_member_data():
    """Load raw member data with validation"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Verify source table exists and has data
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'processing' AND table_name = 'raw_member'
                )
            """)
            if not cur.fetchone()[0]:
                raise Exception("❌ Source table processing.raw_member does not exist")
            
            cur.execute("SELECT COUNT(*) FROM processing.raw_member")
            record_count = cur.fetchone()[0]
            
            if record_count == 0:
                raise Exception("❌ No raw member data found - run script 09 first")
            
            # Check for integer overflow issues
            overflow_issues = validate_integer_ranges(cur)
            if overflow_issues:
                for issue in overflow_issues:
                    logger.warning(f"⚠️ Integer range issues in {issue['column']}: {issue['over_max']} over max, {issue['under_min']} under min")
            
            # Load data with PostgreSQL processing
            cur.execute("""
                SELECT 
                    productid, dimension_position, member_id,
                    member_name_en, member_name_fr,
                    parent_member_id, member_uom_code,
                    classification_code, classification_type_code,
                    geo_level, vintage, terminated
                FROM processing.raw_member
                ORDER BY productid, dimension_position, member_id
            """)
            
            return cur.fetchall(), record_count

def process_member_batch(cur, batch_data, batch_num, total_batches):
    """Process a batch of member records with hash generation"""
    
    processed_count = 0
    invalid_count = 0
    
    for row in batch_data:
        (productid, dimension_position, member_id, member_name_en, member_name_fr,
         parent_member_id, member_uom_code, classification_code, 
         classification_type_code, geo_level, vintage, terminated) = row
        
        # Skip records with missing essential data
        if productid is None or dimension_position is None or member_id is None:
            invalid_count += 1
            continue
        
        # Generate normalized labels and hash
        member_name_en_norm = normalize_text(member_name_en)
        member_name_fr_norm = normalize_text(member_name_fr)
        
        member_hash = hash_member_identity(
            member_id, member_name_en_norm, parent_member_id, member_uom_code
        )
        
        # Convert terminated to boolean
        terminated_bool = bool(terminated) if terminated is not None else False
        
        # Insert with conflict resolution
        cur.execute("""
            INSERT INTO processing.processed_members (
                productid, dimension_position, member_id, member_hash,
                member_name_en, member_name_fr, parent_member_id, member_uom_code,
                classification_code, classification_type_code, geo_level, vintage,
                terminated, member_label_norm
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (productid, dimension_position, member_id) DO UPDATE SET
                member_hash = EXCLUDED.member_hash,
                member_name_en = EXCLUDED.member_name_en,
                member_name_fr = EXCLUDED.member_name_fr,
                member_label_norm = EXCLUDED.member_label_norm
        """, (
            productid, dimension_position, member_id, member_hash,
            member_name_en, member_name_fr, parent_member_id, member_uom_code,
            classification_code, classification_type_code, geo_level, vintage,
            terminated_bool, member_name_en_norm
        ))
        
        processed_count += 1
    
    return processed_count, invalid_count

def validate_processed_results():
    """Validate processing results and generate statistics"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Basic counts
            cur.execute("SELECT COUNT(*) FROM processing.processed_members")
            total_processed = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(DISTINCT member_hash) FROM processing.processed_members")
            unique_hashes = cur.fetchone()[0]
            
            # Data quality checks
            cur.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE member_hash IS NULL) as null_hashes,
                    COUNT(*) FILTER (WHERE member_name_en IS NULL) as null_en_names,
                    COUNT(*) FILTER (WHERE parent_member_id IS NOT NULL) as with_parents,
                    COUNT(DISTINCT productid) as unique_products
                FROM processing.processed_members
            """)
            null_hashes, null_en_names, with_parents, unique_products = cur.fetchone()
            
            # Calculate deduplication rate
            dedup_rate = ((total_processed - unique_hashes) / total_processed * 100) if total_processed > 0 else 0
            
            # Calculate hierarchy rate
            hierarchy_rate = (with_parents / total_processed * 100) if total_processed > 0 else 0
            
            return {
                'total_processed': total_processed,
                'unique_hashes': unique_hashes,
                'dedup_rate': dedup_rate,
                'hierarchy_rate': hierarchy_rate,
                'unique_products': unique_products,
                'null_hashes': null_hashes,
                'null_en_names': null_en_names,
                'with_parents': with_parents
            }

def main():
    """Main member processing function"""
    logger.info("🚀 Starting dimension member processing...")
    
    try:
        # Verify target table exists
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'processing' AND table_name = 'processed_members'
                    )
                """)
                if not cur.fetchone()[0]:
                    raise Exception("❌ Target table processing.processed_members does not exist")
        
        # Load raw data
        raw_data, total_records = get_raw_member_data()
        logger.info(f"📊 Processing {total_records:,} raw member records...")
        
        # Clear existing processed data
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE processing.processed_members")
                conn.commit()
        
        # Process in batches
        total_processed = 0
        total_invalid = 0
        total_batches = (len(raw_data) + BATCH_SIZE - 1) // BATCH_SIZE
        
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                for i in range(0, len(raw_data), BATCH_SIZE):
                    batch = raw_data[i:i + BATCH_SIZE]
                    batch_num = (i // BATCH_SIZE) + 1
                    
                    # Progress logging for long operations
                    if total_records > 10000 and i % 10000 == 0:
                        logger.info(f"📈 Progress: {i:,}/{total_records:,} records processed")
                    
                    processed_count, invalid_count = process_member_batch(
                        cur, batch, batch_num, total_batches
                    )
                    
                    total_processed += processed_count
                    total_invalid += invalid_count
                
                # Commit all changes
                conn.commit()
        
        # Validate results
        results = validate_processed_results()
        
        # Final summary
        logger.success(f"✅ Member processing complete: {results['total_processed']:,} processed, {total_invalid} invalid")
        
        # Warn about concerning issues
        if total_invalid > 0:
            invalid_rate = total_invalid / (total_processed + total_invalid) * 100
            if invalid_rate > 5:
                logger.warning(f"⚠️ High invalid rate: {invalid_rate:.1f}% - check data quality")
        
        if results['null_hashes'] > 0:
            logger.warning(f"⚠️ {results['null_hashes']} records with null hashes - hash generation failed")
        
        if results['null_en_names'] > 0:
            logger.warning(f"⚠️ {results['null_en_names']} members missing English names")
        
    except Exception as e:
        logger.exception(f"❌ Member processing failed: {e}")
        raise

if __name__ == "__main__":
    main()

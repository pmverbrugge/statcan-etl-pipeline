#!/usr/bin/env python3
"""
Statcan Public Data ETL Pipeline
Script: 11_process_dimension.py
Date: 2025-06-21
Author: Paul Verbrugge with Claude Sonnet 4 (Anthropic)

Generate dimension-level hashes from processed member data for registry deduplication.

This script creates dimension-level hashes by concatenating member hashes within each
(productid, dimension_position) group, enabling cross-cube dimension deduplication and
harmonization. The output provides the foundation for the canonical dimension registry.

Key Operations:
- Group processed members by (productid, dimension_position)
- Sort member hashes by member_id for deterministic ordering
- Generate dimension hash from concatenated member hashes
- Merge with raw dimension metadata (names, UOM flags)
- Store processed dimensions for registry building

Processing Logic:
1. Load processed members with hashes from processing.processed_members
2. Load raw dimension metadata from processing.raw_dimension  
3. Group members by dimension and concatenate sorted member hashes
4. Generate 12-character SHA-256 dimension hash from concatenated string
5. Merge dimension metadata and validate completeness
6. Store results in processing.processed_dimensions with conflict resolution

Dependencies:
- Requires processing.processed_members from 10_process_dimension_members.py
- Uses processing.raw_dimension for dimension metadata
- Outputs to processing.processed_dimensions for registry building
"""

import hashlib
import psycopg2
from loguru import logger
from statcan.tools.config import DB_CONFIG

# Configure logging with minimal approach
logger.add("/app/logs/process_dimensions.log", rotation="5 MB", retention="7 days")

# Processing constants
BATCH_SIZE = 1000  # Process dimension groups in batches

def hash_dimension_identity(member_hashes_concatenated):
    """Create deterministic hash for dimension from concatenated member hashes
    
    Args:
        member_hashes_concatenated: String of concatenated member hashes in sorted order
        
    Returns:
        str: 12-character SHA-256 hash representing dimension identity
    """
    full_hash = hashlib.sha256(member_hashes_concatenated.encode("utf-8")).hexdigest()
    return full_hash[:12]

def validate_prerequisites():
    """Validate that prerequisite tables exist and have data"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Check processed_members table
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'processing' AND table_name = 'processed_members'
                )
            """)
            if not cur.fetchone()[0]:
                raise Exception("❌ Source table processing.processed_members does not exist")
            
            cur.execute("SELECT COUNT(*) FROM processing.processed_members")
            member_count = cur.fetchone()[0]
            if member_count == 0:
                raise Exception("❌ No processed members found - run script 10 first")
            
            # Check raw_dimension table
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'processing' AND table_name = 'raw_dimension'
                )
            """)
            if not cur.fetchone()[0]:
                raise Exception("❌ Source table processing.raw_dimension does not exist")
            
            cur.execute("SELECT COUNT(*) FROM processing.raw_dimension")
            dimension_count = cur.fetchone()[0]
            if dimension_count == 0:
                raise Exception("❌ No raw dimension data found - run script 09 first")
            
            # Check target table
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'processing' AND table_name = 'processed_dimensions'
                )
            """)
            if not cur.fetchone()[0]:
                raise Exception("❌ Target table processing.processed_dimensions does not exist")
            
            return member_count, dimension_count

def get_dimension_groups():
    """Get unique dimension groups that need hash generation"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT productid, dimension_position
                FROM processing.processed_members
                ORDER BY productid, dimension_position
            """)
            return cur.fetchall()

def generate_dimension_hash(cur, productid, dimension_position):
    """Generate dimension hash for a specific dimension group
    
    Args:
        cur: Database cursor
        productid: Product identifier
        dimension_position: Dimension position within product
        
    Returns:
        str: Generated dimension hash
    """
    # Get sorted member hashes for this dimension
    cur.execute("""
        SELECT member_hash
        FROM processing.processed_members
        WHERE productid = %s AND dimension_position = %s
        ORDER BY member_id
    """, (productid, dimension_position))
    
    member_hashes = [row[0] for row in cur.fetchall()]
    
    if not member_hashes:
        return None
    
    # Concatenate sorted member hashes and generate dimension hash
    member_hashes_concat = ''.join(member_hashes)
    return hash_dimension_identity(member_hashes_concat)

def get_dimension_metadata(cur, productid, dimension_position):
    """Get raw dimension metadata for merging
    
    Returns:
        tuple: (dimension_name_en, dimension_name_fr, has_uom) or None if not found
    """
    cur.execute("""
        SELECT dimension_name_en, dimension_name_fr, has_uom
        FROM processing.raw_dimension
        WHERE productid = %s AND dimension_position = %s
    """, (productid, dimension_position))
    
    result = cur.fetchone()
    return result if result else (None, None, None)

def process_dimension_batch(cur, dimension_groups, start_idx, batch_size):
    """Process a batch of dimension groups"""
    processed_count = 0
    missing_metadata_count = 0
    
    for i in range(start_idx, min(start_idx + batch_size, len(dimension_groups))):
        productid, dimension_position = dimension_groups[i]
        
        # Generate dimension hash
        dimension_hash = generate_dimension_hash(cur, productid, dimension_position)
        
        if not dimension_hash:
            continue
        
        # Get dimension metadata
        dimension_name_en, dimension_name_fr, has_uom = get_dimension_metadata(
            cur, productid, dimension_position
        )
        
        if dimension_name_en is None and dimension_name_fr is None:
            missing_metadata_count += 1
        
        # Insert processed dimension
        cur.execute("""
            INSERT INTO processing.processed_dimensions (
                productid, dimension_position, dimension_hash,
                dimension_name_en, dimension_name_fr, has_uom
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (productid, dimension_position) DO UPDATE SET
                dimension_hash = EXCLUDED.dimension_hash,
                dimension_name_en = EXCLUDED.dimension_name_en,
                dimension_name_fr = EXCLUDED.dimension_name_fr,
                has_uom = EXCLUDED.has_uom
        """, (
            productid, dimension_position, dimension_hash,
            dimension_name_en, dimension_name_fr, has_uom
        ))
        
        processed_count += 1
    
    return processed_count, missing_metadata_count

def validate_results():
    """Validate processing results and generate statistics"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Basic counts
            cur.execute("SELECT COUNT(*) FROM processing.processed_dimensions")
            total_dimensions = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(DISTINCT dimension_hash) FROM processing.processed_dimensions")
            unique_hashes = cur.fetchone()[0]
            
            # Data quality checks
            cur.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE dimension_hash IS NULL) as null_hashes,
                    COUNT(*) FILTER (WHERE dimension_name_en IS NULL AND dimension_name_fr IS NULL) as missing_metadata,
                    COUNT(DISTINCT productid) as unique_products
                FROM processing.processed_dimensions
            """)
            null_hashes, missing_metadata, unique_products = cur.fetchone()
            
            # Calculate deduplication rate
            dedup_rate = ((total_dimensions - unique_hashes) / total_dimensions * 100) if total_dimensions > 0 else 0
            
            return {
                'total_dimensions': total_dimensions,
                'unique_hashes': unique_hashes,
                'dedup_rate': dedup_rate,
                'unique_products': unique_products,
                'null_hashes': null_hashes,
                'missing_metadata': missing_metadata
            }

def main():
    """Main dimension processing function"""
    logger.info("🚀 Starting dimension hash generation...")
    
    try:
        # Validate prerequisites
        member_count, raw_dimension_count = validate_prerequisites()
        
        # Get dimension groups to process
        dimension_groups = get_dimension_groups()
        logger.info(f"📊 Processing {len(dimension_groups)} dimension groups from {member_count:,} members...")
        
        # Clear existing processed dimensions
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE processing.processed_dimensions")
                conn.commit()
        
        # Process in batches
        total_processed = 0
        total_missing_metadata = 0
        
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                for i in range(0, len(dimension_groups), BATCH_SIZE):
                    # Progress logging for long operations
                    if len(dimension_groups) > 1000 and i % 1000 == 0:
                        logger.info(f"📈 Progress: {i:,}/{len(dimension_groups):,} dimensions processed")
                    
                    processed_count, missing_metadata_count = process_dimension_batch(
                        cur, dimension_groups, i, BATCH_SIZE
                    )
                    
                    total_processed += processed_count
                    total_missing_metadata += missing_metadata_count
                
                # Commit all changes
                conn.commit()
        
        # Validate results
        results = validate_results()
        
        # Final summary
        logger.success(f"✅ Dimension processing complete: {results['total_dimensions']:,} dimensions processed")
        
        # Warn about concerning issues
        if results['null_hashes'] > 0:
            logger.warning(f"⚠️ {results['null_hashes']} dimensions with null hashes - hash generation failed")
        
        if total_missing_metadata > 0:
            missing_rate = total_missing_metadata / results['total_dimensions'] * 100
            if missing_rate > 10:
                logger.warning(f"⚠️ High missing metadata rate: {missing_rate:.1f}% - check raw dimension data")
        
        if results['missing_metadata'] > 0:
            logger.warning(f"⚠️ {results['missing_metadata']} dimensions missing both English and French names")
        
    except Exception as e:
        logger.exception(f"❌ Dimension processing failed: {e}")
        raise

if __name__ == "__main__":
    main()

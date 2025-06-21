#!/usr/bin/env python3
"""
Statcan Public Data ETL Pipeline
Script: 09_dimension_raw_load.py
Date: 2025-06-21
Author: Paul Verbrugge with Claude Sonnet 4 (Anthropic)

Enhanced dimension metadata ingestion from downloaded JSON files into raw processing tables.

This script processes StatCan cube metadata files and extracts dimension definitions and 
member details into the processing schema for downstream normalization. It implements 
comprehensive validation, batch processing, and error recovery mechanisms.

Key Operations:
- Load metadata files based on metadata_status tracking table
- Extract dimension definitions and member hierarchies from JSON
- Insert raw data into processing.raw_dimension and processing.raw_member tables
- Comprehensive validation and data quality checks
- Batch processing with progress tracking and error isolation

Processing Logic:
1. Query metadata_status for completed downloads with file hashes
2. Validate file existence and JSON structure
3. Extract dimension metadata with proper null handling
4. Batch insert with conflict resolution and progress tracking
5. Validate final results and generate summary statistics

Dependencies:
- Requires metadata files from 08_metadata_download.py
- Uses raw_files.metadata_status for file tracking
- Populates processing.raw_dimension and processing.raw_member tables
"""

import psycopg2
import json
from pathlib import Path
from loguru import logger
from statcan.tools.config import DB_CONFIG

# Configure logging with standardized format
logger.add("/app/logs/load_raw_dimensions.log", rotation="5 MB", retention="7 days")

# Configuration constants
METADATA_DIR = Path("/app/raw/metadata")
BATCH_SIZE = 100  # Process files in batches for better memory management
MIN_FILE_SIZE = 1000  # Minimum expected file size in bytes

# Optimized SQL statements for batch processing
INSERT_DIM_SQL = """
    INSERT INTO processing.raw_dimension (
        productid, dimension_position, dimension_name_en, dimension_name_fr, has_uom
    ) VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (productid, dimension_position) DO NOTHING
"""

INSERT_MEMBER_SQL = """
    INSERT INTO processing.raw_member (
        productid, dimension_position, member_id, parent_member_id, 
        classification_code, classification_type_code, member_name_en, 
        member_name_fr, member_uom_code, geo_level, vintage, terminated
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (productid, dimension_position, member_id) DO NOTHING
"""

def safe_int(value):
    """Safely convert value to integer, returning None for invalid values"""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def safe_bool(value):
    """Safely convert value to boolean, handling various input types"""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ('true', '1', 'yes')
    return bool(value)

def validate_metadata_file(file_path: Path, productid: int) -> dict:
    """Validate metadata file exists and has valid JSON structure"""
    if not file_path.exists():
        return {'valid': False, 'error': f"File not found: {file_path.name}", 'data': None}
    
    # Check file size
    file_size = file_path.stat().st_size
    if file_size < MIN_FILE_SIZE:
        return {'valid': False, 'error': f"File too small: {file_size} bytes", 'data': None}
    
    # Validate JSON structure
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # Validate basic structure
        if not isinstance(data, list) or len(data) == 0:
            return {'valid': False, 'error': "Invalid JSON structure", 'data': None}
        
        # Check for required fields
        obj = data[0].get("object", {})
        if "productId" not in obj:
            return {'valid': False, 'error': "Missing productId in metadata", 'data': None}
        
        # Validate product ID matches expected
        if safe_int(obj.get("productId")) != productid:
            return {'valid': False, 'error': f"Product ID mismatch: expected {productid}, got {obj.get('productId')}", 'data': None}
        
        return {'valid': True, 'error': None, 'data': data}
        
    except json.JSONDecodeError as e:
        return {'valid': False, 'error': f"Invalid JSON: {e}", 'data': None}
    except Exception as e:
        return {'valid': False, 'error': f"File validation error: {e}", 'data': None}

def get_metadata_files_to_process():
    """Get list of metadata files to process from metadata_status table"""
    logger.info("📥 Loading metadata file list from database...")
    
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT productid, last_file_hash
                FROM raw_files.metadata_status
                WHERE download_pending = FALSE 
                  AND last_file_hash IS NOT NULL
                ORDER BY productid
            """)
            records = cur.fetchall()
    
    logger.info(f"📋 Found {len(records)} completed metadata entries to process")
    return records

def process_dimension_metadata(data: list, productid: int):
    """Extract dimension and member data from JSON metadata"""
    obj = data[0].get("object", {})
    dimensions_data = []
    members_data = []
    skipped_dims = 0
    skipped_members = 0
    
    for dim in obj.get("dimension", []):
        # Extract dimension data
        position = safe_int(dim.get("dimensionPositionId"))
        dim_name_en = dim.get("dimensionNameEn")
        dim_name_fr = dim.get("dimensionNameFr")
        has_uom = safe_bool(dim.get("hasUom"))
        
        # Skip dimensions with invalid positions (count for reporting)
        if position is None:
            skipped_dims += 1
            continue
        
        dimensions_data.append((
            productid, position, dim_name_en, dim_name_fr, has_uom
        ))
        
        # Extract member data
        for member in dim.get("member", []):
            member_id = safe_int(member.get("memberId"))
            
            # Skip members with invalid IDs (count for reporting)
            if member_id is None:
                skipped_members += 1
                continue
            
            members_data.append((
                productid,
                position,
                member_id,
                safe_int(member.get("parentMemberId")),
                member.get("classificationCode"),
                member.get("classificationTypeCode"),
                member.get("memberNameEn"),
                member.get("memberNameFr"),
                member.get("memberUomCode"),
                safe_int(member.get("geoLevel")),
                safe_int(member.get("vintage")),
                safe_int(member.get("terminated"))
            ))
    
    return dimensions_data, members_data, skipped_dims, skipped_members

def batch_insert_data(cur, dimensions_data: list, members_data: list):
    """Perform batch insertion of dimension and member data"""
    
    # Insert dimensions
    if dimensions_data:
        cur.executemany(INSERT_DIM_SQL, dimensions_data)
        dim_inserted = cur.rowcount
    else:
        dim_inserted = 0
    
    # Insert members
    if members_data:
        cur.executemany(INSERT_MEMBER_SQL, members_data)
        member_inserted = cur.rowcount
    else:
        member_inserted = 0
    
    return dim_inserted, member_inserted

def process_file_batch(records_batch: list, batch_num: int, total_batches: int):
    """Process a batch of metadata files with comprehensive error handling"""
    
    batch_stats = {
        'processed': 0,
        'failed': 0,
        'dimensions_inserted': 0,
        'members_inserted': 0,
        'validation_failures': 0,
        'total_skipped_dims': 0,
        'total_skipped_members': 0
    }
    
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for productid, file_hash in records_batch:
                try:
                    # Construct file path
                    filename = f"{productid}_{file_hash[:16]}.json"
                    file_path = METADATA_DIR / filename
                    
                    # Validate file
                    validation = validate_metadata_file(file_path, productid)
                    
                    if not validation['valid']:
                        batch_stats['validation_failures'] += 1
                        continue
                    
                    # Process metadata
                    dimensions_data, members_data, skipped_dims, skipped_members = process_dimension_metadata(validation['data'], productid)
                    
                    # Track skipped items for summary
                    batch_stats['total_skipped_dims'] += skipped_dims
                    batch_stats['total_skipped_members'] += skipped_members
                    
                    # Batch insert
                    dim_count, member_count = batch_insert_data(cur, dimensions_data, members_data)
                    
                    batch_stats['processed'] += 1
                    batch_stats['dimensions_inserted'] += dim_count
                    batch_stats['members_inserted'] += member_count
                    
                except Exception as e:
                    logger.error(f"❌ Error processing {productid}: {e}")
                    batch_stats['failed'] += 1
                    continue
            
            # Commit batch
            conn.commit()
    
    return batch_stats

def validate_final_results():
    """Validate final insertion results and generate summary statistics"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Count inserted records
            cur.execute("SELECT COUNT(*) FROM processing.raw_dimension")
            dim_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM processing.raw_member")
            member_count = cur.fetchone()[0]
            
            # Check for data quality issues
            cur.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE dimension_name_en IS NULL) as null_en_names,
                    COUNT(DISTINCT productid) as unique_products
                FROM processing.raw_dimension
            """)
            dim_null_en, unique_products = cur.fetchone()
            
            cur.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE member_name_en IS NULL) as null_en_names,
                    COUNT(*) FILTER (WHERE parent_member_id IS NOT NULL) as with_parents
                FROM processing.raw_member
            """)
            mem_null_en, mem_with_parents = cur.fetchone()
    
    # Calculate hierarchy rate
    hierarchy_rate = (mem_with_parents / member_count * 100) if member_count > 0 else 0
    
    return {
        'dimensions': dim_count,
        'members': member_count,
        'products': unique_products,
        'hierarchy_rate': hierarchy_rate,
        'dim_null_en': dim_null_en,
        'mem_null_en': mem_null_en
    }

def main():
    """Main metadata ingestion function with enhanced error handling and validation"""
    logger.info("🚀 Starting dimension metadata ingestion...")
    
    try:
        # Get files to process
        records = get_metadata_files_to_process()
        
        if not records:
            logger.info("📊 No metadata files found for processing")
            return
        
        logger.info(f"📊 Processing {len(records)} metadata files...")
        
        # Process in batches
        total_stats = {
            'processed': 0,
            'failed': 0,
            'dimensions_inserted': 0,
            'members_inserted': 0,
            'validation_failures': 0,
            'total_skipped_dims': 0,
            'total_skipped_members': 0
        }
        
        # Calculate batch parameters
        total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE
        
        # Process batches
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            
            # Progress logging for long operations
            if len(records) > 1000 and i % 1000 == 0:
                logger.info(f"📈 Progress: {i:,}/{len(records):,} files processed")
            
            batch_stats = process_file_batch(batch, batch_num, total_batches)
            
            # Accumulate statistics
            for key in total_stats:
                total_stats[key] += batch_stats[key]
        
        # Validate final results
        final_results = validate_final_results()
        
        # Final summary
        logger.success(f"✅ Metadata ingestion complete: {total_stats['processed']} processed, {total_stats['failed']} failed")
        
        # Warn about concerning issues
        if total_stats['failed'] > 0:
            failure_rate = total_stats['failed'] / len(records) * 100
            if failure_rate > 10:
                logger.warning(f"⚠️ High failure rate: {failure_rate:.1f}% - investigate logs")
        
        if final_results['dim_null_en'] > 0 or final_results['mem_null_en'] > 0:
            logger.warning(f"⚠️ Data quality issues: {final_results['dim_null_en']} dims, {final_results['mem_null_en']} members missing English names")
        
        if total_stats['total_skipped_dims'] > 0 or total_stats['total_skipped_members'] > 0:
            logger.warning(f"⚠️ Skipped invalid data: {total_stats['total_skipped_dims']} dimensions, {total_stats['total_skipped_members']} members")
        
    except Exception as e:
        logger.exception(f"❌ Metadata ingestion failed: {e}")
        raise

if __name__ == "__main__":
    main()

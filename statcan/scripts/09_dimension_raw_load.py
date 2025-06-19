"""
Enhanced Dimension Raw Load Script - Statistics Canada ETL Pipeline
====================================================================

This script parses downloaded metadata JSON files and extracts dimension definitions
and member details into normalized database tables. It implements comprehensive file
validation, JSON parsing safety, and data quality checks to ensure reliable metadata
ingestion and prevent database corruption from malformed files.

Key Features:
- Processes metadata files based on metadata_status tracking
- Extracts dimension definitions and member hierarchies from JSON
- Validates file integrity and JSON structure before processing
- Implements safe data type conversion and NULL handling
- Uses conflict-safe database operations for reprocessing capability
- Provides detailed audit trail of processing results

Process Flow:
1. Query metadata_status for successfully downloaded files
2. Validate each metadata file exists and is readable
3. Parse JSON content with comprehensive error handling
4. Extract and validate dimension and member data structures
5. Convert data types safely with proper NULL handling
6. Insert into raw_dimension and raw_member tables
7. Track processing statistics and handle individual file failures

Protection Mechanisms:
- File existence and integrity validation before processing
- JSON parsing with detailed error reporting
- Data structure validation and type conversion safety
- Database transaction isolation for individual files
- Comprehensive logging for debugging and audit trails
- Graceful handling of malformed or incomplete metadata

Data Processing:
- Extracts dimension metadata (position, names, UOM flags)
- Processes member hierarchies (IDs, labels, parent relationships)
- Handles StatCan's variable metadata structure gracefully
- Converts numeric fields with safe type coercion
- Preserves both English and French labels

Dependencies:
- Requires metadata files from 08_metadata_download.py
- Uses metadata_status tracking for file discovery
- Populates dictionary.raw_dimension and dictionary.raw_member tables

Last Updated: June 2025
Author: Paul Verbrugge
"""

import psycopg2
import json
import os
from pathlib import Path
from loguru import logger
from statcan.tools.config import DB_CONFIG

# Add file logging
logger.add("/app/logs/load_raw_dimensions.log", rotation="1 MB", retention="7 days")

# Configuration constants
METADATA_DIR = Path("/app/raw/metadata")
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB maximum file size
MIN_FILE_SIZE = 100  # 100 bytes minimum file size


def validate_processing_setup(cur) -> dict:
    """Validate that processing tables and directory are properly set up"""
    logger.info("🔍 Validating dimension processing setup...")
    
    # Check that metadata_status table exists and has data
    try:
        cur.execute("""
            SELECT COUNT(*) FROM raw_files.metadata_status 
            WHERE download_pending = FALSE AND last_file_hash IS NOT NULL
        """)
        ready_count = cur.fetchone()[0]
    except Exception as e:
        raise RuntimeError(f"❌ Cannot access metadata_status table: {e}")
    
    # Check that target tables exist
    try:
        cur.execute("SELECT COUNT(*) FROM dictionary.raw_dimension")
        dim_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM dictionary.raw_member")
        member_count = cur.fetchone()[0]
    except Exception as e:
        raise RuntimeError(f"❌ Cannot access dictionary tables: {e}")
    
    # Check that metadata directory exists
    if not METADATA_DIR.exists():
        raise RuntimeError(f"❌ Metadata directory does not exist: {METADATA_DIR}")
    
    stats = {
        'files_ready': ready_count,
        'existing_dimensions': dim_count,
        'existing_members': member_count,
        'metadata_dir': str(METADATA_DIR)
    }
    
    logger.success("✅ Dimension processing setup validated")
    logger.info(f"📊 Ready files: {ready_count}, Existing dims: {dim_count}, Existing members: {member_count}")
    
    return stats


def get_metadata_files_to_process(cur) -> list:
    """Get list of metadata files that are ready for processing"""
    logger.info("📋 Fetching metadata files ready for processing...")
    
    cur.execute("""
        SELECT productid, last_file_hash
        FROM raw_files.metadata_status
        WHERE download_pending = FALSE AND last_file_hash IS NOT NULL
        ORDER BY productid
    """)
    
    records = cur.fetchall()
    logger.info(f"📥 Found {len(records)} metadata files ready for processing")
    
    if records:
        sample_size = min(5, len(records))
        sample_ids = [str(record[0]) for record in records[:sample_size]]
        logger.info(f"📝 Sample product IDs: {sample_ids}")
    
    return records


def validate_metadata_file(productid: int, file_hash: str) -> tuple[Path, dict]:
    """Validate metadata file exists and is parseable"""
    logger.debug(f"🔍 Validating metadata file for product {productid}")
    
    # Construct expected filename
    filename = f"{productid}_{file_hash[:16]}.json"
    file_path = METADATA_DIR / filename
    
    # Check file existence
    if not file_path.exists():
        raise FileNotFoundError(f"Metadata file not found: {filename}")
    
    # Check file size
    file_size = file_path.stat().st_size
    if file_size < MIN_FILE_SIZE:
        raise ValueError(f"File too small: {file_size} bytes (minimum {MIN_FILE_SIZE})")
    
    if file_size > MAX_FILE_SIZE:
        raise ValueError(f"File too large: {file_size} bytes (maximum {MAX_FILE_SIZE})")
    
    # Validate JSON structure
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in file {filename}: {e}")
    except UnicodeDecodeError as e:
        raise ValueError(f"Invalid encoding in file {filename}: {e}")
    
    # Validate basic structure (should be array with success response)
    if not isinstance(data, list):
        raise ValueError(f"JSON root is not an array in {filename}")
    
    if len(data) == 0:
        raise ValueError(f"Empty JSON array in {filename}")
    
    first_element = data[0]
    if not isinstance(first_element, dict):
        raise ValueError(f"First array element is not an object in {filename}")
    
    if first_element.get('status') != 'SUCCESS':
        raise ValueError(f"API response status is not SUCCESS in {filename}: {first_element.get('status')}")
    
    if 'object' not in first_element:
        raise ValueError(f"Missing 'object' field in {filename}")
    
    metadata_obj = first_element['object']
    if not isinstance(metadata_obj, dict):
        raise ValueError(f"Metadata object is not a dictionary in {filename}")
    
    logger.debug(f"✅ File validation passed for {filename} ({file_size:,} bytes)")
    return file_path, data


def safe_int(value) -> int:
    """Safely convert value to integer, return None for invalid values"""
    if value is None:
        return None
    
    try:
        # Handle string representations of numbers
        if isinstance(value, str):
            value = value.strip()
            if value == '' or value.lower() in ('null', 'none', 'n/a'):
                return None
        
        return int(value)
    except (ValueError, TypeError):
        return None


def safe_bool(value) -> bool:
    """Safely convert value to boolean"""
    if value is None:
        return None
    
    if isinstance(value, bool):
        return value
    
    if isinstance(value, str):
        value = value.strip().lower()
        if value in ('true', '1', 'yes', 'y'):
            return True
        elif value in ('false', '0', 'no', 'n', ''):
            return False
    
    try:
        return bool(int(value))
    except (ValueError, TypeError):
        return None


def extract_dimension_data(productid: int, metadata_obj: dict) -> list:
    """Extract dimension definitions from metadata object"""
    dimensions = []
    
    dimension_list = metadata_obj.get('dimension', [])
    if not isinstance(dimension_list, list):
        logger.warning(f"⚠️  Dimension field is not a list for {productid}")
        return dimensions
    
    for dim in dimension_list:
        if not isinstance(dim, dict):
            logger.warning(f"⚠️  Dimension entry is not a dictionary for {productid}")
            continue
        
        try:
            dimension_data = {
                'productid': productid,
                'dimension_position': safe_int(dim.get('dimensionPositionId')),
                'dimension_name_en': dim.get('dimensionNameEn'),
                'dimension_name_fr': dim.get('dimensionNameFr'),
                'has_uom': safe_bool(dim.get('hasUom'))
            }
            
            # Validate required fields
            if dimension_data['dimension_position'] is None:
                logger.warning(f"⚠️  Missing dimension position for {productid}")
                continue
            
            dimensions.append(dimension_data)
            
        except Exception as e:
            logger.warning(f"⚠️  Error processing dimension for {productid}: {e}")
            continue
    
    return dimensions


def extract_member_data(productid: int, metadata_obj: dict) -> list:
    """Extract member definitions from metadata object"""
    members = []
    
    dimension_list = metadata_obj.get('dimension', [])
    if not isinstance(dimension_list, list):
        return members
    
    for dim in dimension_list:
        if not isinstance(dim, dict):
            continue
        
        dimension_position = safe_int(dim.get('dimensionPositionId'))
        if dimension_position is None:
            continue
        
        member_list = dim.get('member', [])
        if not isinstance(member_list, list):
            continue
        
        for member in member_list:
            if not isinstance(member, dict):
                continue
            
            try:
                member_data = {
                    'productid': productid,
                    'dimension_position': dimension_position,
                    'member_id': safe_int(member.get('memberId')),
                    'parent_member_id': safe_int(member.get('parentMemberId')),
                    'classification_code': member.get('classificationCode'),
                    'classification_type_code': member.get('classificationTypeCode'),
                    'member_name_en': member.get('memberNameEn'),
                    'member_name_fr': member.get('memberNameFr'),
                    'member_uom_code': member.get('memberUomCode'),
                    'geo_level': safe_int(member.get('geoLevel')),
                    'vintage': safe_int(member.get('vintage')),
                    'terminated': safe_int(member.get('terminated'))
                }
                
                # Validate required fields
                if member_data['member_id'] is None:
                    logger.debug(f"⚠️  Missing member ID for {productid}, position {dimension_position}")
                    continue
                
                members.append(member_data)
                
            except Exception as e:
                logger.warning(f"⚠️  Error processing member for {productid}: {e}")
                continue
    
    return members


def insert_dimension_data(cur, dimensions: list) -> int:
    """Insert dimension data into raw_dimension table"""
    if not dimensions:
        return 0
    
    insert_sql = """
        INSERT INTO dictionary.raw_dimension (
            productid, dimension_position, dimension_name_en, dimension_name_fr, has_uom
        ) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (productid, dimension_position) DO NOTHING
    """
    
    inserted = 0
    for dim in dimensions:
        try:
            cur.execute(insert_sql, (
                dim['productid'],
                dim['dimension_position'],
                dim['dimension_name_en'],
                dim['dimension_name_fr'],
                dim['has_uom']
            ))
            if cur.rowcount > 0:
                inserted += 1
        except Exception as e:
            logger.warning(f"⚠️  Failed to insert dimension for {dim['productid']}: {e}")
            continue
    
    return inserted


def insert_member_data(cur, members: list) -> int:
    """Insert member data into raw_member table"""
    if not members:
        return 0
    
    insert_sql = """
        INSERT INTO dictionary.raw_member (
            productid, dimension_position, member_id, parent_member_id, classification_code,
            classification_type_code, member_name_en, member_name_fr, member_uom_code,
            geo_level, vintage, terminated
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (productid, dimension_position, member_id) DO NOTHING
    """
    
    inserted = 0
    for member in members:
        try:
            cur.execute(insert_sql, (
                member['productid'],
                member['dimension_position'],
                member['member_id'],
                member['parent_member_id'],
                member['classification_code'],
                member['classification_type_code'],
                member['member_name_en'],
                member['member_name_fr'],
                member['member_uom_code'],
                member['geo_level'],
                member['vintage'],
                member['terminated']
            ))
            if cur.rowcount > 0:
                inserted += 1
        except Exception as e:
            logger.warning(f"⚠️  Failed to insert member for {member['productid']}: {e}")
            continue
    
    return inserted


def process_metadata_file(productid: int, file_hash: str) -> dict:
    """Process a single metadata file and extract dimension/member data"""
    logger.info(f"📥 Processing metadata for product {productid}")
    
    try:
        # Validate and load file
        file_path, data = validate_metadata_file(productid, file_hash)
        metadata_obj = data[0]['object']
        
        # Extract dimension and member data
        dimensions = extract_dimension_data(productid, metadata_obj)
        members = extract_member_data(productid, metadata_obj)
        
        # Insert data into database
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                dimensions_inserted = insert_dimension_data(cur, dimensions)
                members_inserted = insert_member_data(cur, members)
                conn.commit()
        
        stats = {
            'success': True,
            'dimensions_found': len(dimensions),
            'dimensions_inserted': dimensions_inserted,
            'members_found': len(members),
            'members_inserted': members_inserted,
            'file_path': str(file_path)
        }
        
        logger.success(f"✅ Processed {productid}: {dimensions_inserted} dims, {members_inserted} members")
        return stats
        
    except Exception as e:
        logger.error(f"❌ Failed to process metadata for {productid}: {e}")
        return {
            'success': False,
            'error': str(e),
            'dimensions_found': 0,
            'dimensions_inserted': 0,
            'members_found': 0,
            'members_inserted': 0
        }


def main():
    logger.info("🚀 Starting enhanced dimension raw load...")
    
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Validate setup
                setup_stats = validate_processing_setup(cur)
                
                # Get files to process
                metadata_records = get_metadata_files_to_process(cur)
                
                if not metadata_records:
                    logger.success("✅ No metadata files ready for processing")
                    return
                
                # Process each file
                total_files = len(metadata_records)
                successful = 0
                failed = 0
                total_dimensions = 0
                total_members = 0
                
                logger.info(f"📥 Processing {total_files} metadata files...")
                
                for i, (productid, file_hash) in enumerate(metadata_records, 1):
                    logger.info(f"🔄 Processing {i}/{total_files}: Product {productid}")
                    
                    stats = process_metadata_file(productid, file_hash)
                    
                    if stats['success']:
                        successful += 1
                        total_dimensions += stats['dimensions_inserted']
                        total_members += stats['members_inserted']
                    else:
                        failed += 1
                
                # Final summary
                logger.success(f"✅ Enhanced dimension raw load complete")
                logger.info(f"📊 Summary:")
                logger.info(f"   Files processed: {successful} successful, {failed} failed, {total_files} total")
                logger.info(f"   Data inserted: {total_dimensions} dimensions, {total_members} members")
                
                if failed > 0:
                    failure_rate = failed / total_files
                    if failure_rate > 0.1:  # More than 10% failure rate
                        logger.warning(f"⚠️  High failure rate: {failure_rate:.1%}")
                
    except Exception as e:
        logger.exception(f"❌ Enhanced dimension raw load failed: {e}")
        raise


if __name__ == "__main__":
    main()

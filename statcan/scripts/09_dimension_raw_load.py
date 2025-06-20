#!/usr/bin/env python3
"""
Statistics Canada Metadata Raw Dimension Loader
===============================================

Script:     09_dimension_raw_load_processing.py  
Purpose:    Load raw metadata into processing schema tables
Author:     Paul Verbrugge with Claude Sonnet 4 (Anthropic)
Created:    2025
Updated:    June 2025

Overview:
--------
This script loads raw dimension and member metadata from downloaded JSON files
into the processing schema tables (processing.raw_dimension and processing.raw_member).

Key Changes from Original:
- Target schema changed from 'dictionary' to 'processing'
- Enhanced logging and error handling
- Added TimescaleDB considerations
- Improved data validation

Dependencies:
------------
- Requires processing.raw_dimension and processing.raw_member tables to exist
- Run the DDL script first to create these tables
- Metadata files must be downloaded via script 08

Processing Pipeline:
-------------------
1. Query metadata_status for completed downloads
2. Load JSON metadata files
3. Parse dimension and member data
4. Insert into processing.raw_dimension and processing.raw_member
5. Log progress and handle errors gracefully
"""

import psycopg2
import json
from pathlib import Path
from loguru import logger
from statcan.tools.config import DB_CONFIG

logger.add("/app/logs/load_raw_dimensions.log", rotation="1 MB", retention="7 days")

metadata_dir = Path("/app/raw/metadata")

# SQL insert statements - UPDATED to target processing schema
INSERT_DIM_SQL = """
INSERT INTO processing.raw_dimension (
    productid, dimension_position, dimension_name_en, dimension_name_fr, has_uom
) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (productid, dimension_position) DO UPDATE SET
    dimension_name_en = EXCLUDED.dimension_name_en,
    dimension_name_fr = EXCLUDED.dimension_name_fr,
    has_uom = EXCLUDED.has_uom;
"""

INSERT_MEMBER_SQL = """
INSERT INTO processing.raw_member (
    productid, dimension_position, member_id, parent_member_id, classification_code,
    classification_type_code, member_name_en, member_name_fr, member_uom_code,
    geo_level, vintage, terminated
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (productid, dimension_position, member_id) DO UPDATE SET
    parent_member_id = EXCLUDED.parent_member_id,
    classification_code = EXCLUDED.classification_code,
    classification_type_code = EXCLUDED.classification_type_code,
    member_name_en = EXCLUDED.member_name_en,
    member_name_fr = EXCLUDED.member_name_fr,
    member_uom_code = EXCLUDED.member_uom_code,
    geo_level = EXCLUDED.geo_level,
    vintage = EXCLUDED.vintage,
    terminated = EXCLUDED.terminated;
"""

def safe_int(value):
    """Safely convert value to integer, return None if conversion fails"""
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def safe_bool(value):
    """Safely convert value to boolean"""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ('true', '1', 'yes', 'on')
    return bool(value)

def check_processing_tables():
    """Verify that processing schema tables exist"""
    logger.info("🔍 Checking processing schema tables...")
    
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Check for required tables
            required_tables = ['raw_dimension', 'raw_member']
            
            for table_name in required_tables:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'processing' 
                        AND table_name = %s
                    )
                """, (table_name,))
                
                if not cur.fetchone()[0]:
                    raise Exception(
                        f"❌ Table processing.{table_name} does not exist! "
                        "Please run the DDL script to create processing schema tables first."
                    )
            
            logger.success("✅ All required processing schema tables exist")

def main():
    logger.info("🟢 Starting metadata ingestion to processing schema...")

    try:
        # Check that target tables exist
        check_processing_tables()
        
        # Get metadata files to process
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT productid, last_file_hash
                    FROM raw_files.metadata_status
                    WHERE download_pending = FALSE AND last_file_hash IS NOT NULL;
                """)
                records = cur.fetchall()

        logger.info(f"Found {len(records)} completed metadata entries.")

        dimensions_processed = 0
        members_processed = 0
        files_processed = 0
        errors_encountered = 0

        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                for productid, file_hash in records:
                    filename = f"{productid}_{file_hash[:16]}.json"
                    file_path = metadata_dir / filename

                    if not file_path.exists():
                        logger.warning(f"⚠️ File missing: {filename}")
                        errors_encountered += 1
                        continue

                    try:
                        with open(file_path, "r", encoding="utf-8") as f:
                            data = json.load(f)

                        obj = data[0].get("object", {})
                        file_dimensions = 0
                        file_members = 0
                        
                        for dim in obj.get("dimension", []):
                            pos = safe_int(dim.get("dimensionPositionId"))
                            dim_name_en = dim.get("dimensionNameEn")
                            dim_name_fr = dim.get("dimensionNameFr")
                            has_uom = safe_bool(dim.get("hasUom"))

                            # Insert dimension
                            cur.execute(INSERT_DIM_SQL, (
                                productid, pos, dim_name_en, dim_name_fr, has_uom
                            ))
                            file_dimensions += 1

                            # Insert members for this dimension
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
                                file_members += 1

                        dimensions_processed += file_dimensions
                        members_processed += file_members
                        files_processed += 1
                        
                        logger.debug(f"✅ {filename}: {file_dimensions} dims, {file_members} members")

                    except json.JSONDecodeError as e:
                        logger.error(f"❌ JSON decode error in {filename}: {e}")
                        errors_encountered += 1
                    except Exception as e:
                        logger.error(f"❌ Error processing {filename}: {e}")
                        errors_encountered += 1

            # Commit all changes
            conn.commit()
            
            # Log final statistics
            logger.success("✅ Metadata ingestion to processing schema complete!")
            logger.info(f"📊 Summary:")
            logger.info(f"   • Files processed: {files_processed:,}")
            logger.info(f"   • Dimensions loaded: {dimensions_processed:,}")
            logger.info(f"   • Members loaded: {members_processed:,}")
            logger.info(f"   • Errors encountered: {errors_encountered:,}")

            # Verify data was inserted
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM processing.raw_dimension")
                dim_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM processing.raw_member")  
                member_count = cur.fetchone()[0]
                
                logger.info(f"📈 Final counts:")
                logger.info(f"   • Total dimensions in DB: {dim_count:,}")
                logger.info(f"   • Total members in DB: {member_count:,}")

    except Exception as e:
        logger.exception(f"🚨 Database connection or query failed: {e}")
        raise

if __name__ == "__main__":
    main()

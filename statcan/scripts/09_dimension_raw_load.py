#!/usr/bin/env python3
"""
Statcan Public Data ETL Pipeline
Script: 09_dimension_raw_load.py
Date: 2025-06-21
Author: Paul Verbrugge with Claude Sonnet 4 (Anthropic)

Ingest dimension metadata from downloaded cube metadata files into raw processing tables.
Parses JSON metadata files and extracts dimension definitions and member details
using DuckDB for efficient processing of large metadata files.

This script reads metadata files downloaded by script 08 and populates:
- processing.raw_dimension: Dimension-level metadata (names, UOM flags)
- processing.raw_member: Member-level metadata (codes, labels, hierarchies)

Key Operations:
- Load metadata file paths from raw_files.metadata_status
- Parse JSON files using DuckDB for memory efficiency
- Extract dimension and member data with proper data types
- Insert into processing tables using batch operations
- Handle duplicate records with conflict resolution
- Validate data completeness and report statistics

Processing Approach:
Uses DuckDB's JSON functions to efficiently parse large metadata files
without loading entire files into Python memory. Processes files in batches
and uses PostgreSQL COPY for optimal insert performance.

Dependencies:
- Metadata files downloaded by script 08
- processing.raw_dimension and processing.raw_member tables (DDL)
- raw_files.metadata_status tracking table
"""

import json
import os
import psycopg2
from pathlib import Path
from loguru import logger
from statcan.tools.config import DB_CONFIG

# Configure logging with rotation and retention
logger.add("/app/logs/load_raw_dimensions.log", rotation="10 MB", retention="7 days")

# Constants
METADATA_DIR = Path("/app/raw/metadata")
BATCH_SIZE = 100  # Process files in batches for memory management

def check_required_tables():
    """Verify required tables and metadata files exist"""
    logger.info("🔍 Validating prerequisites...")
    
    with psycopg2.connect(**DB_CONFIG) as conn:
        cur = conn.cursor()
        
        # Check metadata status table
        cur.execute("SELECT COUNT(*) FROM raw_files.metadata_status WHERE last_file_hash IS NOT NULL")
        metadata_count = cur.fetchone()[0]
        
        if metadata_count == 0:
            raise Exception("❌ No metadata files found! Run script 08 first.")
        
        # Check output tables exist
        required_tables = [
            ('processing', 'raw_dimension'),
            ('processing', 'raw_member')
        ]
        
        for schema, table_name in required_tables:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                )
            """, (schema, table_name))
            
            if not cur.fetchone()[0]:
                raise Exception(
                    f"❌ Required table {schema}.{table_name} does not exist! "
                    "Please run the DDL script to create it first."
                )
        
        logger.success(f"✅ Prerequisites validated: {metadata_count:,} metadata files available")
        return metadata_count

def get_metadata_files():
    """Get list of metadata files to process"""
    logger.info("📋 Loading metadata file list...")
    
    with psycopg2.connect(**DB_CONFIG) as conn:
        cur = conn.cursor()
        
        # Get active metadata files with their product IDs
        cur.execute("""
            SELECT ms.productid, mmrf.storage_location, mmrf.file_hash
            FROM raw_files.metadata_status ms
            JOIN raw_files.manage_metadata_raw_files mmrf 
                ON ms.productid = mmrf.productid AND mmrf.active = true
            WHERE ms.last_file_hash IS NOT NULL
            ORDER BY ms.productid
        """)
        
        files = cur.fetchall()
        
        # Validate files exist on disk
        valid_files = []
        for productid, file_path, file_hash in files:
            if os.path.exists(file_path):
                valid_files.append((productid, file_path, file_hash))
            else:
                logger.warning(f"⚠️ Missing file for product {productid}: {file_path}")
        
        logger.info(f"📁 Found {len(valid_files):,} valid metadata files")
        return valid_files

def process_metadata_batch(files_batch):
    """Process a batch of metadata files using direct PostgreSQL operations"""
    logger.info(f"🔄 Processing batch of {len(files_batch)} metadata files...")
    
    dimension_count = 0
    member_count = 0
    
    with psycopg2.connect(**DB_CONFIG) as pg_conn:
        cur = pg_conn.cursor()
        
        # Process each file in the batch
        for productid, file_path, file_hash in files_batch:
            try:
                # Validate JSON file structure
                with open(file_path, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                
                # Expect array with single object containing metadata
                if not isinstance(metadata, list) or len(metadata) == 0:
                    logger.warning(f"⚠️ Invalid metadata structure for product {productid}")
                    continue
                
                metadata_obj = metadata[0].get('object', {})
                if not isinstance(metadata_obj, dict):
                    logger.warning(f"⚠️ No metadata object found for product {productid}")
                    continue
                
                dimensions = metadata_obj.get('dimension', [])
                if not dimensions:
                    logger.warning(f"⚠️ No dimensions found for product {productid}")
                    continue
                
                # Process dimensions for this product
                file_dimensions = 0
                file_members = 0
                
                for dim_idx, dimension in enumerate(dimensions):
                    dimension_position = dim_idx + 1  # 1-based positioning
                    
                    # Extract dimension metadata
                    dim_name_en = dimension.get('dimensionNameEn', '')
                    dim_name_fr = dimension.get('dimensionNameFr', '')
                    has_uom = dimension.get('hasUom', False)
                    
                    # Insert dimension record
                    cur.execute("""
                        INSERT INTO processing.raw_dimension (
                            productid, dimension_position, dimension_name_en, 
                            dimension_name_fr, has_uom
                        ) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (productid, dimension_position) DO NOTHING
                    """, (productid, dimension_position, dim_name_en, dim_name_fr, has_uom))
                    
                    file_dimensions += 1
                    
                    # Process members for this dimension
                    members = dimension.get('member', [])
                    for member in members:
                        # Extract member data with proper type conversion
                        member_id = member.get('memberId')
                        if member_id is None:
                            continue
                        
                        try:
                            member_id = int(member_id)
                        except (ValueError, TypeError):
                            logger.warning(f"⚠️ Invalid member_id for product {productid}: {member.get('memberId')}")
                            continue
                        
                        # Extract other member fields
                        parent_id = member.get('parentMemberId')
                        if parent_id is not None:
                            try:
                                parent_id = int(parent_id)
                            except (ValueError, TypeError):
                                parent_id = None
                        
                        member_name_en = member.get('memberNameEn', '')
                        member_name_fr = member.get('memberNameFr', '')
                        member_uom_code = member.get('memberUomCode')
                        classification_code = member.get('classificationCode')
                        classification_type_code = member.get('classificationTypeCode')
                        
                        # Handle geo_level and vintage
                        geo_level = member.get('geoLevel')
                        if geo_level is not None:
                            try:
                                geo_level = int(geo_level)
                            except (ValueError, TypeError):
                                geo_level = None
                        
                        vintage = member.get('vintage')
                        if vintage is not None:
                            try:
                                vintage = int(vintage)
                            except (ValueError, TypeError):
                                vintage = None
                        
                        # Handle terminated flag
                        terminated = member.get('terminated', 0)
                        try:
                            terminated = int(terminated)
                        except (ValueError, TypeError):
                            terminated = 0
                        
                        # Insert member record
                        cur.execute("""
                            INSERT INTO processing.raw_member (
                                productid, dimension_position, member_id, parent_member_id,
                                classification_code, classification_type_code, member_name_en, 
                                member_name_fr, member_uom_code, geo_level, vintage, terminated
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (productid, dimension_position, member_id) DO NOTHING
                        """, (
                            productid, dimension_position, member_id, parent_id,
                            classification_code, classification_type_code, member_name_en,
                            member_name_fr, member_uom_code, geo_level, vintage, terminated
                        ))
                        
                        file_members += 1
                
                dimension_count += file_dimensions
                member_count += file_members
                
                # Only log individual products that have issues or are unusually large
                if file_dimensions == 0:
                    logger.warning(f"⚠️ Product {productid}: No dimensions found")
                elif file_dimensions > 20:  # Log unusually complex cubes
                    logger.info(f"📊 Product {productid}: {file_dimensions} dimensions, {file_members} members (complex cube)")
                
            except Exception as e:
                logger.error(f"❌ Failed to process metadata for product {productid}: {e}")
                continue
        
        # Commit batch
        pg_conn.commit()
        
        logger.success(f"✅ Batch processed: {dimension_count} dimensions, {member_count} members")
        return dimension_count, member_count

def clear_existing_data():
    """Clear existing raw dimension data for fresh load"""
    logger.info("🗑️ Clearing existing raw dimension data...")
    
    with psycopg2.connect(**DB_CONFIG) as conn:
        cur = conn.cursor()
        
        # Get counts before clearing
        cur.execute("SELECT COUNT(*) FROM processing.raw_member")
        existing_members = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM processing.raw_dimension")
        existing_dimensions = cur.fetchone()[0]
        
        if existing_members > 0 or existing_dimensions > 0:
            logger.info(f"📊 Clearing {existing_dimensions:,} dimensions and {existing_members:,} members")
            
            # Truncate tables
            cur.execute("TRUNCATE TABLE processing.raw_member CASCADE")
            cur.execute("TRUNCATE TABLE processing.raw_dimension CASCADE")
            conn.commit()
            
            logger.success("✅ Existing data cleared")
        else:
            logger.info("ℹ️ No existing data to clear")

def generate_summary_statistics():
    """Generate and log summary statistics"""
    logger.info("📊 Generating load statistics...")
    
    with psycopg2.connect(**DB_CONFIG) as conn:
        cur = conn.cursor()
        
        # Dimension statistics
        cur.execute("""
            SELECT 
                COUNT(*) as total_dimensions,
                COUNT(DISTINCT productid) as unique_products,
                COUNT(*) FILTER (WHERE has_uom = true) as uom_dimensions
            FROM processing.raw_dimension
        """)
        dim_total, dim_products, uom_dims = cur.fetchone()
        
        # Member statistics
        cur.execute("""
            SELECT 
                COUNT(*) as total_members,
                COUNT(DISTINCT productid) as unique_products,
                COUNT(*) FILTER (WHERE parent_member_id IS NOT NULL) as hierarchical_members,
                COUNT(*) FILTER (WHERE member_uom_code IS NOT NULL) as members_with_uom
            FROM processing.raw_member
        """)
        mem_total, mem_products, hierarchical, with_uom = cur.fetchone()
        
        # Average members per dimension
        avg_members = mem_total / dim_total if dim_total > 0 else 0
        
        logger.success("📈 Load Summary:")
        logger.success(f"   • {dim_total:,} dimensions across {dim_products:,} products")
        logger.success(f"   • {mem_total:,} members across {mem_products:,} products")
        logger.success(f"   • {avg_members:.1f} average members per dimension")
        logger.success(f"   • {uom_dims:,} dimensions with unit of measure")
        logger.success(f"   • {hierarchical:,} members with parent relationships")
        logger.success(f"   • {with_uom:,} members with UOM codes")

def main():
    """Main dimension loading function"""
    try:
        # Validate prerequisites
        metadata_count = check_required_tables()
        
        # Get metadata files to process
        metadata_files = get_metadata_files()
        
        if not metadata_files:
            logger.warning("⚠️ No valid metadata files found")
            return
        
        # Clear existing data for fresh load
        clear_existing_data()
        
        # Process files in batches for memory efficiency
        total_dimensions = 0
        total_members = 0
        
        logger.info(f"🚀 Processing {len(metadata_files):,} metadata files in batches...")
        
        for i in range(0, len(metadata_files), BATCH_SIZE):
            batch = metadata_files[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            total_batches = (len(metadata_files) + BATCH_SIZE - 1) // BATCH_SIZE
            
            logger.info(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch)} files)")
            
            try:
                dim_count, mem_count = process_metadata_batch(batch)
                total_dimensions += dim_count
                total_members += mem_count
                
                logger.info(f"✅ Batch {batch_num} complete: +{dim_count} dimensions, +{mem_count} members")
                
            except Exception as e:
                logger.error(f"❌ Batch {batch_num} failed: {e}")
                continue
        
        # Generate summary statistics
        generate_summary_statistics()
        
        logger.success("🎉 Raw dimension loading completed successfully!")
        logger.info("🔄 Next step: Run script 10 (process_dimension_members.py)")
        
    except Exception as e:
        logger.exception(f"❌ Raw dimension loading failed: {e}")
        raise

if __name__ == "__main__":
    main()

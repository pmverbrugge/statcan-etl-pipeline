#!/usr/bin/env python3
"""
Statistics Canada Dimension Hash Generator
=========================================

Script:     11_process_dimensions.py
Purpose:    Generate dimension hashes from processed member data
Author:     Paul Verbrugge with Claude Sonnet 4 (Anthropic)
Created:    2025
Updated:    June 2025

Overview:
--------
This script creates dimension-level hashes by concatenating member hashes
within each (productid, dimension_position) group, sorted by member_id.

Key Changes:
- Updated to read from processing.raw_dimension (instead of dictionary.raw_dimension)
- Enhanced error handling and validation
- TimescaleDB optimization support

Requires: 10_process_dimension_members.py to have run successfully first.

Key Operations:
--------------
• Group processed members by (productid, dimension_position)
• Sort by member_id within each group
• Concatenate member hashes in sorted order
• Hash the concatenated string and truncate to 12 characters
• Add raw dimension metadata FROM PROCESSING SCHEMA
• Store in processing.processed_dimensions table

Processing Pipeline:
-------------------
1. Load processed members from script 10
2. Load raw dimension metadata FROM PROCESSING SCHEMA
3. Group by (productid, dimension_position)  
4. Sort by member_id within each group
5. Concatenate member hashes
6. Create dimension_hash (12-char truncated SHA-256)
7. Merge with dimension metadata
8. Store (productid, dimension_position, dimension_hash, names, has_uom)
"""

import hashlib
import pandas as pd
import psycopg2
from loguru import logger
from statcan.tools.config import DB_CONFIG

logger.add("/app/logs/build_dimension_registry.log", rotation="1 MB", retention="7 days")

def hash_dimension_identity(member_hashes_concatenated):
    """Create deterministic hash for dimension from concatenated member hashes"""
    full_hash = hashlib.sha256(member_hashes_concatenated.encode("utf-8")).hexdigest()
    # Truncate to 12 characters for display convenience  
    return full_hash[:12]

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

def check_required_tables():
    """Verify required tables exist"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        
        required_tables = [
            ('processing', 'processed_members'),
            ('processing', 'raw_dimension'),
            ('processing', 'processed_dimensions')
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
        
        logger.info("✅ All required tables exist")

def process_dimensions():
    """Main dimension hash generation function"""
    logger.info("🚀 Starting dimension hash generation...")
    
    check_required_tables()
    
    with get_db_conn() as conn:
        # Load processed member data
        logger.info("📥 Loading processed member data...")
        processed_members = pd.read_sql("""
            SELECT productid, dimension_position, member_id, member_hash 
            FROM processing.processed_members
            ORDER BY productid, dimension_position, member_id
        """, conn)
        
        # Load raw dimension metadata FROM PROCESSING SCHEMA
        logger.info("📥 Loading raw dimension metadata from processing schema...")
        raw_dimensions = pd.read_sql("""
            SELECT productid, dimension_position, dimension_name_en, dimension_name_fr, has_uom
            FROM processing.raw_dimension
        """, conn)
        
        logger.info(f"📊 Processing {len(processed_members)} member records across {len(raw_dimensions)} dimensions")
        
        # Group by (productid, dimension_position) and concatenate member hashes
        logger.info("🔨 Generating dimension hashes...")
        
        def create_dimension_hash(group):
            """Create dimension hash from sorted member hashes"""
            # Sort by member_id (should already be sorted from SQL, but ensure consistency)
            group_sorted = group.sort_values('member_id')
            
            # Concatenate member hashes in order
            member_hashes_concat = ''.join(group_sorted['member_hash'])
            
            # Create dimension hash
            dimension_hash = hash_dimension_identity(member_hashes_concat)
            
            return pd.Series({'dimension_hash': dimension_hash})
        
        # Group and generate hashes
        dimension_hashes = (
            processed_members.groupby(['productid', 'dimension_position'])
            .apply(create_dimension_hash, include_groups=False)
            .reset_index()
        )
        
        logger.info(f"📈 Generated {len(dimension_hashes)} dimension hashes")
        
        # Add raw dimension metadata
        logger.info("🏷️ Adding raw dimension metadata...")
        dimension_data = dimension_hashes.merge(
            raw_dimensions,
            on=['productid', 'dimension_position'],
            how='left'
        )
        
        # Show deduplication statistics
        unique_hashes = len(dimension_data['dimension_hash'].unique())
        total_dimensions = len(dimension_data)
        deduplication_rate = ((total_dimensions - unique_hashes) / total_dimensions * 100)
        
        logger.info(f"🎯 Unique dimension hashes: {unique_hashes:,}")
        logger.info(f"🎯 Total dimensions: {total_dimensions:,}")
        logger.info(f"🎯 Deduplication rate: {deduplication_rate:.1f}%")
        
        # Store in processing table
        logger.info("💾 Storing dimension data...")
        
        cur = conn.cursor()
        
        # Clear existing data for fresh processing
        cur.execute("TRUNCATE TABLE processing.processed_dimensions")
        
        for _, row in dimension_data.iterrows():
            cur.execute("""
                INSERT INTO processing.processed_dimensions (
                    productid, dimension_position, dimension_hash,
                    dimension_name_en, dimension_name_fr, has_uom
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                int(row['productid']), 
                int(row['dimension_position']), 
                row['dimension_hash'],
                row['dimension_name_en'],
                row['dimension_name_fr'], 
                row['has_uom'] if pd.notna(row['has_uom']) else None
            ))
        
        conn.commit()
        
        logger.success(f"✅ Stored {len(dimension_data):,} dimension records with metadata")
        logger.info("🎯 Dimension processing complete")

def main():
    try:
        process_dimensions()
    except Exception as e:
        logger.exception(f"❌ Dimension hash generation failed: {e}")
        raise

if __name__ == "__main__":
    main()

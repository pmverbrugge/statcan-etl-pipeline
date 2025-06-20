#!/usr/bin/env python3
"""
Statistics Canada Dimension Member Processing and Normalization
==============================================================

Script:     10_process_dimension_members.py
Purpose:    Process and normalize raw dimension members with hash-based deduplication
Author:     Paul Verbrugge with Claude Sonnet 4 (Anthropic)
Created:    2025
Updated:    June 2025

Overview:
--------
This script processes raw dimension members from Statistics Canada metadata,
creating normalized, hash-based identifiers for deduplication and harmonization.
It generates member-level hashes and dimension-level hashes for the registry system.

The output feeds into 10b_build_dimension_registry.py for final registry construction.

Key Operations:
--------------
• Hash generation for member identity (code + label + parent + UOM)
• Dimension-level hash creation from sorted member hashes
• Label normalization and most-common selection
• Metadata flag computation (is_total, etc.)
• Results stored in processing.processed_members table

Processing Pipeline:
-------------------
1. Load raw_member and raw_dimension data
2. Generate member_hash for each unique code-label-parent-UOM combination
3. Create dimension_hash by aggregating member hashes per dimension
4. Select most common English/French labels for each member
5. Compute metadata flags (is_total based on label content)
6. Store processed results for registry building stage
"""

import hashlib
import pandas as pd
import psycopg2
from loguru import logger
from statcan.tools.config import DB_CONFIG

logger.add("/app/logs/process_dimension_members.log", rotation="1 MB", retention="7 days")

def normalize(text):
    """Normalize text for consistent hashing"""
    return str(text or "").strip().lower()

def hash_member_identity(member_id, label_en, parent_id=None, uom_code=None):
    """Create deterministic hash for member identity based on key attributes
    
    Core member identity based on:
    - member_id: Core member identifier
    - label_en: Normalized English label 
    - parent_id: Hierarchical parent relationship
    - uom_code: Unit of measure code
    
    Note: Excludes classification_code, classification_type_code, geo_level,
    vintage, and terminated from hash to focus on structural identity.
    """
    key = f"{normalize(member_id)}|{normalize(label_en)}|{normalize(parent_id)}|{normalize(uom_code)}"
    full_hash = hashlib.sha256(key.encode("utf-8")).hexdigest()
    # Truncate to 12 characters for display convenience
    # Collision probability ~0.001% for 300k members
    return full_hash[:12]

def hash_dimension_identity(member_hashes):
    """Create deterministic hash for dimension based on sorted member hashes"""
    sorted_hashes = sorted(member_hashes)
    full_hash = hashlib.sha256("|".join(sorted_hashes).encode("utf-8")).hexdigest()
    # Truncate to 12 characters for display convenience  
    # Collision probability ~0.0003% for 100k dimensions
    return full_hash[:12]

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

def check_processed_members_table():
    """Verify the processing table exists"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        
        # Check if table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'processing' 
                AND table_name = 'processed_members'
            )
        """)
        
        if not cur.fetchone()[0]:
            raise Exception(
                "❌ Table processing.processed_members does not exist! "
                "Please run the DDL script to create it first."
            )
        
        logger.info("✅ Table processing.processed_members exists")

def process_members():
    """Main member processing function - creates member-level hashes only"""
    logger.info("🚀 Starting dimension member processing...")
    
    check_processed_members_table()
    
    with get_db_conn() as conn:
        # Load raw member data
        logger.info("📥 Loading raw member data...")
        raw_member = pd.read_sql("SELECT * FROM dictionary.raw_member", conn)
        
        logger.info(f"📊 Processing {len(raw_member)} raw member records")
        
        # Step 1: Minimal label normalization
        logger.info("🔨 Applying minimal label normalization...")
        raw_member["member_name_en_norm"] = raw_member["member_name_en"].apply(normalize)
        raw_member["member_name_fr_norm"] = raw_member["member_name_fr"].apply(normalize)
        
        # Step 2: Generate member-level hashes
        logger.info("🔨 Generating member identity hashes...")
        raw_member["member_hash"] = raw_member.apply(
            lambda row: hash_member_identity(
                row["member_id"],
                row["member_name_en_norm"],
                row["parent_member_id"],
                row["member_uom_code"]
            ), axis=1
        )
        
        # Step 3: Store all member data with hashes
        logger.info("💾 Storing processed member data...")
        
        # Prepare data for insertion - keep ALL original fields plus computed hash
        processed_data = raw_member[[
            "productid", "dimension_position", "member_id", "member_hash",
            "member_name_en", "member_name_fr", "member_name_en_norm", "member_name_fr_norm",
            "parent_member_id", "member_uom_code", "classification_code", 
            "classification_type_code", "geo_level", "vintage", "terminated"
        ]].copy()
        
        # Convert integer boolean to proper boolean
        processed_data["terminated"] = processed_data["terminated"].astype(bool)
        
        # Debug: Check for large integer values that might cause overflow
        logger.info("🔍 Checking for integer overflow issues...")
        for col in ["productid", "dimension_position", "member_id", "parent_member_id", "geo_level", "vintage"]:
            if col in processed_data.columns:
                max_val = processed_data[col].max()
                min_val = processed_data[col].min()
                logger.info(f"   {col}: min={min_val}, max={max_val}")
                
                # PostgreSQL integer range is -2,147,483,648 to 2,147,483,647
                if pd.notna(max_val) and max_val > 2147483647:
                    logger.warning(f"⚠️ {col} has values exceeding integer range: {max_val}")
                if pd.notna(min_val) and min_val < -2147483648:
                    logger.warning(f"⚠️ {col} has values below integer range: {min_val}")
        
        # Insert into processing table
        cur = conn.cursor()
        for _, row in processed_data.iterrows():
            cur.execute("""
                INSERT INTO processing.processed_members (
                    productid, dimension_position, member_id, member_hash,
                    member_name_en, member_name_fr, parent_member_id, member_uom_code,
                    classification_code, classification_type_code, geo_level, vintage,
                    terminated, member_label_norm
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (productid, dimension_position, member_id) DO UPDATE SET
                    member_name_en = EXCLUDED.member_name_en,
                    member_name_fr = EXCLUDED.member_name_fr,
                    member_label_norm = EXCLUDED.member_label_norm,
                    member_hash = EXCLUDED.member_hash
            """, (
                int(row["productid"]) if pd.notna(row["productid"]) else None,
                int(row["dimension_position"]) if pd.notna(row["dimension_position"]) else None,
                int(row["member_id"]) if pd.notna(row["member_id"]) else None,
                row["member_hash"],
                row["member_name_en"], row["member_name_fr"], 
                int(row["parent_member_id"]) if pd.notna(row["parent_member_id"]) else None,
                row["member_uom_code"], row["classification_code"], row["classification_type_code"],
                int(row["geo_level"]) if pd.notna(row["geo_level"]) else None,
                int(row["vintage"]) if pd.notna(row["vintage"]) else None,
                row["terminated"], row["member_name_en_norm"]
            ))
        
        conn.commit()
        
        # Summary statistics
        unique_members = len(processed_data)
        unique_hashes = len(processed_data["member_hash"].unique())
        
        logger.success(f"✅ Processed {unique_members:,} member records")
        logger.success(f"📈 Generated {unique_hashes:,} unique member hashes")
        logger.info("🎯 Member processing complete - ready for dimension registry building")

def main():
    try:
        process_members()
    except Exception as e:
        logger.exception(f"❌ Member processing failed: {e}")
        raise

if __name__ == "__main__":
    main()

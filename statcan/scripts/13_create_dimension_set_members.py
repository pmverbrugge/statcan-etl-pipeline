#!/usr/bin/env python3
"""
Statistics Canada Dimension Set Members Builder
==============================================

Script:     13_create_dimension_set_members.py
Purpose:    Create canonical member definitions for each dimension
Author:     Paul Verbrugge with Claude Sonnet 4 (Anthropic)
Created:    2025
Updated:    June 2025

Overview:
--------
This script creates canonical member definitions by joining processed members
with their dimension hashes and summarizing to select the most common member
attributes within each dimension.

Requires: 10_process_dimension_members.py and 11_process_dimensions.py to have run first.

Key Operations:
--------------
• Join processed_members with processed_dimensions on (productid, dimension_position)
• Group by (dimension_hash, member_id) 
• Select most common member names (English and French)
• Preserve parent_member_id and member_uom_code
• Store canonical member definitions in processing.dimension_set_members

Processing Pipeline:
-------------------
1. Load processed members and processed dimensions
2. Join on (productid, dimension_position) to get dimension_hash for each member
3. Group by (dimension_hash, member_id) and select most common attributes
4. Store canonical member definitions
5. Generate summary statistics
"""

import pandas as pd
import psycopg2
from loguru import logger
from statcan.tools.config import DB_CONFIG

logger.add("/app/logs/create_dimension_set_members.log", rotation="1 MB", retention="7 days")

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

def check_required_tables():
    """Verify required tables exist"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        
        required_tables = [
            ('processing', 'processed_members'),
            ('processing', 'processed_dimensions'),
            ('processing', 'dimension_set_members')
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

def populate_dimension_hashes():
    """Populate dimension_hash in processed_members from processed_dimensions"""
    logger.info("🔗 Populating dimension hashes in processed_members...")
    
    with get_db_conn() as conn:
        cur = conn.cursor()
        
        # Update processed_members with dimension_hash from processed_dimensions
        cur.execute("""
            UPDATE processing.processed_members 
            SET dimension_hash = pd.dimension_hash
            FROM processing.processed_dimensions pd
            WHERE processing.processed_members.productid = pd.productid
            AND processing.processed_members.dimension_position = pd.dimension_position
        """)
        
        updated_rows = cur.rowcount
        conn.commit()
        
        logger.success(f"✅ Updated {updated_rows:,} member records with dimension hashes")

def create_dimension_set_members():
    """Create canonical member definitions for each dimension"""
    logger.info("🚀 Starting dimension set members creation...")
    
    check_required_tables()
    
    # First populate dimension hashes
    populate_dimension_hashes()
    
    with get_db_conn() as conn:
        # Load processed members with dimension hashes
        logger.info("📥 Loading processed members with dimension hashes...")
        
        members_with_dimensions = pd.read_sql("""
            SELECT 
                dimension_hash,
                member_id,
                member_name_en,
                member_name_fr,
                parent_member_id,
                member_uom_code
            FROM processing.processed_members
            WHERE dimension_hash IS NOT NULL
        """, conn)
        
        logger.info(f"📊 Processing {len(members_with_dimensions)} member instances")
        
        # Group by dimension_hash and member_id to create canonical definitions
        logger.info("🔨 Creating canonical member definitions...")
        
        def select_canonical_member_attributes(group):
            """Select most common member attributes within a dimension"""
            # Count frequency of each English name
            en_counts = group['member_name_en'].value_counts()
            most_common_en = en_counts.index[0] if len(en_counts) > 0 else None
            
            # Count frequency of each French name
            fr_counts = group['member_name_fr'].value_counts()
            most_common_fr = fr_counts.index[0] if len(fr_counts) > 0 else None
            
            # For parent_member_id and member_uom_code, take the most common non-null value
            parent_counts = group['parent_member_id'].value_counts(dropna=False)
            most_common_parent = parent_counts.index[0] if len(parent_counts) > 0 else None
            
            uom_counts = group['member_uom_code'].value_counts(dropna=False)
            most_common_uom = uom_counts.index[0] if len(uom_counts) > 0 else None
            
            # Count usage across cube instances
            usage_count = len(group)
            
            return pd.Series({
                'member_name_en': most_common_en,
                'member_name_fr': most_common_fr,
                'parent_member_id': most_common_parent if pd.notna(most_common_parent) else None,
                'member_uom_code': most_common_uom if pd.notna(most_common_uom) else None,
                'usage_count': usage_count
            })
        
        # Create canonical member definitions
        canonical_members = (
            members_with_dimensions.groupby(['dimension_hash', 'member_id'])
            .apply(select_canonical_member_attributes, include_groups=False)
            .reset_index()
        )
        
        logger.info(f"📈 Created {len(canonical_members)} canonical member definitions")
        
        # Store canonical member definitions
        logger.info("💾 Storing canonical member definitions...")
        cur = conn.cursor()
        
        # Clear existing data
        cur.execute("TRUNCATE TABLE processing.dimension_set_members")
        
        # Insert canonical member definitions
        for _, row in canonical_members.iterrows():
            cur.execute("""
                INSERT INTO processing.dimension_set_members (
                    dimension_hash, member_id, member_name_en, member_name_fr,
                    parent_member_id, member_uom_code, usage_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                row['dimension_hash'],
                int(row['member_id']),
                row['member_name_en'],
                row['member_name_fr'],
                int(row['parent_member_id']) if pd.notna(row['parent_member_id']) else None,
                row['member_uom_code'],
                int(row['usage_count'])
            ))
        
        conn.commit()
        logger.success(f"✅ Stored {len(canonical_members)} canonical member definitions")

def generate_summary_stats():
    """Generate and log summary statistics"""
    logger.info("📊 Generating member statistics...")
    
    with get_db_conn() as conn:
        # Total canonical members
        result = pd.read_sql("SELECT COUNT(*) as count FROM processing.dimension_set_members", conn)
        total_members = result.iloc[0]['count']
        
        # Members per dimension statistics
        result = pd.read_sql("""
            SELECT 
                COUNT(*) as member_count,
                dimension_hash
            FROM processing.dimension_set_members 
            GROUP BY dimension_hash 
            ORDER BY member_count DESC 
            LIMIT 5
        """, conn)
        
        # Most used members
        top_members = pd.read_sql("""
            SELECT member_name_en, usage_count 
            FROM processing.dimension_set_members 
            ORDER BY usage_count DESC 
            LIMIT 5
        """, conn)
        
        logger.success(f"📈 Member Summary:")
        logger.success(f"   • {total_members:,} canonical members across all dimensions")
        
        logger.info("🏆 Top 5 largest dimensions (by member count):")
        for _, dim in result.iterrows():
            logger.info(f"   • {dim['dimension_hash']}: {dim['member_count']:,} members")
            
        logger.info("🏆 Top 5 most used members:")
        for _, member in top_members.iterrows():
            logger.info(f"   • {member['member_name_en']}: {member['usage_count']:,} uses")

def main():
    """Main member definition creation function"""
    try:
        # Create canonical member definitions
        create_dimension_set_members()
        
        # Generate summary
        generate_summary_stats()
        
        logger.success("🎉 Dimension set members creation complete!")
        
    except Exception as e:
        logger.exception(f"❌ Member creation failed: {e}")
        raise

if __name__ == "__main__":
    main()

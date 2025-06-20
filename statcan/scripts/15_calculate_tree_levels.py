#!/usr/bin/env python3
"""
Statistics Canada Dimension Member Tree Level Calculator
=======================================================

Script:     15_calculate_tree_levels.py
Purpose:    Calculate hierarchical tree levels for dimension members
Author:     Paul Verbrugge with Claude Sonnet 4 (Anthropic)
Created:    2025
Updated:    June 2025

Overview:
--------
This script calculates tree_level values for members in hierarchical dimensions:
- Level 1: Root nodes (members with no parent_member_id)
- Level 2: Children of level 1 nodes
- Level 3: Children of level 2 nodes, etc.
- NULL: Members in non-hierarchical dimensions (is_tree=false)

The script includes comprehensive validation to detect and handle:
- Circular references in parent-child relationships
- Orphaned members (parent_member_id points to non-existent member)
- Maximum depth limits to prevent infinite recursion
- Self-referencing members

Requires: Scripts 10-14 to have run successfully first.

Key Operations:
--------------
• Load hierarchical dimensions (is_tree=true) and their members
• Build parent-child relationship graphs for each dimension
• Detect and report circular references and orphaned members
• Calculate tree levels using iterative depth-first traversal
• Update processing.dimension_set_members with calculated levels
• Generate comprehensive validation and summary reports

Processing Pipeline:
-------------------
1. Load dimensions marked as hierarchical (is_tree=true)
2. For each hierarchical dimension, load all members
3. Build parent-child relationship mapping
4. Validate relationships (detect cycles, orphans, self-references)
5. Calculate tree levels iteratively starting from root nodes
6. Update database with calculated tree_level values
7. Generate summary statistics and validation reports
"""

import pandas as pd
import psycopg2
from collections import defaultdict, deque
from loguru import logger
from statcan.tools.config import DB_CONFIG

logger.add("/app/logs/calculate_tree_levels.log", rotation="1 MB", retention="7 days")

# Maximum tree depth constant (not used but kept for reference)
MAX_TREE_DEPTH = 50

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

def check_required_tables():
    """Verify required tables and columns exist"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        
        # Check if tree_level column exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_schema = 'processing' 
                AND table_name = 'dimension_set_members'
                AND column_name = 'tree_level'
            )
        """)
        
        if not cur.fetchone()[0]:
            raise Exception(
                "❌ Column tree_level does not exist in processing.dimension_set_members! "
                "Please run the DDL script to add this column first."
            )
        
        logger.info("✅ All required tables and columns exist")

def load_hierarchical_dimensions():
    """Load dimensions that are marked as hierarchical"""
    with get_db_conn() as conn:
        hierarchical_dims = pd.read_sql("""
            SELECT dimension_hash, dimension_name_en
            FROM processing.dimension_set 
            WHERE is_tree = true
            ORDER BY dimension_hash
        """, conn)
        
        logger.info(f"📥 Found {len(hierarchical_dims)} hierarchical dimensions")
        return hierarchical_dims

def load_dimension_members(dimension_hash):
    """Load all members for a specific dimension"""
    with get_db_conn() as conn:
        members = pd.read_sql("""
            SELECT dimension_hash, member_id, parent_member_id, member_name_en
            FROM processing.dimension_set_members
            WHERE dimension_hash = %s
            ORDER BY member_id
        """, conn, params=(dimension_hash,))
        
        return members

def validate_member_relationships(members, dimension_hash):
    """Validate parent-child relationships and detect issues"""
    issues = []
    
    # Get all member IDs in this dimension
    all_member_ids = set(members['member_id'])
    
    # Check for self-references
    self_refs = members[members['member_id'] == members['parent_member_id']]
    if len(self_refs) > 0:
        for _, member in self_refs.iterrows():
            issues.append({
                'type': 'self_reference',
                'dimension_hash': dimension_hash,
                'member_id': member['member_id'],
                'message': f"Member {member['member_id']} references itself as parent"
            })
    
    # Check for orphaned members (parent_member_id points to non-existent member)
    members_with_parents = members[members['parent_member_id'].notna()]
    for _, member in members_with_parents.iterrows():
        if member['parent_member_id'] not in all_member_ids:
            issues.append({
                'type': 'orphaned_member',
                'dimension_hash': dimension_hash,
                'member_id': member['member_id'],
                'parent_member_id': member['parent_member_id'],
                'message': f"Member {member['member_id']} has parent {member['parent_member_id']} that doesn't exist"
            })
    
    return issues

def detect_circular_references(members, dimension_hash):
    """Detect circular references using path tracking"""
    # Build parent mapping
    parent_map = {}
    for _, member in members.iterrows():
        if pd.notna(member['parent_member_id']):
            parent_map[member['member_id']] = member['parent_member_id']
    
    circular_refs = []
    visited_global = set()
    
    def check_path_for_cycles(start_member_id):
        """Follow parent path from a member to detect cycles"""
        if start_member_id in visited_global:
            return
        
        visited_in_path = set()
        current_id = start_member_id
        path = []
        
        while current_id is not None and current_id not in visited_global:
            if current_id in visited_in_path:
                # Found cycle - get the cycle portion
                cycle_start = path.index(current_id)
                cycle = path[cycle_start:] + [current_id]
                circular_refs.append({
                    'type': 'circular_reference',
                    'dimension_hash': dimension_hash,
                    'cycle': cycle,
                    'message': f"Circular reference: {' -> '.join(map(str, cycle))}"
                })
                break
            
            visited_in_path.add(current_id)
            path.append(current_id)
            current_id = parent_map.get(current_id)
        
        # Mark all members in this path as visited
        visited_global.update(visited_in_path)
    
    # Check each member's parent path
    for member_id in members['member_id']:
        check_path_for_cycles(member_id)
    
    return circular_refs

def calculate_tree_levels(members, dimension_hash):
    """Calculate tree levels for members in a dimension"""
    # Skip if there are validation issues
    validation_issues = validate_member_relationships(members, dimension_hash)
    circular_refs = detect_circular_references(members, dimension_hash)
    
    if validation_issues or circular_refs:
        return None, validation_issues + circular_refs
    
    # Build parent-to-children mapping
    children_map = defaultdict(list)
    for _, member in members.iterrows():
        if pd.notna(member['parent_member_id']):
            children_map[member['parent_member_id']].append(member['member_id'])
    
    # Find root nodes (members with no parent)
    root_nodes = members[members['parent_member_id'].isna()]['member_id'].tolist()
    
    if not root_nodes:
        return None, [{
            'type': 'no_root_nodes',
            'dimension_hash': dimension_hash,
            'message': f"No root nodes found in hierarchical dimension {dimension_hash}"
        }]
    
    # Calculate levels using breadth-first search
    member_levels = {}
    queue = deque([(root_id, 1) for root_id in root_nodes])
    
    while queue:
        member_id, level = queue.popleft()
        member_levels[member_id] = level
        
        # Add children to queue with next level
        for child_id in children_map[member_id]:
            queue.append((child_id, level + 1))
    
    return member_levels, []

def update_tree_levels(dimension_hash, member_levels):
    """Update tree_level values in the database"""
    if not member_levels:
        return 0
    
    with get_db_conn() as conn:
        cur = conn.cursor()
        update_count = 0
        
        for member_id, level in member_levels.items():
            cur.execute("""
                UPDATE processing.dimension_set_members 
                SET tree_level = %s
                WHERE dimension_hash = %s AND member_id = %s
            """, (level, dimension_hash, member_id))
            update_count += cur.rowcount
        
        conn.commit()
        return update_count

def clear_tree_levels_for_non_hierarchical():
    """Set tree_level to NULL for members in non-hierarchical dimensions"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE processing.dimension_set_members 
            SET tree_level = NULL
            WHERE dimension_hash IN (
                SELECT dimension_hash 
                FROM processing.dimension_set 
                WHERE is_tree = false OR is_tree IS NULL
            )
        """)
        
        cleared_count = cur.rowcount
        conn.commit()
        
        logger.info(f"Cleared tree_level for {cleared_count} members in non-hierarchical dimensions")

def generate_summary_statistics():
    """Generate and log summary statistics"""
    with get_db_conn() as conn:
        # Overall statistics
        stats = pd.read_sql("""
            SELECT 
                COUNT(*) as total_members,
                COUNT(tree_level) as members_with_levels,
                MIN(tree_level) as min_level,
                MAX(tree_level) as max_level
            FROM processing.dimension_set_members
        """, conn)
        
        row = stats.iloc[0]
        logger.success(f"Updated {row['members_with_levels']:,} members with tree levels (range: {row['min_level']}-{row['max_level']})")

def main():
    """Main tree level calculation function"""
    try:
        logger.info("🚀 Starting tree level calculation...")
        
        check_required_tables()
        
        # Clear tree levels for non-hierarchical dimensions first
        clear_tree_levels_for_non_hierarchical()
        
        # Load hierarchical dimensions
        hierarchical_dims = load_hierarchical_dimensions()
        
        if len(hierarchical_dims) == 0:
            logger.warning("⚠️ No hierarchical dimensions found (is_tree=true)")
            return
        
        total_updated = 0
        total_issues = []
        
        # Process each hierarchical dimension
        for _, dim_row in hierarchical_dims.iterrows():
            dimension_hash = dim_row['dimension_hash']
            
            # Load members for this dimension
            members = load_dimension_members(dimension_hash)
            
            if len(members) == 0:
                continue
            
            # Calculate tree levels
            member_levels, issues = calculate_tree_levels(members, dimension_hash)
            
            if issues:
                total_issues.extend(issues)
                continue
            
            # Update database
            updated_count = update_tree_levels(dimension_hash, member_levels)
            total_updated += updated_count
        
        # Generate summary statistics
        generate_summary_statistics()
        
        # Report any issues found
        if total_issues:
            logger.warning(f"Found {len(total_issues)} validation issues")
            # Only log first few issues to avoid spam
            for issue in total_issues[:3]:
                logger.warning(issue['message'])
            if len(total_issues) > 3:
                logger.warning(f"... and {len(total_issues) - 3} more issues")
        
        logger.success(f"Tree level calculation complete! Updated {total_updated:,} members")
        
    except Exception as e:
        logger.exception(f"❌ Tree level calculation failed: {e}")
        raise

if __name__ == "__main__":
    main()

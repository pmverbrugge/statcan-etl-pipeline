#!/usr/bin/env python3
"""
Statcan Public Data ETL Pipeline
Script: generate_project_structure.py
Date: 2025-06-21
Author: Paul Verbrugge with Claude 4 Sonnet (Anthropic)

Generate documentation of project structure for development reference.
Excludes sensitive files and focuses on publicly shareable structure.
"""

import os
import json
from pathlib import Path
from datetime import datetime

# Files/directories to exclude for security
EXCLUDE_PATTERNS = [
    '__pycache__',
    '.pyc',
    '.env',
    'config.py',  # May contain credentials
    '.git',
    'logs',  # May contain sensitive data
    '.log',
    'raw',   # Contains downloaded data
    'staging',
    'warehouse',
    'postgres',  # Database files
    'backups'
]

# File extensions to include content for
INCLUDE_CONTENT_EXTENSIONS = [
    '.py', '.sql', '.md', '.txt', '.yml', '.yaml', '.dockerfile'
]

def should_exclude(path_str):
    """Check if path should be excluded for security reasons"""
    return any(pattern in path_str.lower() for pattern in EXCLUDE_PATTERNS)

def get_file_info(file_path):
    """Get basic file information"""
    try:
        stat = file_path.stat()
        return {
            'size': stat.st_size,
            'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
            'type': 'file'
        }
    except:
        return {'type': 'file', 'error': 'Cannot access'}

def should_include_content(file_path):
    """Determine if file content should be included"""
    return (file_path.suffix.lower() in INCLUDE_CONTENT_EXTENSIONS and 
            file_path.stat().st_size < 50000)  # Max 50KB files

def scan_directory(base_path, max_depth=3, current_depth=0):
    """Recursively scan directory structure"""
    if current_depth > max_depth:
        return {}
    
    structure = {}
    
    try:
        for item in sorted(base_path.iterdir()):
            if should_exclude(str(item)):
                continue
                
            if item.is_dir():
                structure[item.name] = {
                    'type': 'directory',
                    'contents': scan_directory(item, max_depth, current_depth + 1)
                }
            else:
                file_info = get_file_info(item)
                
                # Include content for small, relevant files
                if should_include_content(item):
                    try:
                        with open(item, 'r', encoding='utf-8') as f:
                            file_info['content'] = f.read()
                    except:
                        file_info['content_error'] = 'Cannot read file'
                
                structure[item.name] = file_info
                
    except PermissionError:
        structure['_error'] = 'Permission denied'
    
    return structure

def generate_tree_view(structure, prefix="", is_last=True):
    """Generate tree-like text representation"""
    lines = []
    items = list(structure.items())
    
    for i, (name, info) in enumerate(items):
        is_last_item = (i == len(items) - 1)
        
        # Tree formatting
        current_prefix = "└── " if is_last_item else "├── "
        lines.append(f"{prefix}{current_prefix}{name}")
        
        # Recurse for directories
        if info.get('type') == 'directory' and 'contents' in info:
            next_prefix = prefix + ("    " if is_last_item else "│   ")
            lines.extend(generate_tree_view(info['contents'], next_prefix, is_last_item))
    
    return lines

def main():
    """Generate project structure documentation"""
    
    # Base paths to scan (from ETL container perspective)
    scan_paths = [
        Path('/app/statcan'),
        Path('/app/db/statcan_schema') if Path('/app/db/statcan_schema').exists() else None
    ]
    
    # Remove None values
    scan_paths = [p for p in scan_paths if p and p.exists()]
    
    documentation = {
        'generated_at': datetime.now().isoformat(),
        'generator': 'ETL Container Structure Scanner',
        'note': 'Sensitive files and large data directories excluded',
        'structures': {}
    }
    
    for base_path in scan_paths:
        print(f"Scanning {base_path}...")
        structure = scan_directory(base_path)
        documentation['structures'][str(base_path)] = structure
    
    # Output both JSON and tree formats
    output_dir = Path('.')  # Current directory (documentation folder)
    
    # JSON format for programmatic use
    json_file = output_dir / 'project_structure.json'
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(documentation, f, indent=2, ensure_ascii=False)
    
    # Tree format for human reading
    tree_file = output_dir / 'project_structure.txt'
    with open(tree_file, 'w', encoding='utf-8') as f:
        f.write(f"Project Structure - Generated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 60 + "\n\n")
        
        for path_str, structure in documentation['structures'].items():
            f.write(f"{path_str}/\n")
            tree_lines = generate_tree_view(structure)
            f.write('\n'.join(tree_lines))
            f.write('\n\n')
    
    print(f"Documentation generated:")
    print(f"  JSON: {json_file}")
    print(f"  Tree: {tree_file}")
    
    # Show summary
    total_files = sum(len([k for k, v in struct.items() if v.get('type') == 'file']) 
                     for struct in documentation['structures'].values())
    total_dirs = sum(len([k for k, v in struct.items() if v.get('type') == 'directory']) 
                    for struct in documentation['structures'].values())
    
    print(f"\nScanned: {total_files} files, {total_dirs} directories")

if __name__ == "__main__":
    main()

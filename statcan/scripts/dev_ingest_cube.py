"""
Cube CSV Ingestion Script
Ingests StatCan cube data from downloaded ZIP files into cube-specific tables.

Process:
1. Check processing.cube_ingestion_status for pending cubes
2. Extract and parse CSV from ZIP files
3. Create dynamic table schema based on cube dimensions
4. Parse REF_DATE to standardized date format
5. Transform VALUES using scalar factors and decimals
6. Insert data and update ingestion status
"""

import os
import re
import zipfile
import pandas as pd
import psycopg2
from pathlib import Path
from datetime import datetime, date
from loguru import logger
from statcan.tools.config import DB_CONFIG

logger.add("/app/logs/cube_csv_ingest.log", rotation="10 MB", retention="7 days")

# Scalar factor lookup based on StatCan documentation
SCALAR_FACTORS = {
    0: 1,           # units
    1: 10,          # tens  
    2: 100,         # hundreds
    3: 1000,        # thousands
    4: 10000,       # tens of thousands
    5: 100000,      # hundreds of thousands
    6: 1000000,     # millions
    7: 10000000,    # tens of millions
    8: 100000000,   # hundreds of millions
    9: 1000000000   # billions
}

def parse_ref_date(ref_date_str):
    """
    Parse REF_DATE string to standardized date format.
    Returns tuple: (date_obj, original_str, interval_type)
    """
    ref_date_str = str(ref_date_str).strip()
    
    # Annual: YYYY
    if re.match(r'^\d{4}$', ref_date_str):
        year = int(ref_date_str)
        return date(year, 1, 1), ref_date_str, 'annual'
    
    # Monthly: YYYY-MM
    if re.match(r'^\d{4}-\d{2}$', ref_date_str):
        year, month = map(int, ref_date_str.split('-'))
        return date(year, month, 1), ref_date_str, 'monthly'
    
    # Quarterly: YYYY-Q[1-4]
    if re.match(r'^\d{4}-Q[1-4]$', ref_date_str):
        year = int(ref_date_str[:4])
        quarter = int(ref_date_str[-1])
        month = (quarter - 1) * 3 + 1
        return date(year, month, 1), ref_date_str, 'quarterly'
    
    # Weekly: YYYY-W[01-53] (approximate - use first day of year + weeks)
    if re.match(r'^\d{4}-W\d{2}$', ref_date_str):
        year = int(ref_date_str[:4])
        week = int(ref_date_str[6:])
        # Approximate: year start + (week-1)*7 days
        from datetime import timedelta
        start_date = date(year, 1, 1)
        week_date = start_date + timedelta(weeks=week-1)
        return week_date, ref_date_str, 'weekly'
    
    # Fiscal year variations: YYYY/YYYY+1 or similar
    if re.match(r'^\d{4}/\d{4}$', ref_date_str):
        year = int(ref_date_str[:4])
        return date(year, 4, 1), ref_date_str, 'fiscal_annual'  # Canadian fiscal starts April
    
    # Default: try to parse as-is, fall back to string
    try:
        parsed_date = pd.to_datetime(ref_date_str).date()
        return parsed_date, ref_date_str, 'other'
    except:
        logger.warning(f"Could not parse REF_DATE: {ref_date_str}")
        return None, ref_date_str, 'unparsed'

def get_cube_dimensions(productid):
    """Get dimension mapping for a cube from cube_dimension_map."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        query = """
            SELECT dimension_position, dimension_name_slug, dimension_hash
            FROM cube.cube_dimension_map 
            WHERE productid = %s 
            ORDER BY dimension_position
        """
        df = pd.read_sql(query, conn, params=[productid])
        return df.to_dict('records')

def create_cube_table(productid, dimensions):
    """Create dynamic cube table based on dimensions."""
    table_name = f"c{productid}"
    
    # Build column definitions
    columns = [
        "ref_date DATE",
        "ref_date_original TEXT",
        "ref_date_interval_type TEXT"
    ]
    
    # Add dimension columns
    for dim in dimensions:
        col_name = f"{dim['dimension_name_slug']}_member_id"
        columns.append(f"{col_name} INTEGER")
    
    columns.append("value NUMERIC")
    
    # Create table with proper indexes
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS cube_data.{table_name} (
            {', '.join(columns)},
            PRIMARY KEY (ref_date, {', '.join(f"{d['dimension_name_slug']}_member_id" for d in dimensions)})
        );
        
        CREATE INDEX IF NOT EXISTS {table_name}_ref_date_idx 
        ON cube_data.{table_name} (ref_date);
        
        CREATE INDEX IF NOT EXISTS {table_name}_value_idx 
        ON cube_data.{table_name} (value) WHERE value IS NOT NULL;
    """
    
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(create_sql)
            conn.commit()
    
    logger.info(f"✅ Created/verified table cube_data.{table_name}")
    return table_name

def extract_csv_from_zip(zip_path, productid):
    """Extract the data CSV from zip file (ignore metadata CSV)."""
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        files = zip_ref.namelist()
        
        # Look for the main data CSV (not metadata)
        data_csv = None
        for file in files:
            if file.endswith('.csv') and 'metadata' not in file.lower():
                data_csv = file
                break
        
        if not data_csv:
            raise ValueError(f"No data CSV found in {zip_path}")
        
        # Extract to temporary location
        temp_path = f"/tmp/{productid}_data.csv"
        with zip_ref.open(data_csv) as source, open(temp_path, 'wb') as target:
            target.write(source.read())
        
        return temp_path

def transform_value(raw_value, scalar_factor_code, decimals):
    """Apply scalar and decimal transformations to raw value."""
    if pd.isna(raw_value) or raw_value == '':
        return None
    
    try:
        raw_val = float(raw_value)
        scalar_multiplier = SCALAR_FACTORS.get(scalar_factor_code, 1)
        decimal_divisor = 10 ** decimals
        
        actual_value = raw_val * scalar_multiplier / decimal_divisor
        return actual_value
    except (ValueError, TypeError):
        logger.warning(f"Could not transform value: {raw_value}")
        return None

def parse_coordinate(coordinate_str, dimensions):
    """Parse coordinate string into dimension member IDs."""
    if pd.isna(coordinate_str):
        return {}
    
    member_ids = coordinate_str.split('.')
    result = {}
    
    for i, dim in enumerate(dimensions):
        if i < len(member_ids):
            try:
                member_id = int(member_ids[i])
                col_name = f"{dim['dimension_name_slug']}_member_id"
                result[col_name] = member_id
            except (ValueError, IndexError):
                logger.warning(f"Invalid member ID at position {i}: {member_ids}")
                result[f"{dim['dimension_name_slug']}_member_id"] = None
        else:
            result[f"{dim['dimension_name_slug']}_member_id"] = None
    
    return result

def get_pending_cubes(limit=None):
    """Get cubes pending ingestion."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Get cubes where file has been downloaded but not ingested
            cur.execute("""
                SELECT cs.productid, mcrf.storage_location, mcrf.file_hash
                FROM raw_files.cube_status cs
                JOIN raw_files.manage_cube_raw_files mcrf 
                    ON cs.productid = mcrf.productid AND mcrf.active = TRUE
                LEFT JOIN processing.cube_ingestion_status cis 
                    ON cs.productid = cis.productid
                WHERE cs.download_pending = FALSE 
                    AND cs.last_file_hash IS NOT NULL
                    AND (cis.ingestion_pending = TRUE OR cis.productid IS NULL)
                ORDER BY cs.productid
                LIMIT %s
            """, (limit,))
            return cur.fetchall()

def update_ingestion_status(productid, file_hash, table_created=True, row_count=0, success=True, notes=None):
    """Update processing.cube_ingestion_status."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO processing.cube_ingestion_status 
                (productid, last_ingestion, ingestion_pending, last_file_hash, 
                 table_created, row_count, notes)
                VALUES (%s, NOW(), %s, %s, %s, %s, %s)
                ON CONFLICT (productid) DO UPDATE SET
                    last_ingestion = NOW(),
                    ingestion_pending = %s,
                    last_file_hash = %s,
                    table_created = %s,
                    row_count = %s,
                    notes = %s
            """, (productid, not success, file_hash, table_created, row_count, notes,
                  not success, file_hash, table_created, row_count, notes))
            conn.commit()

def ingest_cube_data(productid, zip_path, file_hash):
    """Main ingestion logic for a single cube."""
    logger.info(f"🔄 Starting ingestion for cube {productid}")
    
    try:
        # Get cube dimensions
        dimensions = get_cube_dimensions(productid)
        if not dimensions:
            raise ValueError(f"No dimensions found for productid {productid}")
        
        # Create cube table
        table_name = create_cube_table(productid, dimensions)
        
        # Extract CSV from ZIP
        csv_path = extract_csv_from_zip(zip_path, productid)
        
        try:
            # Read CSV data
            logger.info(f"📖 Reading CSV data for {productid}")
            df = pd.read_csv(csv_path)
            
            # Verify required columns exist
            required_cols = ['REF_DATE', 'COORDINATE', 'VALUE', 'SCALAR_FACTOR', 'DECIMALS']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            # Transform data
            logger.info(f"🔄 Transforming {len(df)} rows for {productid}")
            
            # Parse REF_DATE
            df[['ref_date', 'ref_date_original', 'ref_date_interval_type']] = df['REF_DATE'].apply(
                lambda x: pd.Series(parse_ref_date(x))
            )
            
            # Parse coordinates into dimension columns
            coordinate_data = df.apply(
                lambda row: parse_coordinate(row['COORDINATE'], dimensions), axis=1
            )
            coordinate_df = pd.DataFrame(coordinate_data.tolist())
            df = pd.concat([df, coordinate_df], axis=1)
            
            # Transform values
            df['value'] = df.apply(
                lambda row: transform_value(row['VALUE'], row.get('SCALAR_FACTOR', 0), row.get('DECIMALS', 0)),
                axis=1
            )
            
            # Select final columns for insertion
            final_columns = ['ref_date', 'ref_date_original', 'ref_date_interval_type'] + \
                           [f"{dim['dimension_name_slug']}_member_id" for dim in dimensions] + \
                           ['value']
            
            df_final = df[final_columns].copy()
            
            # Remove rows with null dates or all null dimension values
            df_final = df_final.dropna(subset=['ref_date'])
            
            # Insert data
            logger.info(f"💾 Inserting {len(df_final)} rows into {table_name}")
            
            with psycopg2.connect(**DB_CONFIG) as conn:
                # Truncate existing data for this cube
                with conn.cursor() as cur:
                    cur.execute(f"TRUNCATE TABLE cube_data.{table_name}")
                
                # Insert new data
                df_final.to_sql(
                    table_name, 
                    conn, 
                    schema='cube_data',
                    if_exists='append', 
                    index=False,
                    method='multi'
                )
                conn.commit()
            
            row_count = len(df_final)
            logger.success(f"✅ Ingested {row_count} rows for cube {productid}")
            
            # Update status
            update_ingestion_status(productid, file_hash, True, row_count, True)
            
        finally:
            # Cleanup temp file
            if os.path.exists(csv_path):
                os.remove(csv_path)
    
    except Exception as e:
        logger.error(f"❌ Failed to ingest cube {productid}: {e}")
        update_ingestion_status(productid, file_hash, False, 0, False, str(e))
        raise

def main():
    logger.info("🚀 Starting cube CSV ingestion...")
    
    try:
        # Get pending cubes
        pending_cubes = get_pending_cubes(limit=5)  # Start with small batch
        
        if not pending_cubes:
            logger.info("🎉 No cubes pending ingestion")
            return
        
        logger.info(f"📋 Found {len(pending_cubes)} cubes to ingest")
        
        success_count = 0
        for productid, zip_path, file_hash in pending_cubes:
            try:
                ingest_cube_data(productid, zip_path, file_hash)
                success_count += 1
            except Exception as e:
                logger.error(f"💥 Cube {productid} ingestion failed: {e}")
                continue
        
        logger.success(f"✅ Ingestion complete: {success_count}/{len(pending_cubes)} successful")
        
    except Exception as e:
        logger.exception(f"💥 Ingestion pipeline failed: {e}")

if __name__ == "__main__":
    main()

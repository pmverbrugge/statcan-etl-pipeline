4 June 2025


This project implements a modular ETL (Extract, Transform, Load) pipeline to download, track, and normalize publicly available data cubes from Statistics Canada. Cube data and metadata are retrieved via the Web Data Service (WDS) API, stored in a structured raw file system, and ingested into a PostgreSQL-based data warehouse. A dimension registry deduplicates and harmonizes dimension definitions and member labels across products, enabling cross-cube integration and long-term schema consistency. The system is designed for reproducibility, traceability, and scalability, with future extensions planned for harmonized time series, metadata enrichment, and client-facing analytics tools.


fetch_spine.py — Retrieve and Archive Master Cube List
This script downloads the full list of available data cubes from Statistics Canada's getAllCubesListLite endpoint and stores it as a versioned JSON file in the metadata archive. It calculates a hash of the file contents to detect duplicates and prevent redundant storage. If the hash is new, the script saves the file to disk and logs its metadata into the raw_files.manage_spine_raw_files table, deactivating any previous entries. This ensures a traceable, deduplicated record of spine snapshots for downstream processing.


spine_etl.py — Load Spine Metadata into Warehouse
This ETL script ingests the most recent Statistics Canada spine metadata file into the warehouse. It retrieves the active metadata file path from the manage_spine_raw_files table, reads and stages the JSON content using DuckDB, and normalizes it into structured views (cube, cube_subject, cube_survey). These views are then loaded into the corresponding PostgreSQL spine schema tables after truncation. The script ensures consistent data structure and supports refreshes by replacing old records with the latest snapshot.


fetch_cubes.py — Download and Log StatCan Cube Files
This script automates downloading StatCan data cubes using the getFullTableDownloadCSV endpoint. It checks the raw_files.cube_status table for cubes flagged download_pending, fetches each cube as a .zip, computes a hash for deduplication, and stores it under /app/raw/cubes/. Metadata (hash, path, timestamp) is recorded in raw_files.manage_cube_raw_files, while cube_status is updated to mark completion. Existing active files are deactivated if superseded. Duplicate files are skipped. A 2-second delay is added between downloads to avoid hammering the server.


populate_cube_status.py — Initialize cube_status for New Cubes
This script populates the raw_files.cube_status table with entries for all productids in the spine.cube table that are not already present. Each new entry is initialized with download_pending = TRUE to flag them for initial download. This ensures that newly added cubes are picked up by the cube fetcher script. Existing entries are left unchanged.


update_cube_status.py — Detect Cube Updates and Flag Downloads
This script checks for updates to StatCan data cubes by querying the getChangedCubeList(date) endpoint for each date since the last recorded download. Detected changes are logged in the raw_files.changed_cubes_log table without duplication. If a cube's change_date is more recent than its last_download, it is marked download_pending = TRUE in the raw_files.cube_status table. The script includes logic to account for StatCan’s 08:30 EST release time and introduces a delay between API calls to reduce server load.


verify_cube_files.py — Validate Raw Cube Files
This script checks that every active cube file listed in raw_files.manage_cube_raw_files: 1) Exists at the specified storage_location; 2) Matches the expected SHA-256 hash.
If a file is missing or fails the hash check: It is deleted (if corrupt); Its database record is removed; The corresponding download_pending flag in cube_status is reset to TRUE, triggering a re-download. This ensures integrity of the cube archive and guards against partial or corrupted downloads.


fetch_metadata.py — Download Metadata for StatCan Cubes
This script fetches bilingual metadata for each StatCan cube using the getCubeMetadata endpoint. It checks raw_files.metadata_status for cubes marked download_pending, downloads their metadata in JSON format, stores the file with a content-based hash, and updates tracking tables: Logs each file in raw_files.manage_metadata_raw_files, Updates last_file_hash and last_download in metadata_status, Duplicate downloads are skipped. Files are saved to /app/raw/metadata. A 1-second delay is used between requests to avoid overloading the API.


populate_metadata_status.py — Initialize Metadata Download Status
This script populates the raw_files.metadata_status table by inserting all productids from spine.cube that don’t already exist in metadata_status. Each new entry is marked with download_pending = TRUE, flagging it for metadata download. This ensures all cubes in the spine are tracked for metadata ingestion. Useful for bootstrapping or syncing the metadata status table after new cube entries are added.


load_raw_dimensions.py — Ingest Dimension Metadata
This script parses and ingests dimension definitions and member details from previously downloaded metadata files stored on disk. Loads productid and associated last_file_hash from raw_files.metadata_status. For each metadata JSON file: Inserts dimension definitions into dictionary.raw_dimension; Inserts dimension members into dictionary.raw_member; Uses ON CONFLICT DO NOTHING to avoid duplicate insertions; Supports partial failure by continuing to next file on error. This is the main pipeline step for getting raw dimension metadata into the warehouse for later normalization.


dimension_member_attribute_lists.py — Inspect Metadata Key Usage
This diagnostic script scans all downloaded StatCan cube metadata files and tallies the frequency of keys used within:
Dimension definitions; Member entries. The script: Iterates over JSON files in the metadata directory; Extracts and counts the keys present in dimension and member dictionaries; Outputs a ranked list of observed keys and their frequencies.
Useful for: Schema discovery; Debugging inconsistencies in raw metadata; Informing the design of database tables and normalization logic


build_dimension_registry.py — Construct Harmonized Dimension Registry
This script deduplicates and normalizes raw StatCan dimension metadata into a harmonized registry across cubes.
Key steps:
1. Hashes code-label combinations (member_id, label, parent ID, UOM) to form member_hash.
2. Aggregates member hashes into a dimension_hash per dimension position and product.
3. Selects the most common English label per code to assign canonical names.
4. Computes metadata flags: is_total (label contains “total”), is_grabbag (name includes “characteristics” or “other”), is_tree (any parent-child hierarchy),
is_exclusive (placeholder).
5 Inserts cleaned data into: dictionary.dimension_set, dictionary.dimension_set_member, cube.cube_dimension_map. 
The result is a normalized set of reusable dimension definitions suitable for harmonizing cubes and enabling cross-cube analytics.


member_base_name_normalizer.py — Label Normalization for Deduplication
This script computes a normalized "base name" for each dimension member in the dictionary.dimension_set_member table to support deduplication and harmonization.
Key steps: Tokenizes English labels using NLTK’s tokenizer; Filters out stopwords and non-alphabetic tokens; Normalizes remaining words to lowercase and forms a deterministic, sorted token string as the base_name; Updates the database with the computed base_name for each (dimension_hash, member_id) pair.
Purpose: Enables comparison and grouping of semantically similar member labels across dimensions and cubes.
A foundation for harmonization and cross-cube integration.



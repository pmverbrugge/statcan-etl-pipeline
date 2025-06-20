pg_dump: last built-in OID is 16383
pg_dump: reading extensions
pg_dump: identifying extension members
pg_dump: reading schemas
pg_dump: reading user-defined tables
pg_dump: reading user-defined functions
pg_dump: reading user-defined types
pg_dump: reading procedural languages
pg_dump: reading user-defined aggregate functions
pg_dump: reading user-defined operators
pg_dump: reading user-defined access methods
pg_dump: reading user-defined operator classes
pg_dump: reading user-defined operator families
pg_dump: reading user-defined text search parsers
pg_dump: reading user-defined text search templates
pg_dump: reading user-defined text search dictionaries
pg_dump: reading user-defined text search configurations
pg_dump: reading user-defined foreign-data wrappers
pg_dump: reading user-defined foreign servers
pg_dump: reading default privileges
pg_dump: reading user-defined collations
pg_dump: reading user-defined conversions
pg_dump: reading type casts
pg_dump: reading transforms
pg_dump: reading table inheritance information
pg_dump: reading event triggers
pg_dump: finding extension tables
pg_dump: finding inheritance relationships
pg_dump: reading column info for interesting tables
pg_dump: finding table default expressions
pg_dump: finding table check constraints
pg_dump: flagging inherited columns in subtables
pg_dump: reading partitioning data
pg_dump: reading indexes
pg_dump: flagging indexes in partitioned tables
pg_dump: reading extended statistics
pg_dump: reading constraints
pg_dump: reading triggers
pg_dump: reading rewrite rules
pg_dump: reading policies
pg_dump: reading row-level security policies
pg_dump: reading publications
pg_dump: reading publication membership of tables
pg_dump: reading publication membership of schemas
pg_dump: reading subscriptions
pg_dump: reading dependency data
pg_dump: saving encoding = UTF8
pg_dump: saving standard_conforming_strings = on
pg_dump: saving search_path = 
pg_dump: saving database definition
pg_dump: dropping DATABASE statcan
pg_dump: creating DATABASE "statcan"
pg_dump: connecting to new database "statcan"
pg_dump: creating SCHEMA "cube"
pg_dump: creating SCHEMA "cube_data"
pg_dump: creating SCHEMA "dictionary"
pg_dump: creating SCHEMA "processing"
pg_dump: creating SCHEMA "raw_files"
pg_dump: creating SCHEMA "spine"
pg_dump: creating EXTENSION "pgcrypto"
pg_dump: creating COMMENT "EXTENSION pgcrypto"
pg_dump: creating EXTENSION "postgis"
pg_dump: creating COMMENT "EXTENSION postgis"
pg_dump: creating EXTENSION "uuid-ossp"
pg_dump: creating COMMENT "EXTENSION "uuid-ossp""
pg_dump: creating TABLE "cube.test_table"
pg_dump: creating TABLE "cube_data.13100653"
pg_dump: creating TABLE "cube_data.c10100001"
pg_dump: creating TABLE "cube_data.c10100002"
pg_dump: creating TABLE "cube_data.c10100003"
pg_dump: creating TABLE "cube_data.c10100004"
pg_dump: creating TABLE "cube_data.c10100005"
pg_dump: creating TABLE "dictionary.dimension_set"
pg_dump: creating COMMENT "dictionary.TABLE dimension_set"
pg_dump: creating COMMENT "dictionary.COLUMN dimension_set.dimension_hash"
pg_dump: creating COMMENT "dictionary.COLUMN dimension_set.is_tree"
pg_dump: creating TABLE "dictionary.dimension_set_member"
pg_dump: creating TABLE "processing.dimension_set"
pg_dump: creating COMMENT "processing.TABLE dimension_set"
pg_dump: creating COMMENT "processing.COLUMN dimension_set.dimension_hash"
pg_dump: creating COMMENT "processing.COLUMN dimension_set.dimension_name_en"
pg_dump: creating COMMENT "processing.COLUMN dimension_set.dimension_name_fr"
pg_dump: creating COMMENT "processing.COLUMN dimension_set.has_uom"
pg_dump: creating COMMENT "processing.COLUMN dimension_set.usage_count"
pg_dump: creating TABLE "processing.dimension_set_members"
pg_dump: creating COMMENT "processing.TABLE dimension_set_members"
pg_dump: creating COMMENT "processing.COLUMN dimension_set_members.dimension_hash"
pg_dump: creating COMMENT "processing.COLUMN dimension_set_members.member_name_en"
pg_dump: creating COMMENT "processing.COLUMN dimension_set_members.member_name_fr"
pg_dump: creating COMMENT "processing.COLUMN dimension_set_members.usage_count"
pg_dump: creating TABLE "processing.processed_dimensions"
pg_dump: creating COMMENT "processing.TABLE processed_dimensions"
--
-- PostgreSQL database dump
--

-- Dumped from database version 15.8 (Debian 15.8-1.pgdg110+1)
-- Dumped by pg_dump version 15.8 (Debian 15.8-1.pgdg110+1)

-- Started on 2025-06-20 13:12:01 UTC

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

DROP DATABASE IF EXISTS statcan;
--
-- TOC entry 4491 (class 1262 OID 16384)
-- Name: statcan; Type: DATABASE; Schema: -; Owner: -
--

CREATE DATABASE statcan WITH TEMPLATE = template0 ENCODING = 'UTF8' LOCALE_PROVIDER = libc LOCALE = 'en_US.utf8';


\connect statcan

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 10 (class 2615 OID 17533)
-- Name: cube; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA cube;


--
-- TOC entry 14 (class 2615 OID 26440)
-- Name: cube_data; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA cube_data;


--
-- TOC entry 12 (class 2615 OID 17536)
-- Name: dictionary; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA dictionary;


--
-- TOC entry 9 (class 2615 OID 17534)
-- Name: processing; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA processing;


--
-- TOC entry 13 (class 2615 OID 17532)
-- Name: raw_files; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA raw_files;


--
-- TOC entry 11 (class 2615 OID 17535)
-- Name: spine; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA spine;


--
-- TOC entry 4 (class 3079 OID 17474)
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


--
-- TOC entry 4492 (class 0 OID 0)
-- Dependencies: 4
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


--
-- TOC entry 2 (class 3079 OID 16385)
-- Name: postgis; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;


--
-- TOC entry 4493 (class 0 OID 0)
-- Dependencies: 2
-- Name: EXTENSION postgis; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION postgis IS 'PostGIS geometry and geography spatial types and functions';


--
-- TOC entry 3 (class 3079 OID 17463)
-- Name: uuid-ossp; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;


--
-- TOC entry 4494 (class 0 OID 0)
-- Dependencies: 3
-- Name: EXTENSION "uuid-ossp"; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION "uuid-ossp" IS 'generate universally unique identifiers (UUIDs)';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 242 (class 1259 OID 26437)
-- Name: test_table; Type: TABLE; Schema: cube; Owner: -
--

CREATE TABLE cube.test_table (
    id integer
);


--
-- TOC entry 243 (class 1259 OID 26441)
-- Name: 13100653; Type: TABLE; Schema: cube_data; Owner: -
--

CREATE TABLE cube_data."13100653" (
    "REF_DATE" bigint,
    "GEO" text,
    "DGUID" text,
    "Age group" text,
    "Sex" text,
    "Type of smoker" text,
    "Characteristics" text,
    "UOM" text,
    "UOM_ID" bigint,
    "SCALAR_FACTOR" text,
    "SCALAR_ID" bigint,
    "VECTOR" text,
    "COORDINATE" text,
    "VALUE" double precision,
    "STATUS" text,
    "SYMBOL" double precision,
    "TERMINATED" double precision,
    "DECIMALS" bigint
);


--
-- TOC entry 244 (class 1259 OID 26668)
-- Name: c10100001; Type: TABLE; Schema: cube_data; Owner: -
--

CREATE TABLE cube_data.c10100001 (
    ref_date date NOT NULL,
    ref_date_original text,
    ref_date_interval_type text,
    geography_member_id integer NOT NULL,
    federal_public_sector_employment_member_id integer NOT NULL,
    value numeric
);


--
-- TOC entry 245 (class 1259 OID 26677)
-- Name: c10100002; Type: TABLE; Schema: cube_data; Owner: -
--

CREATE TABLE cube_data.c10100002 (
    ref_date date NOT NULL,
    ref_date_original text,
    ref_date_interval_type text,
    geography_member_id integer NOT NULL,
    central_government_debt_member_id integer NOT NULL,
    value numeric
);


--
-- TOC entry 246 (class 1259 OID 26686)
-- Name: c10100003; Type: TABLE; Schema: cube_data; Owner: -
--

CREATE TABLE cube_data.c10100003 (
    ref_date date NOT NULL,
    ref_date_original text,
    ref_date_interval_type text,
    geography_member_id integer NOT NULL,
    type_of_issues_member_id integer NOT NULL,
    issuers_member_id integer NOT NULL,
    value numeric
);


--
-- TOC entry 247 (class 1259 OID 26701)
-- Name: c10100004; Type: TABLE; Schema: cube_data; Owner: -
--

CREATE TABLE cube_data.c10100004 (
    ref_date date NOT NULL,
    ref_date_original text,
    ref_date_interval_type text,
    geography_member_id integer NOT NULL,
    claims_and_deposits_member_id integer NOT NULL,
    type_of_non_resident_member_id integer NOT NULL,
    country_of_non_resident_member_id integer NOT NULL,
    value numeric
);


--
-- TOC entry 248 (class 1259 OID 26716)
-- Name: c10100005; Type: TABLE; Schema: cube_data; Owner: -
--

CREATE TABLE cube_data.c10100005 (
    ref_date date NOT NULL,
    ref_date_original text,
    ref_date_interval_type text,
    geography_member_id integer NOT NULL,
    public_sector_components_member_id integer NOT NULL,
    canadian_classification_of_functions_of_government_ccofog_membe integer NOT NULL,
    value numeric
);


--
-- TOC entry 240 (class 1259 OID 26178)
-- Name: dimension_set; Type: TABLE; Schema: dictionary; Owner: -
--

CREATE TABLE dictionary.dimension_set (
    dimension_hash text NOT NULL,
    dimension_name_en text,
    base_name text,
    is_statistics boolean,
    is_grabbag boolean,
    is_exclusive boolean,
    has_total boolean,
    is_tree boolean,
    dimension_name_slug text,
    dimension_name_fr text
);


--
-- TOC entry 4495 (class 0 OID 0)
-- Dependencies: 240
-- Name: TABLE dimension_set; Type: COMMENT; Schema: dictionary; Owner: -
--

COMMENT ON TABLE dictionary.dimension_set IS 'Canonical dimension registry with computed characteristics. Built from processing.dimension_set in script 14.';


--
-- TOC entry 4496 (class 0 OID 0)
-- Dependencies: 240
-- Name: COLUMN dimension_set.dimension_hash; Type: COMMENT; Schema: dictionary; Owner: -
--

COMMENT ON COLUMN dictionary.dimension_set.dimension_hash IS '12-character SHA-256 hash identifying this unique dimension structure';


--
-- TOC entry 4497 (class 0 OID 0)
-- Dependencies: 240
-- Name: COLUMN dimension_set.is_tree; Type: COMMENT; Schema: dictionary; Owner: -
--

COMMENT ON COLUMN dictionary.dimension_set.is_tree IS 'True if dimension has hierarchical structure (members with parent relationships)';


--
-- TOC entry 241 (class 1259 OID 26185)
-- Name: dimension_set_member; Type: TABLE; Schema: dictionary; Owner: -
--

CREATE TABLE dictionary.dimension_set_member (
    dimension_hash text NOT NULL,
    member_hash text NOT NULL,
    member_id integer,
    classification_code text,
    classification_type_code text,
    member_name_en text,
    member_name_fr text,
    member_uom_code text,
    parent_member_id integer,
    geo_level integer,
    vintage integer,
    terminated boolean,
    is_total boolean,
    base_name text,
    member_label_norm text
);


--
-- TOC entry 251 (class 1259 OID 27237)
-- Name: dimension_set; Type: TABLE; Schema: processing; Owner: -
--

CREATE TABLE processing.dimension_set (
    dimension_hash text NOT NULL,
    dimension_name_en text,
    dimension_name_fr text,
    dimension_name_en_slug text,
    dimension_name_fr_slug text,
    has_uom boolean DEFAULT false,
    usage_count integer DEFAULT 0,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- TOC entry 4498 (class 0 OID 0)
-- Dependencies: 251
-- Name: TABLE dimension_set; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON TABLE processing.dimension_set IS 'Canonical dimension definitions with most common labels and characteristics. Built from processed_dimensions in script 12.';


--
-- TOC entry 4499 (class 0 OID 0)
-- Dependencies: 251
-- Name: COLUMN dimension_set.dimension_hash; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.dimension_set.dimension_hash IS '12-character SHA-256 hash identifying this unique dimension structure';


--
-- TOC entry 4500 (class 0 OID 0)
-- Dependencies: 251
-- Name: COLUMN dimension_set.dimension_name_en; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.dimension_set.dimension_name_en IS 'Most common English dimension name in title case';


--
-- TOC entry 4501 (class 0 OID 0)
-- Dependencies: 251
-- Name: COLUMN dimension_set.dimension_name_fr; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.dimension_set.dimension_name_fr IS 'Most common French dimension name in title case';


--
-- TOC entry 4502 (class 0 OID 0)
-- Dependencies: 251
-- Name: COLUMN dimension_set.has_uom; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.dimension_set.has_uom IS 'True if any instance of this dimension contains unit of measure information';


--
-- TOC entry 4503 (class 0 OID 0)
-- Dependencies: 251
-- Name: COLUMN dimension_set.usage_count; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.dimension_set.usage_count IS 'Number of (productid, dimension_position) instances using this dimension_hash';


--
-- TOC entry 252 (class 1259 OID 27335)
-- Name: dimension_set_members; Type: TABLE; Schema: processing; Owner: -
--

CREATE TABLE processing.dimension_set_members (
    dimension_hash text NOT NULL,
    member_id integer NOT NULL,
    member_name_en text,
    member_name_fr text,
    parent_member_id integer,
    member_uom_code text,
    usage_count integer DEFAULT 0,
    created_at timestamp with time zone DEFAULT now()
);


--
-- TOC entry 4504 (class 0 OID 0)
-- Dependencies: 252
-- Name: TABLE dimension_set_members; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON TABLE processing.dimension_set_members IS 'Canonical member definitions within each dimension. Built from processed_members in script 13.';


--
-- TOC entry 4505 (class 0 OID 0)
-- Dependencies: 252
-- Name: COLUMN dimension_set_members.dimension_hash; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.dimension_set_members.dimension_hash IS 'Reference to canonical dimension hash';


--
-- TOC entry 4506 (class 0 OID 0)
-- Dependencies: 252
-- Name: COLUMN dimension_set_members.member_name_en; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.dimension_set_members.member_name_en IS 'Most common English member name across all cubes using this dimension';


--
-- TOC entry 4507 (class 0 OID 0)
-- Dependencies: 252
-- Name: COLUMN dimension_set_members.member_name_fr; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.dimension_set_members.member_name_fr IS 'Most common French member name across all cubes using this dimension';


--
-- TOC entry 4508 (class 0 OID 0)
-- Dependencies: 252
-- Name: COLUMN dimension_set_members.usage_count; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.dimension_set_members.usage_count IS 'Number of cube instances where this member appears in this dimension';


--
-- TOC entry 250 (class 1259 OID 27224)
-- Name: processed_dimensions; Type: TABLE; Schema: processing; Owner: -
--

CREATE TABLE processing.processed_dimensions (
    productid bigint NOT NULL,
    dimension_position integer NOT NULL,
    dimension_hash text NOT NULL,
    dimension_name_en text,
    dimension_name_fr text,
    has_uom boolean,
    created_at timestamp with time zone DEFAULT now()
);


pg_dump: creating COMMENT "processing.COLUMN processed_dimensions.dimension_hash"
pg_dump: creating COMMENT "processing.COLUMN processed_dimensions.has_uom"
pg_dump: creating TABLE "processing.processed_members"
pg_dump: --
-- TOC entry 4509 (class 0 OID 0)
-- Dependencies: 250
-- Name: TABLE processed_dimensions; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON TABLE processing.processed_dimensions IS 'Mapping of (productid, dimension_position) to dimension_hash with raw dimension metadata. Built from processed_members + raw_dimension in script 11.';


--
-- TOC entry 4510 (class 0 OID 0)
-- Dependencies: 250
-- Name: COLUMN processed_dimensions.dimension_hash; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.processed_dimensions.dimension_hash IS '12-character SHA-256 hash of concatenated member hashes (sorted by member_id) within this dimension';


--
-- TOC entry 4511 (class 0 OID 0)
-- Dependencies: 250
-- Name: COLUMN processed_dimensions.has_uom; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.processed_dimensions.has_uom IS 'Indicates if this dimension contains unit of measure information';


creating COMMENT "processing.TABLE processed_members"
pg_dump: creating COMMENT "processing.COLUMN processed_members.member_hash"
pg_dump: creating COMMENT "processing.COLUMN processed_members.member_label_norm"
--
-- TOC entry 249 (class 1259 OID 27132)
-- Name: processed_members; Type: TABLE; Schema: processing; Owner: -
--

CREATE TABLE processing.processed_members (
    productid bigint NOT NULL,
    dimension_position integer NOT NULL,
    member_id integer NOT NULL,
    member_hash text NOT NULL,
    member_name_en text,
    member_name_fr text,
    parent_member_id integer,
    member_uom_code text,
    classification_code text,
    classification_type_code text,
    geo_level integer,
    vintage integer,
    terminated boolean,
    member_label_norm text,
    created_at timestamp with time zone DEFAULT now(),
    dimension_hash text
);


--
-- TOC entry 4512 (class 0 OID 0)
-- Dependencies: 249
-- Name: TABLE processed_members; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON TABLE processing.processed_members IS 'Raw member data with computed member-level hashes. Input for dimension registry building (script 11+).';


--
-- TOC entry 4513 (class 0 OID 0)
-- Dependencies: 249
-- Name: COLUMN processed_members.member_hash; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.processed_members.member_hash IS '12-character SHA-256 hash of member_id + normalized_label_en + parent_id + uom_code';


--
-- TOC entry 4514 (class 0 OID 0)
-- Dependencies: 249
-- Name: COLUMN processed_members.member_label_norm; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.processed_members.member_label_norm IS 'Normalized (lowercase, trimmed) version of member_name_en for hashing consistency';


pg_dump: creating COMMENT "processing.COLUMN processed_members.dimension_hash"
pg_dump: creating TABLE "processing.raw_dimension"
--
-- TOC entry 4515 (class 0 OID 0)
-- Dependencies: 249
-- Name: COLUMN processed_members.dimension_hash; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.processed_members.dimension_hash IS '12-character dimension hash populated by script 13 from processed_dimensions';


--
-- TOC entry 253 (class 1259 OID 27365)
-- Name: raw_dimension; Type: TABLE; Schema: processing; Owner: -
--

CREATE TABLE processing.raw_dimension (
    productid bigint NOT NULL,
    dimension_position integer NOT NULL,
    dimension_name_en text,
    dimension_name_fr text,
    has_uom boolean,
    created_at timestamp with time zone DEFAULT now()
);


--
-- TOC entry 4516 (class 0 OID 0)
-- Dependencies: 253
-- Name: TABLE raw_dimension; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON TABLE processing.raw_dimension IS 'Raw dimension metadata from Statistics Canada API - processing schema';


--
-- TOC entry 4517 (class 0 OID 0)
-- Dependencies: 253
-- Name: COLUMN raw_dimension.created_at; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.raw_dimension.created_at IS 'Timestamp when record was created';


pg_dump: creating COMMENT "processing.TABLE raw_dimension"
pg_dump: creating COMMENT "processing.COLUMN raw_dimension.created_at"
pg_dump: creating TABLE "processing.raw_member"
--
-- TOC entry 254 (class 1259 OID 27373)
-- Name: raw_member; Type: TABLE; Schema: processing; Owner: -
--

CREATE TABLE processing.raw_member (
    productid bigint NOT NULL,
    dimension_position integer NOT NULL,
    member_id integer NOT NULL,
    parent_member_id integer,
    classification_code text,
    classification_type_code text,
    member_name_en text,
    member_name_fr text,
    member_uom_code text,
    geo_level integer,
    vintage integer,
    terminated integer,
    created_at timestamp with time zone DEFAULT now()
);


pg_dump: creating COMMENT "processing.TABLE raw_member"
pg_dump: creating COMMENT "processing.COLUMN raw_member.created_at"
--
-- TOC entry 4518 (class 0 OID 0)
-- Dependencies: 254
-- Name: TABLE raw_member; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON TABLE processing.raw_member IS 'Raw member metadata from Statistics Canada API - processing schema';


--
-- TOC entry 4519 (class 0 OID 0)
-- Dependencies: 254
-- Name: COLUMN raw_member.created_at; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.raw_member.created_at IS 'Timestamp when record was created';


pg_dump: creating TABLE "raw_files.changed_cubes_log"
--
-- TOC entry 236 (class 1259 OID 25947)
-- Name: changed_cubes_log; Type: TABLE; Schema: raw_files; Owner: -
--

CREATE TABLE raw_files.changed_cubes_log (
    productid bigint NOT NULL,
    change_date date NOT NULL
);


pg_dump: creating TABLE "raw_files.cube_status"
--
-- TOC entry 235 (class 1259 OID 25864)
-- Name: cube_status; Type: TABLE; Schema: raw_files; Owner: -
--

CREATE TABLE raw_files.cube_status (
    productid bigint NOT NULL,
    last_download timestamp with time zone,
    download_pending boolean DEFAULT false NOT NULL,
    last_file_hash text
);


--
-- TOC entry 232 (class 1259 OID 25732)
-- Name: manage_cube_raw_files; Type: TABLE; Schema: raw_files; Owner: -
--

CREATE TABLE raw_files.manage_cube_raw_files (
    id integer NOT NULL,
    productid bigint NOT NULL,
    file_hash text NOT NULL,
    date_download timestamp with time zone DEFAULT now() NOT NULL,
    active boolean DEFAULT false NOT NULL,
    storage_location text NOT NULL
);


pg_dump: creating TABLE "raw_files.manage_cube_raw_files"
pg_dump: creating SEQUENCE "raw_files.manage_cube_raw_files_id_seq"
--
-- TOC entry 231 (class 1259 OID 25731)
-- Name: manage_cube_raw_files_id_seq; Type: SEQUENCE; Schema: raw_files; Owner: -
--

CREATE SEQUENCE raw_files.manage_cube_raw_files_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 4520 (class 0 OID 0)
-- Dependencies: 231
-- Name: manage_cube_raw_files_id_seq; Type: SEQUENCE OWNED BY; Schema: raw_files; Owner: -
--

ALTER SEQUENCE raw_files.manage_cube_raw_files_id_seq OWNED BY raw_files.manage_cube_raw_files.id;


--
-- TOC entry 239 (class 1259 OID 26011)
-- Name: manage_metadata_raw_files; Type: TABLE; Schema: raw_files; Owner: -
--

CREATE TABLE raw_files.manage_metadata_raw_files (
    id integer NOT NULL,
    productid bigint NOT NULL,
    file_hash text NOT NULL,
    date_download timestamp with time zone DEFAULT now() NOT NULL,
    active boolean DEFAULT true NOT NULL,
    storage_location text NOT NULL
);


--
-- TOC entry 238 (class 1259 OID 26010)
-- Name: manage_metadata_raw_files_id_seq; Type: SEQUENCE; Schema: raw_files; Owner: -
--

CREATE SEQUENCE raw_files.manage_metadata_raw_files_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 4521 (class 0 OID 0)
-- Dependencies: 238
-- Name: manage_metadata_raw_files_id_seq; Type: SEQUENCE OWNED BY; Schema: raw_files; Owner: -
--

ALTER SEQUENCE raw_files.manage_metadata_raw_files_id_seq OWNED BY raw_files.manage_metadata_raw_files.id;


--
-- TOC entry 234 (class 1259 OID 25745)
-- Name: manage_spine_raw_files; Type: TABLE; Schema: raw_files; Owner: -
--

CREATE TABLE raw_files.manage_spine_raw_files (
    id integer NOT NULL,
    file_hash text NOT NULL,
    date_download timestamp with time zone DEFAULT now() NOT NULL,
    active boolean DEFAULT false NOT NULL,
    storage_location text NOT NULL
);


pg_dump: creating SEQUENCE OWNED BY "raw_files.manage_cube_raw_files_id_seq"
pg_dump: creating TABLE "raw_files.manage_metadata_raw_files"
pg_dump: creating SEQUENCE "raw_files.manage_metadata_raw_files_id_seq"
pg_dump: creating SEQUENCE OWNED BY "raw_files.manage_metadata_raw_files_id_seq"
pg_dump: creating TABLE "raw_files.manage_spine_raw_files"
pg_dump: creating SEQUENCE "raw_files.manage_spine_raw_files_id_seq"
pg_dump: creating SEQUENCE OWNED BY "raw_files.manage_spine_raw_files_id_seq"
pg_dump: creating TABLE "raw_files.metadata_status"
pg_dump: creating TABLE "spine.cube"
pg_dump: creating TABLE "spine.cube_subject"
--
-- TOC entry 233 (class 1259 OID 25744)
-- Name: manage_spine_raw_files_id_seq; Type: SEQUENCE; Schema: raw_files; Owner: -
--

CREATE SEQUENCE raw_files.manage_spine_raw_files_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 4522 (class 0 OID 0)
-- Dependencies: 233
-- Name: manage_spine_raw_files_id_seq; Type: SEQUENCE OWNED BY; Schema: raw_files; Owner: -
--

ALTER SEQUENCE raw_files.manage_spine_raw_files_id_seq OWNED BY raw_files.manage_spine_raw_files.id;


--
-- TOC entry 237 (class 1259 OID 25992)
-- Name: metadata_status; Type: TABLE; Schema: raw_files; Owner: -
--

CREATE TABLE raw_files.metadata_status (
    productid bigint NOT NULL,
    last_download timestamp with time zone,
    download_pending boolean DEFAULT true NOT NULL,
    last_file_hash text
);


--
-- TOC entry 228 (class 1259 OID 17611)
-- Name: cube; Type: TABLE; Schema: spine; Owner: -
--

CREATE TABLE spine.cube (
    productid bigint NOT NULL,
    cansimid text,
    cubetitleen text,
    cubetitlefr text,
    cubestartdate date,
    cubeenddate date,
    releasetime date,
    archived smallint,
    frequencycode smallint,
    issuedate date
);


--
-- TOC entry 229 (class 1259 OID 17625)
-- Name: cube_subject; Type: TABLE; Schema: spine; Owner: -
--

CREATE TABLE spine.cube_subject (
    productid bigint NOT NULL,
    subjectcode text NOT NULL
);


--
-- TOC entry 230 (class 1259 OID 17632)
-- Name: cube_survey; Type: TABLE; Schema: spine; Owner: -
--

CREATE TABLE spine.cube_survey (
    productid bigint NOT NULL,
    surveycode text NOT NULL
);


pg_dump: creating TABLE "spine.cube_survey"
pg_dump: creating DEFAULT "raw_files.manage_cube_raw_files id"
--
-- TOC entry 4249 (class 2604 OID 25735)
-- Name: manage_cube_raw_files id; Type: DEFAULT; Schema: raw_files; Owner: -
--

ALTER TABLE ONLY raw_files.manage_cube_raw_files ALTER COLUMN id SET DEFAULT nextval('raw_files.manage_cube_raw_files_id_seq'::regclass);


--
-- TOC entry 4257 (class 2604 OID 26014)
-- Name: manage_metadata_raw_files id; Type: DEFAULT; Schema: raw_files; Owner: -
--

ALTER TABLE ONLY raw_files.manage_metadata_raw_files ALTER COLUMN id SET DEFAULT nextval('raw_files.manage_metadata_raw_files_id_seq'::regclass);


--
-- TOC entry 4252 (class 2604 OID 25748)
-- Name: manage_spine_raw_files id; Type: DEFAULT; Schema: raw_files; Owner: -
--

ALTER TABLE ONLY raw_files.manage_spine_raw_files ALTER COLUMN id SET DEFAULT nextval('raw_files.manage_spine_raw_files_id_seq'::regclass);


--
-- TOC entry 4299 (class 2606 OID 26674)
-- Name: c10100001 c10100001_pkey; Type: CONSTRAINT; Schema: cube_data; Owner: -
--

ALTER TABLE ONLY cube_data.c10100001
    ADD CONSTRAINT c10100001_pkey PRIMARY KEY (ref_date, geography_member_id, federal_public_sector_employment_member_id);


--
-- TOC entry 4303 (class 2606 OID 26683)
-- Name: c10100002 c10100002_pkey; Type: CONSTRAINT; Schema: cube_data; Owner: -
--

ALTER TABLE ONLY cube_data.c10100002
    ADD CONSTRAINT c10100002_pkey PRIMARY KEY (ref_date, geography_member_id, central_government_debt_member_id);


--
-- TOC entry 4307 (class 2606 OID 26692)
-- Name: c10100003 c10100003_pkey; Type: CONSTRAINT; Schema: cube_data; Owner: -
--

ALTER TABLE ONLY cube_data.c10100003
    ADD CONSTRAINT c10100003_pkey PRIMARY KEY (ref_date, geography_member_id, type_of_issues_member_id, issuers_member_id);


--
-- TOC entry 4311 (class 2606 OID 26707)
-- Name: c10100004 c10100004_pkey; Type: CONSTRAINT; Schema: cube_data; Owner: -
--

ALTER TABLE ONLY cube_data.c10100004
    ADD CONSTRAINT c10100004_pkey PRIMARY KEY (ref_date, geography_member_id, claims_and_deposits_member_id, type_of_non_resident_member_id, country_of_non_resident_member_id);


--
-- TOC entry 4315 (class 2606 OID 26722)
-- Name: c10100005 c10100005_pkey; Type: CONSTRAINT; Schema: cube_data; Owner: -
--

ALTER TABLE ONLY cube_data.c10100005
    ADD CONSTRAINT c10100005_pkey PRIMARY KEY (ref_date, geography_member_id, public_sector_components_member_id, canadian_classification_of_functions_of_government_ccofog_membe);


--
-- TOC entry 4297 (class 2606 OID 26321)
-- Name: dimension_set_member dimension_set_member_unique; Type: CONSTRAINT; Schema: dictionary; Owner: -
--

ALTER TABLE ONLY dictionary.dimension_set_member
    ADD CONSTRAINT dimension_set_member_unique UNIQUE (dimension_hash, member_id);


--
-- TOC entry 4295 (class 2606 OID 26184)
-- Name: dimension_set dimension_set_pkey; Type: CONSTRAINT; Schema: dictionary; Owner: -
--

ALTER TABLE ONLY dictionary.dimension_set
    ADD CONSTRAINT dimension_set_pkey PRIMARY KEY (dimension_hash);


pg_dump: creating DEFAULT "raw_files.manage_metadata_raw_files id"
pg_dump: creating DEFAULT "raw_files.manage_spine_raw_files id"
pg_dump: creating CONSTRAINT "cube_data.c10100001 c10100001_pkey"
pg_dump: creating CONSTRAINT "cube_data.c10100002 c10100002_pkey"
pg_dump: creating CONSTRAINT "cube_data.c10100003 c10100003_pkey"
pg_dump: creating CONSTRAINT "cube_data.c10100004 c10100004_pkey"
pg_dump: creating CONSTRAINT "cube_data.c10100005 c10100005_pkey"
pg_dump: creating CONSTRAINT "dictionary.dimension_set_member dimension_set_member_unique"
pg_dump: creating CONSTRAINT "dictionary.dimension_set dimension_set_pkey"
pg_dump: creating CONSTRAINT "processing.dimension_set_members dimension_set_members_pkey"
pg_dump: creating CONSTRAINT "processing.dimension_set dimension_set_pkey"
pg_dump: creating CONSTRAINT "processing.processed_dimensions processed_dimensions_pkey"
pg_dump: creating CONSTRAINT "processing.processed_members processed_members_pkey"
--
-- TOC entry 4329 (class 2606 OID 27343)
-- Name: dimension_set_members dimension_set_members_pkey; Type: CONSTRAINT; Schema: processing; Owner: -
--

ALTER TABLE ONLY processing.dimension_set_members
    ADD CONSTRAINT dimension_set_members_pkey PRIMARY KEY (dimension_hash, member_id);


--
-- TOC entry 4327 (class 2606 OID 27247)
-- Name: dimension_set dimension_set_pkey; Type: CONSTRAINT; Schema: processing; Owner: -
--

ALTER TABLE ONLY processing.dimension_set
    ADD CONSTRAINT dimension_set_pkey PRIMARY KEY (dimension_hash);


--
-- TOC entry 4325 (class 2606 OID 27231)
-- Name: processed_dimensions processed_dimensions_pkey; Type: CONSTRAINT; Schema: processing; Owner: -
--

ALTER TABLE ONLY processing.processed_dimensions
    ADD CONSTRAINT processed_dimensions_pkey PRIMARY KEY (productid, dimension_position);


pg_dump: creating CONSTRAINT "processing.raw_dimension processing_raw_dimension_pkey"
pg_dump: creating CONSTRAINT "processing.raw_member processing_raw_member_pkey"
pg_dump: creating CONSTRAINT "raw_files.changed_cubes_log changed_cubes_log_pkey"
--
-- TOC entry 4322 (class 2606 OID 27139)
-- Name: processed_members processed_members_pkey; Type: CONSTRAINT; Schema: processing; Owner: -
--

ALTER TABLE ONLY processing.processed_members
    ADD CONSTRAINT processed_members_pkey PRIMARY KEY (productid, dimension_position, member_id);


--
-- TOC entry 4333 (class 2606 OID 27372)
-- Name: raw_dimension processing_raw_dimension_pkey; Type: CONSTRAINT; Schema: processing; Owner: -
--

ALTER TABLE ONLY processing.raw_dimension
    ADD CONSTRAINT processing_raw_dimension_pkey PRIMARY KEY (productid, dimension_position);


--
-- TOC entry 4337 (class 2606 OID 27380)
-- Name: raw_member processing_raw_member_pkey; Type: CONSTRAINT; Schema: processing; Owner: -
--

ALTER TABLE ONLY processing.raw_member
    ADD CONSTRAINT processing_raw_member_pkey PRIMARY KEY (productid, dimension_position, member_id);


--
-- TOC entry 4289 (class 2606 OID 25951)
-- Name: changed_cubes_log changed_cubes_log_pkey; Type: CONSTRAINT; Schema: raw_files; Owner: -
--

ALTER TABLE ONLY raw_files.changed_cubes_log
    ADD CONSTRAINT changed_cubes_log_pkey PRIMARY KEY (productid, change_date);


--
-- TOC entry 4287 (class 2606 OID 25871)
-- Name: cube_status cube_status_pkey; Type: CONSTRAINT; Schema: raw_files; Owner: -
--

ALTER TABLE ONLY raw_files.cube_status
    ADD CONSTRAINT cube_status_pkey PRIMARY KEY (productid);


pg_dump: creating CONSTRAINT "raw_files.cube_status cube_status_pkey"
pg_dump: creating CONSTRAINT "raw_files.manage_cube_raw_files manage_cube_raw_files_pkey"
pg_dump: creating CONSTRAINT "raw_files.manage_metadata_raw_files manage_metadata_raw_files_pkey"
--
-- TOC entry 4280 (class 2606 OID 25741)
-- Name: manage_cube_raw_files manage_cube_raw_files_pkey; Type: CONSTRAINT; Schema: raw_files; Owner: -
--

ALTER TABLE ONLY raw_files.manage_cube_raw_files
    ADD CONSTRAINT manage_cube_raw_files_pkey PRIMARY KEY (id);


--
-- TOC entry 4293 (class 2606 OID 26020)
-- Name: manage_metadata_raw_files manage_metadata_raw_files_pkey; Type: CONSTRAINT; Schema: raw_files; Owner: -
--

ALTER TABLE ONLY raw_files.manage_metadata_raw_files
    ADD CONSTRAINT manage_metadata_raw_files_pkey PRIMARY KEY (id);


--
-- TOC entry 4285 (class 2606 OID 25754)
-- Name: manage_spine_raw_files manage_spine_raw_files_pkey; Type: CONSTRAINT; Schema: raw_files; Owner: -
--

ALTER TABLE ONLY raw_files.manage_spine_raw_files
    ADD CONSTRAINT manage_spine_raw_files_pkey PRIMARY KEY (id);


pg_dump: creating CONSTRAINT "raw_files.manage_spine_raw_files manage_spine_raw_files_pkey"
pg_dump: creating CONSTRAINT "raw_files.metadata_status metadata_status_pkey"
--
-- TOC entry 4291 (class 2606 OID 26002)
-- Name: metadata_status metadata_status_pkey; Type: CONSTRAINT; Schema: raw_files; Owner: -
--

ALTER TABLE ONLY raw_files.metadata_status
    ADD CONSTRAINT metadata_status_pkey PRIMARY KEY (productid);


pg_dump: creating CONSTRAINT "spine.cube cube_pkey"
pg_dump: creating CONSTRAINT "spine.cube_subject cube_subject_pkey"
pg_dump:--
-- TOC entry 4274 (class 2606 OID 17617)
-- Name: cube cube_pkey; Type: CONSTRAINT; Schema: spine; Owner: -
--

ALTER TABLE ONLY spine.cube
    ADD CONSTRAINT cube_pkey PRIMARY KEY (productid);


--
-- TOC entry 4276 (class 2606 OID 17631)
-- Name: cube_subject cube_subject_pkey; Type: CONSTRAINT; Schema: spine; Owner: -
--

ALTER TABLE ONLY spine.cube_subject
    ADD CONSTRAINT cube_subject_pkey PRIMARY KEY (productid, subjectcode);


 creating CONSTRAINT "spine.cube_survey cube_survey_pkey"
pg_dump: creating INDEX "cube_data.c10100001_ref_date_idx"
--
-- TOC entry 4278 (class 2606 OID 17638)
-- Name: cube_survey cube_survey_pkey; Type: CONSTRAINT; Schema: spine; Owner: -
--

ALTER TABLE ONLY spine.cube_survey
    ADD CONSTRAINT cube_survey_pkey PRIMARY KEY (productid, surveycode);


--
-- TOC entry 4300 (class 1259 OID 26675)
-- Name: c10100001_ref_date_idx; Type: INDEX; Schema: cube_data; Owner: -
--

CREATE INDEX c10100001_ref_date_idx ON cube_data.c10100001 USING btree (ref_date);


pg_dump: creating INDEX "cube_data.c10100001_value_idx"
pg_dump: creating INDEX "cube_data.c10100002_ref_date_idx"
--
-- TOC entry 4301 (class 1259 OID 26676)
-- Name: c10100001_value_idx; Type: INDEX; Schema: cube_data; Owner: -
--

CREATE INDEX c10100001_value_idx ON cube_data.c10100001 USING btree (value) WHERE (value IS NOT NULL);


--
-- TOC entry 4304 (class 1259 OID 26684)
-- Name: c10100002_ref_date_idx; Type: INDEX; Schema: cube_data; Owner: -
--

CREATE INDEX c10100002_ref_date_idx ON cube_data.c10100002 USING btree (ref_date);


pg_dump: creating INDEX "cube_data.c10100002_value_idx"
--
-- TOC entry 4305 (class 1259 OID 26685)
-- Name: c10100002_value_idx; Type: INDEX; Schema: cube_data; Owner: -
--

CREATE INDEX c10100002_value_idx ON cube_data.c10100002 USING btree (value) WHERE (value IS NOT NULL);


--
-- TOC entry 4308 (class 1259 OID 26693)
-- Name: c10100003_ref_date_idx; Type: INDEX; Schema: cube_data; Owner: -
--

CREATE INDEX c10100003_ref_date_idx ON cube_data.c10100003 USING btree (ref_date);


--
-- TOC entry 4309 (class 1259 OID 26694)
-- Name: c10100003_value_idx; Type: INDEX; Schema: cube_data; Owner: -
--

CREATE INDEX c10100003_value_idx ON cube_data.c10100003 USING btree (value) WHERE (value IS NOT NULL);


pg_dump: creating INDEX "cube_data.c10100003_ref_date_idx"
pg_dump: creating INDEX "cube_data.c10100003_value_idx"
pg_dump: creating INDEX "cube_data.c10100004_ref_date_idx"
pg_dump: creating INDEX "cube_data.c10100004_value_idx"
--
-- TOC entry 4312 (class 1259 OID 26708)
-- Name: c10100004_ref_date_idx; Type: INDEX; Schema: cube_data; Owner: -
--

CREATE INDEX c10100004_ref_date_idx ON cube_data.c10100004 USING btree (ref_date);


--
-- TOC entry 4313 (class 1259 OID 26709)
-- Name: c10100004_value_idx; Type: INDEX; Schema: cube_data; Owner: -
--

CREATE INDEX c10100004_value_idx ON cube_data.c10100004 USING btree (value) WHERE (value IS NOT NULL);


pg_dump: creating INDEX "cube_data.c10100005_ref_date_idx"
--
-- TOC entry 4316 (class 1259 OID 26723)
-- Name: c10100005_ref_date_idx; Type: INDEX; Schema: cube_data; Owner: -
--

CREATE INDEX c10100005_ref_date_idx ON cube_data.c10100005 USING btree (ref_date);


pg_dump: creating INDEX "cube_data.c10100005_value_idx"
--
-- TOC entry 4317 (class 1259 OID 26724)
-- Name: c10100005_value_idx; Type: INDEX; Schema: cube_data; Owner: -
--

CREATE INDEX c10100005_value_idx ON cube_data.c10100005 USING btree (value) WHERE (value IS NOT NULL);


pg_dump: creating INDEX "processing.idx_dimension_set_members_member_id"
pg_dump: creating INDEX "processing.idx_processed_dimensions_dimension_hash"
--
-- TOC entry 4330 (class 1259 OID 27344)
-- Name: idx_dimension_set_members_member_id; Type: INDEX; Schema: processing; Owner: -
--

CREATE INDEX idx_dimension_set_members_member_id ON processing.dimension_set_members USING btree (member_id);


--
-- TOC entry 4323 (class 1259 OID 27232)
-- Name: idx_processed_dimensions_dimension_hash; Type: INDEX; Schema: processing; Owner: -
--

CREATE INDEX idx_processed_dimensions_dimension_hash ON processing.processed_dimensions USING btree (dimension_hash);


pg_dump: creating INDEX "processing.idx_processed_members_dimension_hash"
pg_dump: --
-- TOC entry 4318 (class 1259 OID 27334)
-- Name: idx_processed_members_dimension_hash; Type: INDEX; Schema: processing; Owner: -
--

CREATE INDEX idx_processed_members_dimension_hash ON processing.processed_members USING btree (dimension_hash);


--
-- TOC entry 4319 (class 1259 OID 27141)
-- Name: idx_processed_members_member_hash; Type: INDEX; Schema: processing; Owner: -
--

CREATE INDEX idx_processed_members_member_hash ON processing.processed_members USING btree (member_hash);


--
-- TOC entry 4320 (class 1259 OID 27140)
-- Name: idx_processed_members_productid_pos; Type: INDEX; Schema: processing; Owner: -
--

CREATE INDEX idx_processed_members_productid_pos ON processing.processed_members USING btree (productid, dimension_position);


creating INDEX "processing.idx_processed_members_member_hash"
pg_dump: creating INDEX "processing.idx_processed_members_productid_pos"
pg_dump: creating INDEX "processing.idx_processing_raw_dimension_productid"
--
-- TOC entry 4331 (class 1259 OID 27381)
-- Name: idx_processing_raw_dimension_productid; Type: INDEX; Schema: processing; Owner: -
--

CREATE INDEX idx_processing_raw_dimension_productid ON processing.raw_dimension USING btree (productid);


--
-- TOC entry 4334 (class 1259 OID 27383)
-- Name: idx_processing_raw_member_classification; Type: INDEX; Schema: processing; Owner: -
--

CREATE INDEX idx_processing_raw_member_classification ON processing.raw_member USING btree (classification_code);


pg_dump: creating INDEX "processing.idx_processing_raw_member_classification"
pg_dump: creating INDEX "processing.idx_processing_raw_member_productid"
pg_dump: creating INDEX "raw_files.manage_cube_raw_files_productid_file_hash_idx"
--
-- TOC entry 4335 (class 1259 OID 27382)
-- Name: idx_processing_raw_member_productid; Type: INDEX; Schema: processing; Owner: -
--

CREATE INDEX idx_processing_raw_member_productid ON processing.raw_member USING btree (productid);


--
-- TOC entry 4281 (class 1259 OID 25743)
-- Name: manage_cube_raw_files_productid_file_hash_idx; Type: INDEX; Schema: raw_files; Owner: -
--

CREATE UNIQUE INDEX manage_cube_raw_files_productid_file_hash_idx ON raw_files.manage_cube_raw_files USING btree (productid, file_hash);


pg_dump: creating INDEX "raw_files.manage_cube_raw_files_productid_idx"
pg_dump: creating INDEX "raw_files.manage_spine_raw_files_file_hash_idx"
--
-- TOC entry 4282 (class 1259 OID 25742)
-- Name: manage_cube_raw_files_productid_idx; Type: INDEX; Schema: raw_files; Owner: -
--

CREATE INDEX manage_cube_raw_files_productid_idx ON raw_files.manage_cube_raw_files USING btree (productid);


--
-- TOC entry 4283 (class 1259 OID 25755)
-- Name: manage_spine_raw_files_file_hash_idx; Type: INDEX; Schema: raw_files; Owner: -
--

CREATE UNIQUE INDEX manage_spine_raw_files_file_hash_idx ON raw_files.manage_spine_raw_files USING btree (file_hash);


pg_dump: creating FK CONSTRAINT "dictionary.dimension_set_member dimension_member_dimension_hash_fkey"
--
-- TOC entry 4338 (class 2606 OID 26192)
-- Name: dimension_set_member dimension_member_dimension_hash_fkey; Type: FK CONSTRAINT; Schema: dictionary; Owner: -
--

ALTER TABLE ONLY dictionary.dimension_set_member
    ADD CONSTRAINT dimension_member_dimension_hash_fkey FOREIGN KEY (dimension_hash) REFERENCES dictionary.dimension_set(dimension_hash);


-- Completed on 2025-06-20 13:12:01 UTC

--
-- PostgreSQL database dump complete
--


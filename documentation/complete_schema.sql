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
pg_dump: creating TABLE "processing.dimension_set"
pg_dump: creating COMMENT "processing.TABLE dimension_set"
pg_dump: creating COMMENT "processing.COLUMN dimension_set.dimension_hash"
pg_dump: creating COMMENT "processing.COLUMN dimension_set.dimension_name_en"
pg_dump: creating COMMENT "processing.COLUMN dimension_set.dimension_name_fr"
pg_dump: creating COMMENT "processing.COLUMN dimension_set.has_uom"
pg_dump: creating COMMENT "processing.COLUMN dimension_set.usage_count"
pg_dump: creating COMMENT "processing.COLUMN dimension_set.is_tree"
pg_dump: creating COMMENT "processing.COLUMN dimension_set.is_hetero"
pg_dump: creating TABLE "processing.dimension_set_members"
pg_dump: creating COMMENT "processing.TABLE dimension_set_members"
pg_dump: creating COMMENT "processing.COLUMN dimension_set_members.dimension_hash"
pg_dump: creating COMMENT "processing.COLUMN dimension_set_members.member_name_en"
pg_dump: creating COMMENT "processing.COLUMN dimension_set_members.member_name_fr"
pg_dump: creating COMMENT "processing.COLUMN dimension_set_members.usage_count"
pg_dump: creating COMMENT "processing.COLUMN dimension_set_members.tree_level"
pg_dump: creating TABLE "processing.processed_dimensions"
pg_dump: creating COMMENT "processing.TABLE processed_dimensions"
pg_dump: creating COMMENT "processing.COLUMN processed_dimensions.dimension_hash"
pg_dump: creating COMMENT "processing.COLUMN processed_dimensions.has_uom"
pg_dump: creating TABLE "processing.processed_members"
pg_dump: creating COMMENT "processing.TABLE processed_members"
pg_dump:--
-- PostgreSQL database dump
--

-- Dumped from database version 15.8 (Debian 15.8-1.pgdg110+1)
-- Dumped by pg_dump version 15.8 (Debian 15.8-1.pgdg110+1)

-- Started on 2025-06-20 15:33:18 UTC

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
-- TOC entry 4432 (class 1262 OID 16384)
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
-- TOC entry 4433 (class 0 OID 0)
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
-- TOC entry 4434 (class 0 OID 0)
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
-- TOC entry 4435 (class 0 OID 0)
-- Dependencies: 3
-- Name: EXTENSION "uuid-ossp"; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION "uuid-ossp" IS 'generate universally unique identifiers (UUIDs)';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 242 (class 1259 OID 27237)
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
    updated_at timestamp with time zone DEFAULT now(),
    is_tree boolean DEFAULT false,
    is_hetero boolean DEFAULT false
);


--
-- TOC entry 4436 (class 0 OID 0)
-- Dependencies: 242
-- Name: TABLE dimension_set; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON TABLE processing.dimension_set IS 'Canonical dimension definitions with most common labels and characteristics. Built from processed_dimensions in script 12.';


--
-- TOC entry 4437 (class 0 OID 0)
-- Dependencies: 242
-- Name: COLUMN dimension_set.dimension_hash; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.dimension_set.dimension_hash IS '12-character SHA-256 hash identifying this unique dimension structure';


--
-- TOC entry 4438 (class 0 OID 0)
-- Dependencies: 242
-- Name: COLUMN dimension_set.dimension_name_en; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.dimension_set.dimension_name_en IS 'Most common English dimension name in title case';


--
-- TOC entry 4439 (class 0 OID 0)
-- Dependencies: 242
-- Name: COLUMN dimension_set.dimension_name_fr; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.dimension_set.dimension_name_fr IS 'Most common French dimension name in title case';


--
-- TOC entry 4440 (class 0 OID 0)
-- Dependencies: 242
-- Name: COLUMN dimension_set.has_uom; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.dimension_set.has_uom IS 'True if any instance of this dimension contains unit of measure information';


--
-- TOC entry 4441 (class 0 OID 0)
-- Dependencies: 242
-- Name: COLUMN dimension_set.usage_count; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.dimension_set.usage_count IS 'Number of (productid, dimension_position) instances using this dimension_hash';


--
-- TOC entry 4442 (class 0 OID 0)
-- Dependencies: 242
-- Name: COLUMN dimension_set.is_tree; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.dimension_set.is_tree IS 'True if any members in this dimension have parent-child relationships (hierarchical structure)';


--
-- TOC entry 4443 (class 0 OID 0)
-- Dependencies: 242
-- Name: COLUMN dimension_set.is_hetero; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.dimension_set.is_hetero IS 'True if members in this dimension have varying units of measure (heterogeneous UOM)';


--
-- TOC entry 243 (class 1259 OID 27335)
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
    created_at timestamp with time zone DEFAULT now(),
    tree_level integer
);


--
-- TOC entry 4444 (class 0 OID 0)
-- Dependencies: 243
-- Name: TABLE dimension_set_members; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON TABLE processing.dimension_set_members IS 'Canonical member definitions within each dimension. Built from processed_members in script 13.';


--
-- TOC entry 4445 (class 0 OID 0)
-- Dependencies: 243
-- Name: COLUMN dimension_set_members.dimension_hash; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.dimension_set_members.dimension_hash IS 'Reference to canonical dimension hash';


--
-- TOC entry 4446 (class 0 OID 0)
-- Dependencies: 243
-- Name: COLUMN dimension_set_members.member_name_en; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.dimension_set_members.member_name_en IS 'Most common English member name across all cubes using this dimension';


--
-- TOC entry 4447 (class 0 OID 0)
-- Dependencies: 243
-- Name: COLUMN dimension_set_members.member_name_fr; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.dimension_set_members.member_name_fr IS 'Most common French member name across all cubes using this dimension';


--
-- TOC entry 4448 (class 0 OID 0)
-- Dependencies: 243
-- Name: COLUMN dimension_set_members.usage_count; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.dimension_set_members.usage_count IS 'Number of cube instances where this member appears in this dimension';


--
-- TOC entry 4449 (class 0 OID 0)
-- Dependencies: 243
-- Name: COLUMN dimension_set_members.tree_level; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.dimension_set_members.tree_level IS 'Hierarchical level: 1 for root nodes (no parent), 2 for children of root nodes, etc. NULL for non-hierarchical dimensions.';


--
-- TOC entry 241 (class 1259 OID 27224)
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


--
-- TOC entry 4450 (class 0 OID 0)
-- Dependencies: 241
-- Name: TABLE processed_dimensions; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON TABLE processing.processed_dimensions IS 'Mapping of (productid, dimension_position) to dimension_hash with raw dimension metadata. Built from processed_members + raw_dimension in script 11.';


--
-- TOC entry 4451 (class 0 OID 0)
-- Dependencies: 241
-- Name: COLUMN processed_dimensions.dimension_hash; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.processed_dimensions.dimension_hash IS '12-character SHA-256 hash of concatenated member hashes (sorted by member_id) within this dimension';


--
-- TOC entry 4452 (class 0 OID 0)
-- Dependencies: 241
-- Name: COLUMN processed_dimensions.has_uom; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.processed_dimensions.has_uom IS 'Indicates if this dimension contains unit of measure information';


--
-- TOC entry 240 (class 1259 OID 27132)
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
-- TOC entry 4453 (class 0 OID 0)
-- Dependencies: 240
-- Name: TABLE processed_members; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON TABLE processing.processed_members IS 'Raw member data with computed member-level hashes. Input for dimension registry building (script 11+).';


--
-- TOC entry 4454 (class 0 OID 0)
-- Dependencies: 240
-- Name: COLUMN processed_members.member_hash; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.processed_members.member_hash IS '12-character SHA-256 hash of member_id + normalized_label_en + parent_id + uom_code';


--
-- TOC entry 4455 (class 0 OID 0)
-- Dependencies: 240
-- Name: COLUMN processed_members.member_label_norm; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.processed_members.member_label_norm IS 'Normalized (lowercase, trimmed) version of member_name_en for hashing consistency';


--
-- TOC entry 4456 (class 0 OID 0)
-- Dependencies: 240
-- Name: COLUMN processed_members.dimension_hash; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.processed_members.dimension_hash IS '12-character dimension hash populated by script 13 from processed_dimensions';


--
-- TOC entry 244 (class 1259 OID 27365)
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


 creating COMMENT "processing.COLUMN processed_members.member_hash"
pg_dump: creating COMMENT "processing.COLUMN processed_members.member_label_norm"
pg_dump: creating COMMENT "processing.COLUMN processed_members.dimension_hash"
pg_dump: creating TABLE "processing.raw_dimension"
pg_dump: creating COMMENT "processing.TABLE raw_dimension"
pg_dump: creating COMMENT "processing.COLUMN raw_dimension.created_at"
pg_dump: creating TABLE "processing.raw_member"
--
-- TOC entry 4457 (class 0 OID 0)
-- Dependencies: 244
-- Name: TABLE raw_dimension; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON TABLE processing.raw_dimension IS 'Raw dimension metadata from Statistics Canada API - processing schema';


--
-- TOC entry 4458 (class 0 OID 0)
-- Dependencies: 244
-- Name: COLUMN raw_dimension.created_at; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.raw_dimension.created_at IS 'Timestamp when record was created';


--
-- TOC entry 245 (class 1259 OID 27373)
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


--
-- TOC entry 4459 (class 0 OID 0)
-- Dependencies: 245
-- Name: TABLE raw_member; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON TABLE processing.raw_member IS 'Raw member metadata from Statistics Canada API - processing schema';


pg_dump: creating COMMENT "processing.TABLE raw_member"
pg_dump: creating COMMENT "processing.COLUMN raw_member.created_at"
pg_dump: creating TABLE "raw_files.changed_cubes_log"
pg_dump: creating TABLE "raw_files.cube_status"
pg_dump: creating TABLE "raw_files.manage_cube_raw_files"
--
-- TOC entry 4460 (class 0 OID 0)
-- Dependencies: 245
-- Name: COLUMN raw_member.created_at; Type: COMMENT; Schema: processing; Owner: -
--

COMMENT ON COLUMN processing.raw_member.created_at IS 'Timestamp when record was created';


--
-- TOC entry 236 (class 1259 OID 25947)
-- Name: changed_cubes_log; Type: TABLE; Schema: raw_files; Owner: -
--

CREATE TABLE raw_files.changed_cubes_log (
    productid bigint NOT NULL,
    change_date date NOT NULL
);


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
-- TOC entry 4461 (class 0 OID 0)
-- Dependencies: 231
-- Name: manage_cube_raw_files_id_seq; Type: SEQUENCE OWNED BY; Schema: raw_files; Owner: -
--

ALTER SEQUENCE raw_files.manage_cube_raw_files_id_seq OWNED BY raw_files.manage_cube_raw_files.id;


pg_dump: creating SEQUENCE "raw_files.manage_cube_raw_files_id_seq"
pg_dump: creating SEQUENCE OWNED BY "raw_files.manage_cube_raw_files_id_seq"
pg_dump: creating TABLE "raw_files.manage_metadata_raw_files"
pg_dump: creating SEQUENCE "raw_files.manage_metadata_raw_files_id_seq"
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
-- TOC entry 4462 (class 0 OID 0)
-- Dependencies: 238
-- Name: manage_metadata_raw_files_id_seq; Type: SEQUENCE OWNED BY; Schema: raw_files; Owner: -
--

ALTER SEQUENCE raw_files.manage_metadata_raw_files_id_seq OWNED BY raw_files.manage_metadata_raw_files.id;


pg_dump: creating SEQUENCE OWNED BY "raw_files.manage_metadata_raw_files_id_seq"
pg_dump: creating TABLE "raw_files.manage_spine_raw_files"
pg_dump: creating SEQUENCE "raw_files.manage_spine_raw_files_id_seq"
pg_dump: creating SEQUENCE OWNED BY "raw_files.manage_spine_raw_files_id_seq"
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
-- TOC entry 4463 (class 0 OID 0)
-- Dependencies: 233
-- Name: manage_spine_raw_files_id_seq; Type: SEQUENCE OWNED BY; Schema: raw_files; Owner: -
--

ALTER SEQUENCE raw_files.manage_spine_raw_files_id_seq OWNED BY raw_files.manage_spine_raw_files.id;


pg_dump: creating TABLE "raw_files.metadata_status"
pg_dump: creating TABLE "spine.cube"
pg_dump: creating TABLE "spine.cube_subject"
pg_dump: creating TABLE "spine.cube_survey"
pg_dump: creating DEFAULT "raw_files.manage_cube_raw_files id"
pg_dump: creating DEFAULT "raw_files.manage_metadata_raw_files id"
pg_dump: creating DEFAULT "raw_files.manage_spine_raw_files id"
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


--
-- TOC entry 4213 (class 2604 OID 25735)
-- Name: manage_cube_raw_files id; Type: DEFAULT; Schema: raw_files; Owner: -
--

ALTER TABLE ONLY raw_files.manage_cube_raw_files ALTER COLUMN id SET DEFAULT nextval('raw_files.manage_cube_raw_files_id_seq'::regclass);


--
-- TOC entry 4221 (class 2604 OID 26014)
-- Name: manage_metadata_raw_files id; Type: DEFAULT; Schema: raw_files; Owner: -
--

ALTER TABLE ONLY raw_files.manage_metadata_raw_files ALTER COLUMN id SET DEFAULT nextval('raw_files.manage_metadata_raw_files_id_seq'::regclass);


--
-- TOC entry 4216 (class 2604 OID 25748)
-- Name: manage_spine_raw_files id; Type: DEFAULT; Schema: raw_files; Owner: -
--

ALTER TABLE ONLY raw_files.manage_spine_raw_files ALTER COLUMN id SET DEFAULT nextval('raw_files.manage_spine_raw_files_id_seq'::regclass);


pg_dump: creating CONSTRAINT "processing.dimension_set_members dimension_set_members_pkey"
pg_dump: creating CONSTRAINT "processing.dimension_set dimension_set_pkey"
--
-- TOC entry 4271 (class 2606 OID 27343)
-- Name: dimension_set_members dimension_set_members_pkey; Type: CONSTRAINT; Schema: processing; Owner: -
--

ALTER TABLE ONLY processing.dimension_set_members
    ADD CONSTRAINT dimension_set_members_pkey PRIMARY KEY (dimension_hash, member_id);


--
-- TOC entry 4269 (class 2606 OID 27247)
-- Name: dimension_set dimension_set_pkey; Type: CONSTRAINT; Schema: processing; Owner: -
--

ALTER TABLE ONLY processing.dimension_set
    ADD CONSTRAINT dimension_set_pkey PRIMARY KEY (dimension_hash);


pg_dump: creating CONSTRAINT "processing.processed_dimensions processed_dimensions_pkey"
--
-- TOC entry 4267 (class 2606 OID 27231)
-- Name: processed_dimensions processed_dimensions_pkey; Type: CONSTRAINT; Schema: processing; Owner: -
--

ALTER TABLE ONLY processing.processed_dimensions
    ADD CONSTRAINT processed_dimensions_pkey PRIMARY KEY (productid, dimension_position);


pg_dump: creating CONSTRAINT "processing.processed_members processed_members_pkey"
--
-- TOC entry 4264 (class 2606 OID 27139)
-- Name: processed_members processed_members_pkey; Type: CONSTRAINT; Schema: processing; Owner: -
--

ALTER TABLE ONLY processing.processed_members
    ADD CONSTRAINT processed_members_pkey PRIMARY KEY (productid, dimension_position, member_id);


pg_dump: creating CONSTRAINT "processing.raw_dimension processing_raw_dimension_pkey"
--
-- TOC entry 4275 (class 2606 OID 27372)
-- Name: raw_dimension processing_raw_dimension_pkey; Type: CONSTRAINT; Schema: processing; Owner: -
--

ALTER TABLE ONLY processing.raw_dimension
    ADD CONSTRAINT processing_raw_dimension_pkey PRIMARY KEY (productid, dimension_position);


pg_dump: creating CONSTRAINT "processing.raw_member processing_raw_member_pkey"
pg_dump: creating CONSTRAINT "raw_files.changed_cubes_log changed_cubes_log_pkey"
pg_dump: creating CONSTRAINT "raw_files.cube_status cube_status_pkey"
pg_dump: creating CONSTRAINT "raw_files.manage_cube_raw_files manage_cube_raw_files_pkey"
pg_dump: creating CONSTRAINT "raw_files.manage_metadata_raw_files manage_metadata_raw_files_pkey"
--
-- TOC entry 4279 (class 2606 OID 27380)
-- Name: raw_member processing_raw_member_pkey; Type: CONSTRAINT; Schema: processing; Owner: -
--

ALTER TABLE ONLY processing.raw_member
    ADD CONSTRAINT processing_raw_member_pkey PRIMARY KEY (productid, dimension_position, member_id);


--
-- TOC entry 4255 (class 2606 OID 25951)
-- Name: changed_cubes_log changed_cubes_log_pkey; Type: CONSTRAINT; Schema: raw_files; Owner: -
--

ALTER TABLE ONLY raw_files.changed_cubes_log
    ADD CONSTRAINT changed_cubes_log_pkey PRIMARY KEY (productid, change_date);


--
-- TOC entry 4253 (class 2606 OID 25871)
-- Name: cube_status cube_status_pkey; Type: CONSTRAINT; Schema: raw_files; Owner: -
--

ALTER TABLE ONLY raw_files.cube_status
    ADD CONSTRAINT cube_status_pkey PRIMARY KEY (productid);


--
-- TOC entry 4246 (class 2606 OID 25741)
-- Name: manage_cube_raw_files manage_cube_raw_files_pkey; Type: CONSTRAINT; Schema: raw_files; Owner: -
--

ALTER TABLE ONLY raw_files.manage_cube_raw_files
    ADD CONSTRAINT manage_cube_raw_files_pkey PRIMARY KEY (id);


--
-- TOC entry 4259 (class 2606 OID 26020)
-- Name: manage_metadata_raw_files manage_metadata_raw_files_pkey; Type: CONSTRAINT; Schema: raw_files; Owner: -
--

ALTER TABLE ONLY raw_files.manage_metadata_raw_files
    ADD CONSTRAINT manage_metadata_raw_files_pkey PRIMARY KEY (id);


--
-- TOC entry 4251 (class 2606 OID 25754)
-- Name: manage_spine_raw_files manage_spine_raw_files_pkey; Type: CONSTRAINT; Schema: raw_files; Owner: -
--

ALTER TABLE ONLY raw_files.manage_spine_raw_files
    ADD CONSTRAINT manage_spine_raw_files_pkey PRIMARY KEY (id);


--
-- TOC entry 4257 (class 2606 OID 26002)
-- Name: metadata_status metadata_status_pkey; Type: CONSTRAINT; Schema: raw_files; Owner: -
--

ALTER TABLE ONLY raw_files.metadata_status
    ADD CONSTRAINT metadata_status_pkey PRIMARY KEY (productid);


pg_dump: creating CONSTRAINT "raw_files.manage_spine_raw_files manage_spine_raw_files_pkey"
pg_dump: creating CONSTRAINT "raw_files.metadata_status metadata_status_pkey"
pg_dump: creating CONSTRAINT "spine.cube cube_pkey"
pg_dump: creating CONSTRAINT "spine.cube_subject cube_subject_pkey"
--
-- TOC entry 4240 (class 2606 OID 17617)
-- Name: cube cube_pkey; Type: CONSTRAINT; Schema: spine; Owner: -
--

ALTER TABLE ONLY spine.cube
    ADD CONSTRAINT cube_pkey PRIMARY KEY (productid);


--
-- TOC entry 4242 (class 2606 OID 17631)
-- Name: cube_subject cube_subject_pkey; Type: CONSTRAINT; Schema: spine; Owner: -
--

ALTER TABLE ONLY spine.cube_subject
    ADD CONSTRAINT cube_subject_pkey PRIMARY KEY (productid, subjectcode);


pg_dump: creating CONSTRAINT "spine.cube_survey cube_survey_pkey"
pg_dump: creating INDEX "processing.idx_dimension_set_members_member_id"
--
-- TOC entry 4244 (class 2606 OID 17638)
-- Name: cube_survey cube_survey_pkey; Type: CONSTRAINT; Schema: spine; Owner: -
--

ALTER TABLE ONLY spine.cube_survey
    ADD CONSTRAINT cube_survey_pkey PRIMARY KEY (productid, surveycode);


pg_dump: creating INDEX "processing.idx_processed_dimensions_dimension_hash"
pg_dump:--
-- TOC entry 4272 (class 1259 OID 27344)
-- Name: idx_dimension_set_members_member_id; Type: INDEX; Schema: processing; Owner: -
--

CREATE INDEX idx_dimension_set_members_member_id ON processing.dimension_set_members USING btree (member_id);


--
-- TOC entry 4265 (class 1259 OID 27232)
-- Name: idx_processed_dimensions_dimension_hash; Type: INDEX; Schema: processing; Owner: -
--

CREATE INDEX idx_processed_dimensions_dimension_hash ON processing.processed_dimensions USING btree (dimension_hash);


 creating INDEX "processing.idx_processed_members_dimension_hash"
pg_dump: creating INDEX "processing.idx_processed_members_member_hash"
pg_dump: creating INDEX "processing.idx_processed_members_productid_pos"
pg_dump: creating INDEX "processing.idx_processing_raw_dimension_productid"
--
-- TOC entry 4260 (class 1259 OID 27334)
-- Name: idx_processed_members_dimension_hash; Type: INDEX; Schema: processing; Owner: -
--

CREATE INDEX idx_processed_members_dimension_hash ON processing.processed_members USING btree (dimension_hash);


--
-- TOC entry 4261 (class 1259 OID 27141)
-- Name: idx_processed_members_member_hash; Type: INDEX; Schema: processing; Owner: -
--

CREATE INDEX idx_processed_members_member_hash ON processing.processed_members USING btree (member_hash);


--
-- TOC entry 4262 (class 1259 OID 27140)
-- Name: idx_processed_members_productid_pos; Type: INDEX; Schema: processing; Owner: -
--

CREATE INDEX idx_processed_members_productid_pos ON processing.processed_members USING btree (productid, dimension_position);


--
-- TOC entry 4273 (class 1259 OID 27381)
-- Name: idx_processing_raw_dimension_productid; Type: INDEX; Schema: processing; Owner: -
--

CREATE INDEX idx_processing_raw_dimension_productid ON processing.raw_dimension USING btree (productid);


pg_dump: creating INDEX "processing.idx_processing_raw_member_classification"
pg_dump: creating INDEX "processing.idx_processing_raw_member_productid"
pg_dump: creating INDEX "raw_files.manage_cube_raw_files_productid_file_hash_idx"
pg_dump: creating INDEX "raw_files.manage_cube_raw_files_productid_idx"
pg_dump: creating INDEX "raw_files.manage_spine_raw_files_file_hash_idx"
--
-- TOC entry 4276 (class 1259 OID 27383)
-- Name: idx_processing_raw_member_classification; Type: INDEX; Schema: processing; Owner: -
--

CREATE INDEX idx_processing_raw_member_classification ON processing.raw_member USING btree (classification_code);


--
-- TOC entry 4277 (class 1259 OID 27382)
-- Name: idx_processing_raw_member_productid; Type: INDEX; Schema: processing; Owner: -
--

CREATE INDEX idx_processing_raw_member_productid ON processing.raw_member USING btree (productid);


--
-- TOC entry 4247 (class 1259 OID 25743)
-- Name: manage_cube_raw_files_productid_file_hash_idx; Type: INDEX; Schema: raw_files; Owner: -
--

CREATE UNIQUE INDEX manage_cube_raw_files_productid_file_hash_idx ON raw_files.manage_cube_raw_files USING btree (productid, file_hash);


--
-- TOC entry 4248 (class 1259 OID 25742)
-- Name: manage_cube_raw_files_productid_idx; Type: INDEX; Schema: raw_files; Owner: -
--

CREATE INDEX manage_cube_raw_files_productid_idx ON raw_files.manage_cube_raw_files USING btree (productid);


--
-- TOC entry 4249 (class 1259 OID 25755)
-- Name: manage_spine_raw_files_file_hash_idx; Type: INDEX; Schema: raw_files; Owner: -
--

CREATE UNIQUE INDEX manage_spine_raw_files_file_hash_idx ON raw_files.manage_spine_raw_files USING btree (file_hash);


-- Completed on 2025-06-20 15:33:18 UTC

--
-- PostgreSQL database dump complete
--


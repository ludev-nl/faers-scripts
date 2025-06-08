-- Standardizing terms for COMBINED_REAC and COMBINED_INDI and adding MedDRA numerical codes.

-- Drop and Create Tables with COPY Commands
DROP TABLE IF EXISTS low_level_term;
CREATE TABLE IF NOT EXISTS low_level_term (
    llt_code BIGINT,llt_name VARCHAR(100),pt_code CHAR(8),llt_whoart_code CHAR(7),llt_harts_code BIGINT,llt_costart_sym VARCHAR(21),llt_icd9_code CHAR(8),llt_icd9cm_code CHAR(8),llt_icd10_code CHAR(8),llt_jart_code CHAR(8)
);
COPY low_level_term FROM '../faers-data/MedDRA_25_1_English/MedAscii/llt.asc'
WITH (FORMAT CSV, DELIMITER '$', HEADER false);

-- Repeat for other tables using the same pattern
DROP TABLE IF EXISTS pref_term;
CREATE TABLE IF NOT EXISTS pref_term (
    pt_code BIGINT, pt_name VARCHAR(100),null_field CHAR(1),pt_soc_code BIGINT,pt_whoart_code CHAR(7),pt_harts_code BIGINT,pt_costart_sym CHAR(21),pt_icd9_code CHAR(8),pt_icd9cm_code CHAR(8),pt_icd10_code CHAR(8),pt_jart_code CHAR(8)
);
COPY pref_term FROM '../faers-data/MedDRA_25_1_English/MedAscii/pt.asc'
WITH (FORMAT CSV, DELIMITER '$', HEADER false);

-- Translate hlt_pref_term
DROP TABLE IF EXISTS hlt_pref_term;
CREATE TABLE IF NOT EXISTS hlt_pref_term (
    hlt_code BIGINT, hlt_name VARCHAR(100), hlt_whoart_code CHAR(7), hlt_harts_code BIGINT, hlt_costart_sym VARCHAR(21),hlt_icd9_code CHAR(8), hlt_icd9cm_code CHAR(8), hlt_icd10_code CHAR(8), hlt_jart_code CHAR(6)
);
COPY hlt_pref_term FROM '../faers-data/MedDRA_25_1_English/MedAscii/hlt.asc'
WITH (FORMAT CSV, DELIMITER '$', HEADER false);

-- Translate hlt_pref_comp
DROP TABLE IF EXISTS hlt_pref_comp;
CREATE TABLE IF NOT EXISTS hlt_pref_comp (
    hlt_code BIGINT, pt_code BIGINT
);
COPY hlt_pref_comp FROM '../faers-data/MedDRA_25_1_English/MedAscii/hlt_pt.asc'
WITH (FORMAT CSV, DELIMITER '$', HEADER false);

-- Translate hlgt_pref_term
DROP TABLE IF EXISTS hlgt_pref_term;
CREATE TABLE IF NOT EXISTS hlgt_pref_term (
    hlgt_code BIGINT, hlgt_name VARCHAR(100), hlgt_whoart_code CHAR(7), hlgt_harts_code BIGINT, hlgt_costart_sym VARCHAR(21),hlgt_icd9_code CHAR(8), hlgt_icd9cm_code CHAR(8), hlgt_icd10_code CHAR(8), hlgt_jart_code CHAR(6)
);
COPY hlgt_pref_term FROM '../faers-data/MedDRA_25_1_English/MedAscii/hlgt.asc'
WITH (FORMAT CSV, DELIMITER '$', HEADER false);

-- Translate hlgt_hlt_comp
DROP TABLE IF EXISTS hlgt_hlt_comp;
CREATE TABLE IF NOT EXISTS hlgt_hlt_comp (
    hlgt_code BIGINT, hlt_code BIGINT
);
COPY hlgt_hlt_comp FROM '../faers-data/MedDRA_25_1_English/MedAscii/hlgt_hlt.asc'
WITH (FORMAT CSV, DELIMITER '$', HEADER false);

-- Translate soc_term
DROP TABLE IF EXISTS soc_term;
CREATE TABLE IF NOT EXISTS soc_term (
    soc_code BIGINT, soc_name VARCHAR(100), soc_abbrev VARCHAR(5), soc_whoart_code CHAR(7), soc_harts_code BIGINT,soc_costart_sym VARCHAR(21), soc_icd9_code CHAR(8), soc_icd9cm_code CHAR(8), soc_icd10_code CHAR(8), soc_jart_code CHAR(6)
);
COPY soc_term FROM '../faers-data/MedDRA_25_1_English/MedAscii/soc.asc'
WITH (FORMAT CSV, DELIMITER '$', HEADER false);

-- Translate soc_hlgt_comp
DROP TABLE IF EXISTS soc_hlgt_comp;
CREATE TABLE IF NOT EXISTS soc_hlgt_comp (
    soc_code BIGINT, hlgt_code BIGINT
);
COPY soc_hlgt_comp FROM '../faers-data/MedDRA_25_1_English/MedAscii/soc_hlgt.asc'
WITH (FORMAT CSV, DELIMITER '$', HEADER false);

-- Translate md_hierarchy
DROP TABLE IF EXISTS md_hierarchy;
CREATE TABLE IF NOT EXISTS md_hierarchy (
    pt_code BIGINT, hlt_code BIGINT, hlgt_code BIGINT, soc_code BIGINT, pt_name VARCHAR(100),hlt_name VARCHAR(100), hlgt_name VARCHAR(100), soc_name VARCHAR(100), soc_abbrev VARCHAR(5),null_field CHAR(1), pt_soc_code BIGINT, primary_soc_fg CHAR(1)
);
COPY md_hierarchy FROM '../faers-data/MedDRA_25_1_English/MedAscii/mdhier.asc'
WITH (FORMAT CSV, DELIMITER '$', HEADER false);

-- Translate soc_intl_order
DROP TABLE IF EXISTS soc_intl_order;
CREATE TABLE IF NOT EXISTS soc_intl_order (
    intl_ord_code BIGINT, soc_code BIGINT
);
COPY soc_intl_order FROM '../faers-data/MedDRA_25_1_English/MedAscii/intl_ord.asc'
WITH (FORMAT CSV, DELIMITER '$', HEADER false);

-- Translate smq_list
DROP TABLE IF EXISTS smq_list;
CREATE TABLE IF NOT EXISTS smq_list (
    smq_code BIGINT, smq_name VARCHAR(100), smq_level INT, smq_description TEXT, smq_source VARCHAR(2000),smq_note VARCHAR(2000), meddra_version CHAR(5), status CHAR(1), smq_algorithm VARCHAR(2000)
);
COPY smq_list FROM '../faers-data/MedDRA_25_1_English/MedAscii/SMQ_List.asc'
WITH (FORMAT CSV, DELIMITER '$', HEADER false);

-- Translate smq_Content
DROP TABLE IF EXISTS smq_content;
CREATE TABLE IF NOT EXISTS smq_content (
    smq_code BIGINT, term_code BIGINT, term_level INT, term_scope INT, term_category CHAR(1), term_weight INT,term_status CHAR(1), term_addition_version CHAR(5), term_last_modified CHAR(5)
);
COPY smq_content FROM '../faers-data/MedDRA_25_1_English/MedAscii/SMQ_Content.asc'
WITH (FORMAT CSV, DELIMITER '$', HEADER false);

-- Create tables for MedDRA mappings
DROP TABLE IF EXISTS indi_medra_mappings;
CREATE TABLE IF NOT EXISTS indi_medra_mappings (
    term_name TEXT PRIMARY KEY, -- The term name
    meddra_code TEXT            -- A single associated MedDRA code stored as a string
);

DROP TABLE IF EXISTS reac_medra_mappings;
CREATE TABLE IF NOT EXISTS reac_medra_mappings (
    term_name TEXT PRIMARY KEY, -- The term name
    meddra_code TEXT            -- A single associated MedDRA code stored as a string
);

-- Load the updated JSON files into the mapping tables
COPY indi_medra_mappings(term_name, meddra_code) FROM '../faers-data/INDI_medra_mappings.json' WITH (FORMAT json);
COPY reac_medra_mappings(term_name, meddra_code) FROM '../faers-data/REAC_medra_mappings.json' WITH (FORMAT json);

-- INDI_Combined Adjustments
ALTER TABLE IF EXISTS INDI_Combined ADD COLUMN IF NOT EXISTS meddra_code TEXT;
ALTER TABLE IF EXISTS INDI_Combined ADD COLUMN IF NOT EXISTS cleaned_pt VARCHAR(100);

-- Remove white spaces in the cleaned_pt column
UPDATE INDI_Combined
SET cleaned_pt = UPPER(TRIM(BOTH FROM REPLACE(REPLACE(REPLACE(indi_pt, E'\n', ''), E'\r', ''), E'\t', '')));

-- Update meddra_code using pref_term and low_level_term
UPDATE INDI_Combined
SET meddra_code = b.pt_code::TEXT
FROM pref_term b
WHERE INDI_Combined.cleaned_pt = b.pt_name AND meddra_code IS NULL;

UPDATE INDI_Combined
SET meddra_code = b.llt_code::TEXT
FROM low_level_term b
WHERE INDI_Combined.cleaned_pt = b.llt_name AND meddra_code IS NULL;

-- Update meddra_code using INDI_medra_mappings
UPDATE INDI_Combined
SET meddra_code = m.meddra_code
FROM indi_medra_mappings m
WHERE INDI_Combined.cleaned_pt = m.term_name
AND meddra_code IS NULL;

-- Create index on meddra_code
CREATE INDEX IF NOT EXISTS indi_meddra_code_idx ON INDI_Combined (meddra_code);

-- REAC_Combined Adjustments
ALTER TABLE IF EXISTS REAC_Combined ADD COLUMN IF NOT EXISTS meddra_code TEXT;

-- Update meddra_code using pref_term and low_level_term
UPDATE REAC_Combined
SET meddra_code = b.pt_code::TEXT
FROM pref_term b
WHERE REAC_Combined.pt = b.pt_name AND meddra_code IS NULL;

UPDATE REAC_Combined
SET meddra_code = b.llt_code::TEXT
FROM low_level_term b
WHERE REAC_Combined.pt = b.llt_name AND meddra_code IS NULL;

-- Update meddra_code using REAC_medra_mappings
UPDATE REAC_Combined
SET meddra_code = m.meddra_code
FROM reac_medra_mappings m
WHERE REAC_Combined.pt = m.term_name
AND meddra_code IS NULL;

-- Create index on meddra_code for REAC_Combined
CREATE INDEX IF NOT EXISTS reac_meddra_code_idx ON REAC_Combined (meddra_code);

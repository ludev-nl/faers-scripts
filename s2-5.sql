-- s2-5.sql: Create combined tables in faers_combined schema, skipping if they exist

-- Set session parameters
SET search_path TO faers_combined, faers_a, public;
SET work_mem = '256MB';
SET statement_timeout = '600s';
SET client_min_messages TO NOTICE;

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS faers_combined;

-- Create DEMO_Combined
CREATE TABLE IF NOT EXISTS faers_combined."DEMO_Combined" (
    "DEMO_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    primaryid BIGINT,
    caseid BIGINT,
    caseversion INTEGER,
    i_f_cod VARCHAR(3),
    event_dt VARCHAR(20),
    mfr_dt VARCHAR(20),
    init_fda_dt VARCHAR(20),
    fda_dt VARCHAR(20),
    rept_cod VARCHAR(10),
    auth_num VARCHAR(100),
    mfr_num VARCHAR(100),
    mfr_sndr VARCHAR(100),
    lit_ref TEXT,
    age VARCHAR(28),
    age_cod VARCHAR(3),
    age_grp VARCHAR(5),
    gndr_cod VARCHAR(3),
    e_sub VARCHAR(1),
    wt VARCHAR(25),
    wt_cod VARCHAR(20),
    rept_dt VARCHAR(20),
    to_mfr VARCHAR(10),
    occp_cod VARCHAR(10),
    reporter_country VARCHAR(100),
    occr_country VARCHAR(20),
    "PERIOD" VARCHAR(10)
);

-- Create DRUG_Combined
CREATE TABLE IF NOT EXISTS faers_combined."DRUG_Combined" (
    "DRUG_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    primaryid BIGINT,
    caseid BIGINT,
    drug_seq BIGINT,
    role_cod VARCHAR(2),
    drugname TEXT,
    prod_ai TEXT,
    val_vbm INTEGER,
    route VARCHAR(70),
    dose_vbm TEXT,
    cum_dose_chr FLOAT,
    cum_dose_unit VARCHAR(8),
    dechal VARCHAR(2),
    rechal VARCHAR(2),
    lot_num TEXT,
    exp_dt VARCHAR(200),
    nda_num VARCHAR(200),
    dose_amt VARCHAR(15),
    dose_unit VARCHAR(20),
    dose_form VARCHAR(100),
    dose_freq VARCHAR(20),
    "PERIOD" VARCHAR(10)
);

-- Create INDI_Combined
CREATE TABLE IF NOT EXISTS faers_combined."INDI_Combined" (
    "INDI_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    primaryid BIGINT,
    caseid BIGINT,
    indi_drug_seq BIGINT,
    indi_pt TEXT,
    "PERIOD" VARCHAR(10)
);

-- Create THER_Combined
CREATE TABLE IF NOT EXISTS faers_combined."THER_Combined" (
    "THER_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    primaryid BIGINT,
    caseid BIGINT,
    dsg_drug_seq BIGINT,
    start_dt BIGINT,
    end_dt BIGINT,
    dur VARCHAR(50),
    dur_cod VARCHAR(50),
    "PERIOD" VARCHAR(10)
);

-- Create REAC_Combined
CREATE TABLE IF NOT EXISTS faers_combined."REAC_Combined" (
    primaryid BIGINT,
    caseid BIGINT,
    pt VARCHAR(100),
    drug_rec_act VARCHAR(100),
    "PERIOD" VARCHAR(10)
);

-- Create RPSR_Combined
CREATE TABLE IF NOT EXISTS faers_combined."RPSR_Combined" (
    "RPSR_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    primaryid BIGINT,
    caseid BIGINT,
    rpsr_cod VARCHAR(100),
    "PERIOD" VARCHAR(10)
);

-- Create OUTC_Combined
CREATE TABLE IF NOT EXISTS faers_combined."OUTC_Combined" (
    "OUTC_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    primaryid BIGINT,
    caseid BIGINT,
    outc_cod VARCHAR(20),
    "PERIOD" VARCHAR(10)
);

-- Create COMBINED_DELETED_CASES_REPORTS
CREATE TABLE IF NOT EXISTS faers_combined."COMBINED_DELETED_CASES" (
    "Field1" TEXT
);

-- Create indexes
DO $$  
BEGIN
    CREATE INDEX IF NOT EXISTS idx_demo_combined ON faers_combined."DEMO_Combined" (primaryid);
    CREATE INDEX IF NOT EXISTS idx_drug_combined ON faers_combined."DRUG_Combined" (primaryid);
    CREATE INDEX IF NOT EXISTS idx_indi_combined ON faers_combined."INDI_Combined" (primaryid);
    CREATE INDEX IF NOT EXISTS idx_ther_combined ON faers_combined."THER_Combined" (primaryid);
    CREATE INDEX IF NOT EXISTS idx_reac_combined ON faers_combined."REAC_Combined" (primaryid);
    CREATE INDEX IF NOT EXISTS idx_rpsr_combined ON faers_combined."RPSR_Combined" (primaryid);
    CREATE INDEX IF NOT EXISTS idx_outc_combined ON faers_combined."OUTC_Combined" (primaryid);
    RAISE NOTICE 'Indexes created successfully';
END $$;

-- Log table status
DO $$  
DECLARE
    table_name TEXT;
    row_count BIGINT;
BEGIN
    FOR table_name IN (
        SELECT unnest(ARRAY[
            'DEMO_Combined', 'DRUG_Combined', 'INDI_Combined', 'THER_Combined',
            'REAC_Combined', 'RPSR_Combined', 'OUTC_Combined', 'COMBINED_DELETED_CASES'
        ])
    ) LOOP
        EXECUTE format('SELECT COUNT(*) FROM faers_combined.%I', table_name) INTO row_count;
        IF row_count = 0 THEN
            RAISE NOTICE 'Table faers_combined.% is empty', table_name;
        ELSE
            RAISE NOTICE 'Table faers_combined.% has % rows', table_name, row_count;
        END IF;
    END LOOP;
END $$;

-- s2-5.sql: Create and populate combined tables in faers_combined schema

-- Set session parameters
SET search_path TO faers_combined, faers_a, public;
SET work_mem = '256MB';
SET statement_timeout = '600s';
SET client_min_messages TO NOTICE;

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS faers_combined;

-- Create tables
CREATE TABLE IF NOT EXISTS faers_combined."DEMO_Combined" (
    "DEMO_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    primaryid BIGINT,
    caseid BIGINT,
    caseversion TEXT,
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
    sex VARCHAR(3),
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

CREATE TABLE IF NOT EXISTS faers_combined."INDI_Combined" (
    "INDI_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    primaryid BIGINT,
    caseid BIGINT,
    indi_drug_seq BIGINT,
    indi_pt TEXT,
    "PERIOD" VARCHAR(10)
);

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

CREATE TABLE IF NOT EXISTS faers_combined."REAC_Combined" (
    primaryid BIGINT,
    caseid BIGINT,
    pt VARCHAR(100),
    drug_rec_act VARCHAR(100),
    "PERIOD" VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS faers_combined."RPSR_Combined" (
    "RPSR_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    primaryid BIGINT,
    caseid BIGINT,
    rpsr_cod VARCHAR(100),
    "PERIOD" VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS faers_combined."OUTC_Combined" (
    "OUTC_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    primaryid BIGINT,
    caseid BIGINT,
    outc_cod VARCHAR(20),
    "PERIOD" VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS faers_combined."COMBINED_DELETED_CASES" (
    "Field1" TEXT
);

-- Populate combined tables using get_completed_year_quarters
DO $$
DECLARE
    rec RECORD;
    table_prefixes TEXT[] := ARRAY['demo', 'drug', 'indi', 'ther', 'reac', 'rpsr', 'outc'];
    table_prefix TEXT;
    table_name TEXT;
    combined_table TEXT;
    sql_text TEXT;
    year INT;
    quarter INT;
BEGIN
    FOR rec IN SELECT year, quarter FROM faers_a.get_completed_year_quarters(4)
    LOOP
        year := rec.year;
        quarter := rec.quarter;
        FOR table_prefix IN SELECT unnest(table_prefixes)
        LOOP
            table_name := format('faers_a.%s%02dq%s', table_prefix, year % 100, quarter);
            combined_table := format('faers_combined."%s_Combined"', initcap(table_prefix));

            IF table_prefix = 'demo' THEN
                sql_text := format('
                    INSERT INTO %s (
                        primaryid, caseid, caseversion, i_f_cod, event_dt, mfr_dt, init_fda_dt, fda_dt,
                        rept_cod, auth_num, mfr_num, mfr_sndr, lit_ref, age, age_cod, age_grp, sex,
                        e_sub, wt, wt_cod, rept_dt, to_mfr, occp_cod, reporter_country, occr_country, "PERIOD"
                    )
                    SELECT
                        primaryid::BIGINT, caseid::BIGINT, NULLIF(caseversion, '''') AS caseversion, i_f_cod, event_dt, mfr_dt, init_fda_dt, fda_dt,
                        rept_cod,
                        CASE WHEN %s >= 2014 AND (%s > 2014 OR %s >= 3) THEN auth_num ELSE NULL END AS auth_num,
                        mfr_num, mfr_sndr,
                        CASE WHEN %s >= 2014 AND (%s > 2014 OR %s >= 3) THEN lit_ref ELSE NULL END AS lit_ref,
                        age, age_cod, age_grp,
                        COALESCE(sex, gndr_cod) AS sex,
                        e_sub, wt, wt_cod, rept_dt, to_mfr, occp_cod, reporter_country, occr_country,
                        %L AS "PERIOD"
                    FROM %s
                    ON CONFLICT DO NOTHING;
                ', combined_table, year, year, quarter, year, year, quarter, format('%sq%s', year, quarter), table_name);
            ELSIF table_prefix = 'drug' THEN
                sql_text := format('
                    INSERT INTO %s (
                        primaryid, caseid, drug_seq, role_cod, drugname, prod_ai, val_vbm, route, dose_vbm,
                        cum_dose_chr, cum_dose_unit, dechal, rechal, lot_num, exp_dt, nda_num, dose_amt,
                        dose_unit, dose_form, dose_freq, "PERIOD"
                    )
                    SELECT
                        primaryid, caseid, drug_seq, role_cod, drugname, prod_ai, val_vbm, route, dose_vbm,
                        cum_dose_chr, cum_dose_unit, dechal, rechal, lot_num, exp_dt, nda_num, dose_amt,
                        dose_unit, dose_form, dose_freq,
                        %L AS "PERIOD"
                    FROM %s
                    ON CONFLICT DO NOTHING;
                ', combined_table, format('%sq%s', year, quarter), table_name);
            ELSIF table_prefix = 'indi' THEN
                sql_text := format('
                    INSERT INTO %s (
                        primaryid, caseid, indi_drug_seq, indi_pt, "PERIOD"
                    )
                    SELECT
                        primaryid, caseid, indi_drug_seq, indi_pt,
                        %L AS "PERIOD"
                    FROM %s
                    ON CONFLICT DO NOTHING;
                ', combined_table, format('%sq%s', year, quarter), table_name);
            ELSIF table_prefix = 'ther' THEN
                sql_text := format('
                    INSERT INTO %s (
                        primaryid, caseid, dsg_drug_seq, start_dt, end_dt, dur, dur_cod, "PERIOD"
                    )
                    SELECT
                        primaryid, caseid, dsg_drug_seq, start_dt, end_dt, dur, dur_cod,
                        %L AS "PERIOD"
                    FROM %s
                    ON CONFLICT DO NOTHING;
                ', combined_table, format('%sq%s', year, quarter), table_name);
            ELSIF table_prefix = 'reac' THEN
                sql_text := format('
                    INSERT INTO %s (
                        primaryid, caseid, pt, drug_rec_act, "PERIOD"
                    )
                    SELECT
                        primaryid, caseid, pt, drug_rec_act,
                        %L AS "PERIOD"
                    FROM %s
                    ON CONFLICT DO NOTHING;
                ', combined_table, format('%sq%s', year, quarter), table_name);
            ELSIF table_prefix = 'rpsr' THEN
                sql_text := format('
                    INSERT INTO %s (
                        primaryid, caseid, rpsr_cod, "PERIOD"
                    )
                    SELECT
                        primaryid, caseid, rpsr_cod,
                        %L AS "PERIOD"
                    FROM %s
                    ON CONFLICT DO NOTHING;
                ', combined_table, format('%sq%s', year, quarter), table_name);
            ELSIF table_prefix = 'outc' THEN
                sql_text := format('
                    INSERT INTO %s (
                        primaryid, caseid, outc_cod, "PERIOD"
                    )
                    SELECT
                        primaryid, caseid, outc_cod,
                        %L AS "PERIOD"
                    FROM %s
                    ON CONFLICT DO NOTHING;
                ', combined_table, format('%sq%s', year, quarter), table_name);
            END IF;

            BEGIN
                EXECUTE sql_text;
                RAISE NOTICE 'Inserted data into % from %', combined_table, table_name;
            EXCEPTION WHEN OTHERS THEN
                RAISE WARNING 'Error inserting data into % from %: %', combined_table, table_name, SQLERRM;
                CONTINUE; -- Continue to the next table
            END;
        END LOOP;

        table_name := format('faers_a.dele%02dq%s', year % 100, quarter);
        sql_text := format('
            INSERT INTO faers_combined."COMBINED_DELETED_CASES" ("Field1")
            SELECT "Field1"
            FROM %s
            ON CONFLICT DO NOTHING;
        ', table_name);
        BEGIN
            EXECUTE sql_text;
            RAISE NOTICE 'Inserted data into faers_combined.COMBINED_DELETED_CASES from %', table_name;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Error inserting data into faers_combined.COMBINED_DELETED_CASES from %: %', table_name, SQLERRM;
        END;
    END LOOP;
END $$;

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
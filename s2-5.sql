-- s2-5.sql: Merge FAERS quarterly data into combined tables in faers_combined schema

-- Set session parameters
SET search_path TO faers_combined, faers_a, public;
SET work_mem = '256MB';
SET statement_timeout = '600s';

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS faers_combined;

-- Function to check if a column exists in a table
CREATE OR REPLACE FUNCTION column_exists(p_schema TEXT, p_table TEXT, p_column TEXT)
RETURNS BOOLEAN AS $$  
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = p_schema
        AND table_name = p_table
        AND column_name = LOWER(p_column)
    ) THEN
        RAISE NOTICE 'Column % not found in %.%', p_column, p_schema, p_table;
        RETURN FALSE;
    END IF;
    RETURN TRUE;
END;
  $$ LANGUAGE plpgsql;

-- Function to determine completed year-quarter combinations (fixed to start from 2004)
CREATE OR REPLACE FUNCTION get_completed_year_quarters(start_year INT DEFAULT 2004)
RETURNS TABLE (year INT, quarter INT)
AS $func$
DECLARE
    current_year INT;
    current_quarter INT;
    last_year INT;
    last_quarter INT;
    y INT;
    q INT;
BEGIN
    -- Get current year and quarter
    SELECT EXTRACT(YEAR FROM CURRENT_DATE)::INT INTO current_year;
    SELECT EXTRACT(QUARTER FROM CURRENT_DATE)::INT INTO current_quarter;
   
    -- Determine last completed quarter
    IF current_quarter = 1 THEN
        last_year := current_year - 1;
        last_quarter := 4;
    ELSE
        last_year := current_year;
        last_quarter := current_quarter - 1;
    END IF;

    -- Generate year-quarter pairs from start_year (absolute year, e.g., 2004)
    y := GREATEST(start_year, 2004); -- Ensure no years before 2004
    WHILE y <= last_year LOOP
        q := 1;
        WHILE q <= 4 LOOP
            IF y = last_year AND q > last_quarter THEN
                EXIT;
            END IF;
            year := y;
            quarter := q;
            RETURN NEXT;
            q := q + 1;
        END LOOP;
        y := y + 1;
    END LOOP;
END;
$func$ LANGUAGE plpgsql;

-- Create DEMO_Combined (permissive for all periods)
DROP TABLE IF EXISTS faers_combined."DEMO_Combined";
CREATE TABLE faers_combined."DEMO_Combined" (
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
DROP TABLE IF EXISTS faers_combined."DRUG_Combined";
CREATE TABLE faers_combined."DRUG_Combined" (
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
DROP TABLE IF EXISTS faers_combined."INDI_Combined";
CREATE TABLE faers_combined."INDI_Combined" (
    "INDI_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    primaryid BIGINT,
    caseid BIGINT,
    indi_drug_seq BIGINT,
    indi_pt TEXT,
    "PERIOD" VARCHAR(10)
);

-- Create THER_Combined
DROP TABLE IF EXISTS faers_combined."THER_Combined";
CREATE TABLE faers_combined."THER_Combined" (
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
DROP TABLE IF EXISTS faers_combined."REAC_Combined";
CREATE TABLE faers_combined."REAC_Combined" (
    primaryid BIGINT,
    caseid BIGINT,
    pt VARCHAR(100),
    drug_rec_act VARCHAR(100),
    "PERIOD" VARCHAR(10)
);

-- Create RPSR_Combined
DROP TABLE IF EXISTS faers_combined."RPSR_Combined";
CREATE TABLE faers_combined."RPSR_Combined" (
    "RPSR_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    primaryid BIGINT,
    caseid BIGINT,
    rpsr_cod VARCHAR(100),
    "PERIOD" VARCHAR(10)
);

-- Create OUTC_Combined
DROP TABLE IF EXISTS faers_combined."OUTC_Combined";
CREATE TABLE faers_combined."OUTC_Combined" (
    "OUTC_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    primaryid BIGINT,
    caseid BIGINT,
    outc_cod VARCHAR(20),
    "PERIOD" VARCHAR(10)
);

-- Create COMBINED_DELETED_CASES_REPORTS (placeholder)
DROP TABLE IF EXISTS faers_combined."COMBINED_DELETED_CASES_REPORTS";
CREATE TABLE faers_combined."COMBINED_DELETED_CASES_REPORTS" (
    "Field1" BIGINT
);

-- Merge data using dynamic SQL with robust error handling
DO $$  
DECLARE
    rec RECORD;
    rec_temp RECORD;
    tbl_name TEXT;
    schema_name TEXT := 'faers_a';
    row_count INTEGER;
    skipped_rows INTEGER;
BEGIN
    -- DEMO_Combined
    FOR rec IN (
        SELECT year, quarter
        FROM get_completed_year_quarters(2004)
    ) LOOP
        tbl_name := 'faers_a.demo' || RIGHT(rec.year::text, 2) || 'q' || rec.quarter;
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.tables t
            WHERE t.table_schema = 'faers_a'
            AND t.table_name = 'demo' || RIGHT(rec.year::text, 2) || 'q' || rec.quarter
        ) THEN
            RAISE NOTICE 'Table % does not exist, skipping for period %', tbl_name, rec.year || 'Q' || rec.quarter;
            CONTINUE;
        END IF;

        RAISE NOTICE 'Processing table % for period %', tbl_name, rec.year || 'Q' || rec.quarter;
       
        -- Reset counters
        row_count := 0;
        skipped_rows := 0;

        -- Create temp table to stage data
        EXECUTE format('
            CREATE TEMP TABLE temp_demo AS
            SELECT
                %s AS primaryid,
                %s AS caseid,
                %s AS caseversion,
                %s AS i_f_cod,
                %s AS event_dt,
                %s AS mfr_dt,
                %s AS init_fda_dt,
                %s AS fda_dt,
                %s AS rept_cod,
                %s AS auth_num,
                %s AS mfr_num,
                %s AS mfr_sndr,
                %s AS lit_ref,
                %s AS age,
                %s AS age_cod,
                %s AS age_grp,
                %s AS gndr_cod,
                %s AS e_sub,
                %s AS wt,
                %s AS wt_cod,
                %s AS rept_dt,
                %s AS to_mfr,
                %s AS occp_cod,
                %s AS reporter_country,
                %s AS occr_country,
                %L AS "PERIOD"
            FROM %s',
            CASE WHEN rec.year < 2012 OR (rec.year = 2012 AND rec.quarter <= 3)
                 THEN (CASE WHEN column_exists(schema_name, tbl_name, 'isr') THEN 'isr' ELSE 'NULL' END)
                 ELSE (CASE WHEN column_exists(schema_name, tbl_name, 'primaryid') THEN 'primaryid' ELSE 'NULL' END) END,
            CASE WHEN rec.year < 2012 OR (rec.year = 2012 AND rec.quarter <= 3)
                 THEN (CASE WHEN column_exists(schema_name, tbl_name, 'case') THEN '"case"' ELSE 'NULL' END)
                 ELSE (CASE WHEN column_exists(schema_name, tbl_name, 'caseid') THEN 'caseid' ELSE 'NULL' END) END,
            CASE WHEN column_exists(schema_name, tbl_name, 'caseversion') THEN 'caseversion' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'i_f_cod') THEN '"i_f_cod"' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'event_dt') THEN 'event_dt' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'mfr_dt') THEN 'mfr_dt' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'init_fda_dt') THEN 'init_fda_dt' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'fda_dt') THEN 'fda_dt' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'rept_cod') THEN 'rept_cod' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'auth_num') THEN 'auth_num' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'mfr_num') THEN 'mfr_num' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'mfr_sndr') THEN 'mfr_sndr' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'lit_ref') THEN 'lit_ref' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'age') THEN 'age' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'age_cod') THEN 'age_cod' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'age_grp') THEN 'age_grp' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'gndr_cod') THEN 'gndr_cod' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'e_sub') THEN 'e_sub' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'wt') THEN 'wt' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'wt_cod') THEN 'wt_cod' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'rept_dt') THEN 'rept_dt' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'to_mfr') THEN 'to_mfr' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'occp_cod') THEN 'occp_cod' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'reporter_country') THEN 'reporter_country' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'occr_country') THEN 'occr_country' ELSE 'NULL' END,
            rec.year || 'Q' || rec.quarter, tbl_name);

        -- Insert from temp table with error handling
        BEGIN
            FOR rec_temp IN (SELECT * FROM temp_demo) LOOP
                BEGIN
                    INSERT INTO faers_combined."DEMO_Combined" (
                        primaryid, caseid, caseversion, i_f_cod, event_dt, mfr_dt, init_fda_dt, fda_dt,
                        rept_cod, auth_num, mfr_num, mfr_sndr, lit_ref, age, age_cod, age_grp, gndr_cod,
                        e_sub, wt, wt_cod, rept_dt, to_mfr, occp_cod, reporter_country, occr_country, "PERIOD"
                    )
                    VALUES (
                        rec_temp.primaryid, rec_temp.caseid, rec_temp.caseversion, rec_temp.i_f_cod, rec_temp.event_dt,
                        rec_temp.mfr_dt, rec_temp.init_fda_dt, rec_temp.fda_dt, rec_temp.rept_cod, rec_temp.auth_num,
                        rec_temp.mfr_num, rec_temp.mfr_sndr, rec_temp.lit_ref, rec_temp.age, rec_temp.age_cod,
                        rec_temp.age_grp, rec_temp.gndr_cod, rec_temp.e_sub, rec_temp.wt, rec_temp.wt_cod,
                        rec_temp.rept_dt, rec_temp.to_mfr, rec_temp.occp_cod, rec_temp.reporter_country,
                        rec_temp.occr_country, rec_temp."PERIOD"
                    );
                    row_count := row_count + 1;
                EXCEPTION WHEN OTHERS THEN
                    skipped_rows := skipped_rows + 1;
                    RAISE NOTICE 'Skipped row in table % for period % due to error: %', tbl_name, rec.year || 'Q' || rec.quarter, SQLERRM;
                    CONTINUE;
                END;
            END LOOP;
            RAISE NOTICE 'Processed % rows, skipped % rows for table % in period %', row_count, skipped_rows, tbl_name, rec.year || 'Q' || rec.quarter;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Error processing table % for period %: %', tbl_name, rec.year || 'Q' || rec.quarter, SQLERRM;
        END;

        DROP TABLE IF EXISTS temp_demo;
    END LOOP;

    -- DRUG_Combined
    FOR rec IN (
        SELECT year, quarter
        FROM get_completed_year_quarters(2004)
    ) LOOP
        tbl_name := 'faers_a.drug' || RIGHT(rec.year::text, 2) || 'q' || rec.quarter;
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.tables t
            WHERE t.table_schema = 'faers_a'
            AND t.table_name = 'drug' || RIGHT(rec.year::text, 2) || 'q' || rec.quarter
        ) THEN
            RAISE NOTICE 'Table % does not exist, skipping for period %', tbl_name, rec.year || 'Q' || rec.quarter;
            CONTINUE;
        END IF;

        RAISE NOTICE 'Processing table % for period %', tbl_name, rec.year || 'Q' || rec.quarter;

        row_count := 0;
        skipped_rows := 0;

        EXECUTE format('
            CREATE TEMP TABLE temp_drug AS
            SELECT
                %s AS primaryid,
                %s AS caseid,
                %s AS drug_seq,
                %s AS role_cod,
                %s AS drugname,
                %s AS prod_ai,
                %s AS val_vbm,
                %s AS route,
                %s AS dose_vbm,
                %s AS cum_dose_chr,
                %s AS cum_dose_unit,
                %s AS dechal,
                %s AS rechal,
                %s AS lot_num,
                %s AS exp_dt,
                %s AS nda_num,
                %s AS dose_amt,
                %s AS dose_unit,
                %s AS dose_form,
                %s AS dose_freq,
                %L AS "PERIOD"
            FROM %s',
            CASE WHEN rec.year < 2012 OR (rec.year = 2012 AND rec.quarter <= 3)
                 THEN (CASE WHEN column_exists(schema_name, tbl_name, 'isr') THEN 'isr' ELSE 'NULL' END)
                 ELSE (CASE WHEN column_exists(schema_name, tbl_name, 'primaryid') THEN 'primaryid' ELSE 'NULL' END) END,
            CASE WHEN column_exists(schema_name, tbl_name, 'caseid') THEN 'caseid' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'drug_seq') THEN 'drug_seq' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'role_cod') THEN 'role_cod' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'drugname') THEN 'drugname' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'prod_ai') THEN 'prod_ai' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'val_vbm') THEN 'val_vbm' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'route') THEN 'route' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'dose_vbm') THEN 'dose_vbm' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'cum_dose_chr') THEN 'cum_dose_chr' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'cum_dose_unit') THEN 'cum_dose_unit' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'dechal') THEN 'dechal' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'rechal') THEN 'rechal' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'lot_num') THEN 'lot_num' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'exp_dt') THEN 'exp_dt' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'nda_num') THEN 'nda_num' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'dose_amt') THEN 'dose_amt' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'dose_unit') THEN 'dose_unit' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'dose_form') THEN 'dose_form' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'dose_freq') THEN 'dose_freq' ELSE 'NULL' END,
            rec.year || 'Q' || rec.quarter, tbl_name);

        BEGIN
            FOR rec_temp IN (SELECT * FROM temp_drug) LOOP
                BEGIN
                    INSERT INTO faers_combined."DRUG_Combined" (
                        primaryid, caseid, drug_seq, role_cod, drugname, prod_ai, val_vbm, route,
                        dose_vbm, cum_dose_chr, cum_dose_unit, dechal, rechal, lot_num, exp_dt,
                        nda_num, dose_amt, dose_unit, dose_form, dose_freq, "PERIOD"
                    )
                    VALUES (
                        rec_temp.primaryid, rec_temp.caseid, rec_temp.drug_seq, rec_temp.role_cod,
                        rec_temp.drugname, rec_temp.prod_ai, rec_temp.val_vbm, rec_temp.route,
                        rec_temp.dose_vbm, rec_temp.cum_dose_chr, rec_temp.cum_dose_unit, rec_temp.dechal,
                        rec_temp.rechal, rec_temp.lot_num, rec_temp.exp_dt, rec_temp.nda_num,
                        rec_temp.dose_amt, rec_temp.dose_unit, rec_temp.dose_form, rec_temp.dose_freq,
                        rec_temp."PERIOD"
                    );
                    row_count := row_count + 1;
                EXCEPTION WHEN OTHERS THEN
                    skipped_rows := skipped_rows + 1;
                    RAISE NOTICE 'Skipped row in table % for period % due to error: %', tbl_name, rec.year || 'Q' || rec.quarter, SQLERRM;
                    CONTINUE;
                END;
            END LOOP;
            RAISE NOTICE 'Processed % rows, skipped % rows for table % in period %', row_count, skipped_rows, tbl_name, rec.year || 'Q' || rec.quarter;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Error processing table % for period %: %', tbl_name, rec.year || 'Q' || rec.quarter, SQLERRM;
        END;

        DROP TABLE IF EXISTS temp_drug;
    END LOOP;

    -- INDI_Combined
    FOR rec IN (
        SELECT year, quarter
        FROM get_completed_year_quarters(2004)
    ) LOOP
        tbl_name := 'faers_a.indi' || RIGHT(rec.year::text, 2) || 'q' || rec.quarter;
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.tables t
            WHERE t.table_schema = 'faers_a'
            AND t.table_name = 'indi' || RIGHT(rec.year::text, 2) || 'q' || rec.quarter
        ) THEN
            RAISE NOTICE 'Table % does not exist, skipping for period %', tbl_name, rec.year || 'Q' || rec.quarter;
            CONTINUE;
        END IF;

        RAISE NOTICE 'Processing table % for period %', tbl_name, rec.year || 'Q' || rec.quarter;

        row_count := 0;
        skipped_rows := 0;

        EXECUTE format('
            CREATE TEMP TABLE temp_indi AS
            SELECT
                %s AS primaryid,
                %s AS caseid,
                %s AS indi_drug_seq,
                %s AS indi_pt,
                %L AS "PERIOD"
            FROM %s',
            CASE WHEN rec.year < 2012 OR (rec.year = 2012 AND rec.quarter <= 3)
                 THEN (CASE WHEN column_exists(schema_name, tbl_name, 'isr') THEN 'isr' ELSE 'NULL' END)
                 ELSE (CASE WHEN column_exists(schema_name, tbl_name, 'primaryid') THEN 'primaryid' ELSE 'NULL' END) END,
            CASE WHEN column_exists(schema_name, tbl_name, 'caseid') THEN 'caseid' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'indi_drug_seq') THEN 'indi_drug_seq' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'indi_pt') THEN 'indi_pt' ELSE 'NULL' END,
            rec.year || 'Q' || rec.quarter, tbl_name);

        BEGIN
            FOR rec_temp IN (SELECT * FROM temp_indi) LOOP
                BEGIN
                    INSERT INTO faers_combined."INDI_Combined" (
                        primaryid, caseid, indi_drug_seq, indi_pt, "PERIOD"
                    )
                    VALUES (
                        rec_temp.primaryid, rec_temp.caseid, rec_temp.indi_drug_seq, rec_temp.indi_pt,
                        rec_temp."PERIOD"
                    );
                    row_count := row_count + 1;
                EXCEPTION WHEN OTHERS THEN
                    skipped_rows := skipped_rows + 1;
                    RAISE NOTICE 'Skipped row in table % for period % due to error: %', tbl_name, rec.year || 'Q' || rec.quarter, SQLERRM;
                    CONTINUE;
                END;
            END LOOP;
            RAISE NOTICE 'Processed % rows, skipped % rows for table % in period %', row_count, skipped_rows, tbl_name, rec.year || 'Q' || rec.quarter;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Error processing table % for period %: %', tbl_name, rec.year || 'Q' || rec.quarter, SQLERRM;
        END;

        DROP TABLE IF EXISTS temp_indi;
    END LOOP;

    -- THER_Combined
    FOR rec IN (
        SELECT year, quarter
        FROM get_completed_year_quarters(2004)
    ) LOOP
        tbl_name := 'faers_a.ther' || RIGHT(rec.year::text, 2) || 'q' || rec.quarter;
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.tables t
            WHERE t.table_schema = 'faers_a'
            AND t.table_name = 'ther' || RIGHT(rec.year::text, 2) || 'q' || rec.quarter
        ) THEN
            RAISE NOTICE 'Table % does not exist, skipping for period %', tbl_name, rec.year || 'Q' || rec.quarter;
            CONTINUE;
        END IF;

        RAISE NOTICE 'Processing table % for period %', tbl_name, rec.year || 'Q' || rec.quarter;

        row_count := 0;
        skipped_rows := 0;

        EXECUTE format('
            CREATE TEMP TABLE temp_ther AS
            SELECT
                %s AS primaryid,
                %s AS caseid,
                %s AS dsg_drug_seq,
                %s AS start_dt,
                %s AS end_dt,
                %s AS dur,
                %s AS dur_cod,
                %L AS "PERIOD"
            FROM %s',
            CASE WHEN rec.year < 2012 OR (rec.year = 2012 AND rec.quarter <= 3)
                 THEN (CASE WHEN column_exists(schema_name, tbl_name, 'isr') THEN 'isr' ELSE 'NULL' END)
                 ELSE (CASE WHEN column_exists(schema_name, tbl_name, 'primaryid') THEN 'primaryid' ELSE 'NULL' END) END,
            CASE WHEN column_exists(schema_name, tbl_name, 'caseid') THEN 'caseid' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'dsg_drug_seq') THEN 'dsg_drug_seq' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'start_dt') THEN 'start_dt' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'end_dt') THEN 'end_dt' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'dur') THEN 'dur' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'dur_cod') THEN 'dur_cod' ELSE 'NULL' END,
            rec.year || 'Q' || rec.quarter, tbl_name);

        BEGIN
            FOR rec_temp IN (SELECT * FROM temp_ther) LOOP
                BEGIN
                    INSERT INTO faers_combined."THER_Combined" (
                        primaryid, caseid, dsg_drug_seq, start_dt, end_dt, dur, dur_cod, "PERIOD"
                    )
                    VALUES (
                        rec_temp.primaryid, rec_temp.caseid, rec_temp.dsg_drug_seq, rec_temp.start_dt,
                        rec_temp.end_dt, rec_temp.dur, rec_temp.dur_cod, rec_temp."PERIOD"
                    );
                    row_count := row_count + 1;
                EXCEPTION WHEN OTHERS THEN
                    skipped_rows := skipped_rows + 1;
                    RAISE NOTICE 'Skipped row in table % for period % due to error: %', tbl_name, rec.year || 'Q' || rec.quarter, SQLERRM;
                    CONTINUE;
                END;
            END LOOP;
            RAISE NOTICE 'Processed % rows, skipped % rows for table % in period %', row_count, skipped_rows, tbl_name, rec.year || 'Q' || rec.quarter;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Error processing table % for period %: %', tbl_name, rec.year || 'Q' || rec.quarter, SQLERRM;
        END;

        DROP TABLE IF EXISTS temp_ther;
    END LOOP;

    -- REAC_Combined
    FOR rec IN (
        SELECT year, quarter
        FROM get_completed_year_quarters(2004)
    ) LOOP
        tbl_name := 'faers_a.reac' || RIGHT(rec.year::text, 2) || 'q' || rec.quarter;
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.tables t
            WHERE t.table_schema = 'faers_a'
            AND t.table_name = 'reac' || RIGHT(rec.year::text, 2) || 'q' || rec.quarter
        ) THEN
            RAISE NOTICE 'Table % does not exist, skipping for period %', tbl_name, rec.year || 'Q' || rec.quarter;
            CONTINUE;
        END IF;

        RAISE NOTICE 'Processing table % for period %', tbl_name, rec.year || 'Q' || rec.quarter;

        row_count := 0;
        skipped_rows := 0;

        EXECUTE format('
            CREATE TEMP TABLE temp_reac AS
            SELECT
                %s AS primaryid,
                %s AS caseid,
                %s AS pt,
                %s AS drug_rec_act,
                %L AS "PERIOD"
            FROM %s',
            CASE WHEN rec.year < 2012 OR (rec.year = 2012 AND rec.quarter <= 3)
                 THEN (CASE WHEN column_exists(schema_name, tbl_name, 'isr') THEN 'isr' ELSE 'NULL' END)
                 ELSE (CASE WHEN column_exists(schema_name, tbl_name, 'primaryid') THEN 'primaryid' ELSE 'NULL' END) END,
            CASE WHEN column_exists(schema_name, tbl_name, 'caseid') THEN 'caseid' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'pt') THEN 'pt' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'drug_rec_act') THEN 'drug_rec_act' ELSE 'NULL' END,
            rec.year || 'Q' || rec.quarter, tbl_name);

        BEGIN
            FOR rec_temp IN (SELECT * FROM temp_reac) LOOP
                BEGIN
                    INSERT INTO faers_combined."REAC_Combined" (
                        primaryid, caseid, pt, drug_rec_act, "PERIOD"
                    )
                    VALUES (
                        rec_temp.primaryid, rec_temp.caseid, rec_temp.pt, rec_temp.drug_rec_act,
                        rec_temp."PERIOD"
                    );
                    row_count := row_count + 1;
                EXCEPTION WHEN OTHERS THEN
                    skipped_rows := skipped_rows + 1;
                    RAISE NOTICE 'Skipped row in table % for period % due to error: %', tbl_name, rec.year || 'Q' || rec.quarter, SQLERRM;
                    CONTINUE;
                END;
            END LOOP;
            RAISE NOTICE 'Processed % rows, skipped % rows for table % in period %', row_count, skipped_rows, tbl_name, rec.year || 'Q' || rec.quarter;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Error processing table % for period %: %', tbl_name, rec.year || 'Q' || rec.quarter, SQLERRM;
        END;

        DROP TABLE IF EXISTS temp_reac;
    END LOOP;

    -- RPSR_Combined
    FOR rec IN (
        SELECT year, quarter
        FROM get_completed_year_quarters(2004)
    ) LOOP
        tbl_name := 'faers_a.rpsr' || RIGHT(rec.year::text, 2) || 'q' || rec.quarter;
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.tables t
            WHERE t.table_schema = 'faers_a'
            AND t.table_name = 'rpsr' || RIGHT(rec.year::text, 2) || 'q' || rec.quarter
        ) THEN
            RAISE NOTICE 'Table % does not exist, skipping for period %', tbl_name, rec.year || 'Q' || rec.quarter;
            CONTINUE;
        END IF;

        RAISE NOTICE 'Processing table % for period %', tbl_name, rec.year || 'Q' || rec.quarter;

        row_count := 0;
        skipped_rows := 0;

        EXECUTE format('
            CREATE TEMP TABLE temp_rpsr AS
            SELECT
                %s AS primaryid,
                %s AS caseid,
                %s AS rpsr_cod,
                %L AS "PERIOD"
            FROM %s',
            CASE WHEN rec.year < 2012 OR (rec.year = 2012 AND rec.quarter <= 3)
                 THEN (CASE WHEN column_exists(schema_name, tbl_name, 'isr') THEN 'isr' ELSE 'NULL' END)
                 ELSE (CASE WHEN column_exists(schema_name, tbl_name, 'primaryid') THEN 'primaryid' ELSE 'NULL' END) END,
            CASE WHEN column_exists(schema_name, tbl_name, 'caseid') THEN 'caseid' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'rpsr_cod') THEN 'rpsr_cod' ELSE 'NULL' END,
            rec.year || 'Q' || rec.quarter, tbl_name);

        BEGIN
            FOR rec_temp IN (SELECT * FROM temp_rpsr) LOOP
                BEGIN
                    INSERT INTO faers_combined."RPSR_Combined" (
                        primaryid, caseid, rpsr_cod, "PERIOD"
                    )
                    VALUES (
                        rec_temp.primaryid, rec_temp.caseid, rec_temp.rpsr_cod, rec_temp."PERIOD"
                    );
                    row_count := row_count + 1;
                EXCEPTION WHEN OTHERS THEN
                    skipped_rows := skipped_rows + 1;
                    RAISE NOTICE 'Skipped row in table % for period % due to error: %', tbl_name, rec.year || 'Q' || rec.quarter, SQLERRM;
                    CONTINUE;
                END;
            END LOOP;
            RAISE NOTICE 'Processed % rows, skipped % rows for table % in period %', row_count, skipped_rows, tbl_name, rec.year || 'Q' || rec.quarter;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Error processing table % for period %: %', tbl_name, rec.year || 'Q' || rec.quarter, SQLERRM;
        END;

        DROP TABLE IF EXISTS temp_rpsr;
    END LOOP;

    -- OUTC_Combined
    FOR rec IN (
        SELECT year, quarter
        FROM get_completed_year_quarters(2004)
    ) LOOP
        tbl_name := 'faers_a.outc' || RIGHT(rec.year::text, 2) || 'q' || rec.quarter;
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.tables t
            WHERE t.table_schema = 'faers_a'
            AND t.table_name = 'outc' || RIGHT(rec.year::text, 2) || 'q' || rec.quarter
        ) THEN
            RAISE NOTICE 'Table % does not exist, skipping for period %', tbl_name, rec.year || 'Q' || rec.quarter;
            CONTINUE;
        END IF;

        RAISE NOTICE 'Processing table % for period %', tbl_name, rec.year || 'Q' || rec.quarter;

        row_count := 0;
        skipped_rows := 0;

        EXECUTE format('
            CREATE TEMP TABLE temp_outc AS
            SELECT
                %s AS primaryid,
                %s AS caseid,
                %s AS outc_cod,
                %L AS "PERIOD"
            FROM %s',
            CASE WHEN rec.year < 2012 OR (rec.year = 2012 AND rec.quarter <= 3)
                 THEN (CASE WHEN column_exists(schema_name, tbl_name, 'isr') THEN 'isr' ELSE 'NULL' END)
                 ELSE (CASE WHEN column_exists(schema_name, tbl_name, 'primaryid') THEN 'primaryid' ELSE 'NULL' END) END,
            CASE WHEN column_exists(schema_name, tbl_name, 'caseid') THEN 'caseid' ELSE 'NULL' END,
            CASE WHEN column_exists(schema_name, tbl_name, 'outc_cod') THEN 'outc_cod' ELSE 'NULL' END,
            rec.year || 'Q' || rec.quarter, tbl_name);

        BEGIN
            FOR rec_temp IN (SELECT * FROM temp_outc) LOOP
                BEGIN
                    INSERT INTO faers_combined."OUTC_Combined" (
                        primaryid, caseid, outc_cod, "PERIOD"
                    )
                    VALUES (
                        rec_temp.primaryid, rec_temp.caseid, rec_temp.outc_cod, rec_temp."PERIOD"
                    );
                    row_count := row_count + 1;
                EXCEPTION WHEN OTHERS THEN
                    skipped_rows := skipped_rows + 1;
                    RAISE NOTICE 'Skipped row in table % for period % due to error: %', tbl_name, rec.year || 'Q' || rec.quarter, SQLERRM;
                    CONTINUE;
                END;
            END LOOP;
            RAISE NOTICE 'Processed % rows, skipped % rows for table % in period %', row_count, skipped_rows, tbl_name, rec.year || 'Q' || rec.quarter;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Error processing table % for period %: %', tbl_name, rec.year || 'Q' || rec.quarter, SQLERRM;
        END;

        DROP TABLE IF EXISTS temp_outc;
    END LOOP;
END   $$;

-- Clean data (remove $ and control characters)
DO $$  
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'faers_combined' AND table_name = 'RPSR_Combined') THEN
        UPDATE faers_combined."RPSR_Combined"
        SET rpsr_cod = TRIM(' $ ' FROM REPLACE(REPLACE(REPLACE(rpsr_cod, CHR(10), ''), CHR(13), ''), CHR(9), ''))
        WHERE rpsr_cod IS NOT NULL;
        RAISE NOTICE 'Cleaned RPSR_Combined';
    ELSE
        RAISE NOTICE 'Table faers_combined."RPSR_Combined" does not exist, skipping cleaning';
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'faers_combined' AND table_name = 'OUTC_Combined') THEN
        UPDATE faers_combined."OUTC_Combined"
        SET outc_cod = TRIM(' $ ' FROM REPLACE(REPLACE(REPLACE(outc_cod, CHR(10), ''), CHR(13), ''), CHR(9), ''))
        WHERE outc_cod IS NOT NULL;
        RAISE NOTICE 'Cleaned OUTC_Combined';
    ELSE
        RAISE NOTICE 'Table faers_combined."OUTC_Combined" does not exist, skipping cleaning';
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'faers_combined' AND table_name = 'REAC_Combined') THEN
        UPDATE faers_combined."REAC_Combined"
        SET pt = TRIM(' $ ' FROM REPLACE(REPLACE(REPLACE(pt, CHR(10), ''), CHR(13), ''), CHR(9), ''))
        WHERE pt IS NOT NULL;
        RAISE NOTICE 'Cleaned REAC_Combined';
    ELSE
        RAISE NOTICE 'Table faers_combined."REAC_Combined" does not exist, skipping cleaning';
    END IF;
END   $$;

-- Create indexes with error handling
DO $$  
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'faers_combined' AND table_name = 'DEMO_Combined') THEN
        CREATE INDEX IF NOT EXISTS idx_demo_combined_primaryid ON faers_combined."DEMO_Combined" (primaryid);
        RAISE NOTICE 'Created index idx_demo_combined_primaryid';
    ELSE
        RAISE NOTICE 'Table faers_combined."DEMO_Combined" does not exist, skipping index creation';
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'faers_combined' AND table_name = 'DRUG_Combined') THEN
        CREATE INDEX IF NOT EXISTS idx_drug_combined_primaryid ON faers_combined."DRUG_Combined" (primaryid);
        RAISE NOTICE 'Created index idx_drug_combined_primaryid';
    ELSE
        RAISE NOTICE 'Table faers_combined."DRUG_Combined" does not exist, skipping index creation';
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'faers_combined' AND table_name = 'INDI_Combined') THEN
        CREATE INDEX IF NOT EXISTS idx_indi_combined_primaryid ON faers_combined."INDI_Combined" (primaryid);
        RAISE NOTICE 'Created index idx_indi_combined_primaryid';
    ELSE
        RAISE NOTICE 'Table faers_combined."INDI_Combined" does not exist, skipping index creation';
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'faers_combined' AND table_name = 'THER_Combined') THEN
        CREATE INDEX IF NOT EXISTS idx_ther_combined_primaryid ON faers_combined."THER_Combined" (primaryid);
        RAISE NOTICE 'Created index idx_ther_combined_primaryid';
    ELSE
        RAISE NOTICE 'Table faers_combined."THER_Combined" does not exist, skipping index creation';
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'faers_combined' AND table_name = 'REAC_Combined') THEN
        CREATE INDEX IF NOT EXISTS idx_reac_combined_primaryid ON faers_combined."REAC_Combined" (primaryid);
        RAISE NOTICE 'Created index idx_reac_combined_primaryid';
    ELSE
        RAISE NOTICE 'Table faers_combined."REAC_Combined" does not exist, skipping index creation';
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'faers_combined' AND table_name = 'RPSR_Combined') THEN
        CREATE INDEX IF NOT EXISTS idx_rpsr_combined_primaryid ON faers_combined."RPSR_Combined" (primaryid);
        RAISE NOTICE 'Created index idx_rpsr_combined_primaryid';
    ELSE
        RAISE NOTICE 'Table faers_combined."RPSR_Combined" does not exist, skipping index creation';
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'faers_combined' AND table_name = 'OUTC_Combined') THEN
        CREATE INDEX IF NOT EXISTS idx_outc_combined_primaryid ON faers_combined."OUTC_Combined" (primaryid);
        RAISE NOTICE 'Created index idx_outc_combined_primaryid';
    ELSE
        RAISE NOTICE 'Table faers_combined."OUTC_Combined" does not exist, skipping index creation';
    END IF;
END   $$;
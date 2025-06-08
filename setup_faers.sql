-- s2-5.sql: Consolidate FAERS data into faers_combined schema

SET search_path TO faers_combined, faers_a, public;
SET work_mem = '256MB';
SET statement_timeout = '600s';
SET client_min_messages TO NOTICE;

-- Create faers_combined schema if not exists
CREATE SCHEMA IF NOT EXISTS faers_combined;

-- Create combined tables
CREATE TABLE IF NOT EXISTS faers_combined."DEMO_Combined" (
    "DEMO_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    primaryid BIGINT,
    caseid BIGINT,
    caseversion TEXT,
    i_f_cod TEXT,
    event_dt TEXT,
    mfr_dt TEXT,
    init_fda_dt TEXT,
    fda_dt TEXT,
    rept_cod TEXT,
    auth_num TEXT,
    mfr_num TEXT,
    mfr_sndr TEXT,
    lit_ref TEXT,
    age TEXT,
    age_cod TEXT,
    age_grp TEXT,
    gndr_cod TEXT,
    e_sub TEXT,
    wt TEXT,
    wt_cod TEXT,
    rept_dt TEXT,
    to_mfr TEXT,
    occp_cod TEXT,
    reporter_country TEXT,
    occr_country TEXT,
    "PERIOD" TEXT
);

CREATE TABLE IF NOT EXISTS faers_combined."DRUG_Combined" (
    "DRUG_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    primaryid BIGINT,
    caseid BIGINT,
    drug_seq TEXT,
    role_cod TEXT,
    drugname TEXT,
    prod_ai TEXT,
    val_vbm TEXT,
    route TEXT,
    dose_vbm TEXT,
    cum_dose_chr TEXT,
    cum_dose_unit TEXT,
    dechal TEXT,
    rechal TEXT,
    lot_num TEXT,
    exp_dt TEXT,
    nda_num TEXT,
    dose_amt TEXT,
    dose_unit TEXT,
    dose_form TEXT,
    dose_freq TEXT,
    "PERIOD" TEXT
);

CREATE TABLE IF NOT EXISTS faers_combined."ERROR_LOG" (
    "ERROR_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    table_name TEXT,
    error_column TEXT,
    error_value TEXT,
    error_message TEXT,
    error_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Populate combined tables
DO $$
DECLARE
    rec RECORD;
    table_prefixes TEXT[] := ARRAY['demo', 'drug'];
    table_prefix TEXT;
    table_name TEXT;
    combined_table TEXT;
    sql_text TEXT;
BEGIN
    FOR rec IN SELECT year, quarter FROM faers_a.get_completed_year_quarters(4)
    LOOP
        FOR table_prefix IN SELECT unnest(table_prefixes)
        LOOP
            table_name := format('faers_a.%s%02dq%s', table_prefix, rec.year % 100, rec.quarter);
            combined_table := format('faers_combined."%s_Combined"', initcap(table_prefix));
            IF table_prefix = 'demo' THEN
                sql_text := format('
                    INSERT INTO %s (
                        primaryid, caseid, caseversion, i_f_cod, event_dt, mfr_dt, init_fda_dt, fda_dt,
                        rept_cod, auth_num, mfr_num, mfr_sndr, lit_ref, age, age_cod, age_grp, gndr_cod,
                        e_sub, wt, wt_cod, rept_dt, to_mfr, occp_cod, reporter_country, occr_country, "PERIOD"
                    )
                    SELECT
                        primaryid::BIGINT, caseid::BIGINT, caseversion, i_f_cod, event_dt, mfr_dt, init_fda_dt, fda_dt,
                        rept_cod, auth_num, mfr_num, mfr_sndr, lit_ref, age, age_cod, age_grp, gndr_cod,
                        e_sub, wt, wt_cod, rept_dt, to_mfr, occp_cod, reporter_country, occr_country,
                        format(''%sQ%s'', rec.year, rec.quarter)
                    FROM %s
                    ON CONFLICT DO NOTHING;
                ', combined_table, table_name);
            ELSIF table_prefix = 'drug' THEN
                sql_text := format('
                    INSERT INTO %s (
                        primaryid, caseid, drug_seq, role_cod, drugname, prod_ai, val_vbm, route, dose_vbm,
                        cum_dose_chr, cum_dose_unit, dechal, rechal, lot_num, exp_dt, nda_num, dose_amt,
                        dose_unit, dose_form, dose_freq, "PERIOD"
                    )
                    SELECT
                        primaryid::BIGINT, caseid::BIGINT, drug_seq, role_cod, drugname, prod_ai, val_vbm, route, dose_vbm,
                        cum_dose_chr, cum_dose_unit, dechal, rechal, lot_num, exp_dt, nda_num, dose_amt,
                        dose_unit, dose_form, dose_freq,
                        format(''%sQ%s'', rec.year, rec.quarter)
                    FROM %s
                    ON CONFLICT DO NOTHING;
                ', combined_table, table_name);
            END IF;
            BEGIN
                EXECUTE sql_text;
                RAISE NOTICE 'Inserted rows from % into %', table_name, combined_table;
            EXCEPTION WHEN OTHERS THEN
                INSERT INTO faers_combined."ERROR_LOG" (table_name, error_column, error_value, error_message)
                VALUES (table_name, NULL, NULL, SQLERRM);
                RAISE NOTICE 'Error inserting from % into %: %', table_name, combined_table, SQLERRM;
            END;
        END LOOP;
    END LOOP;
END $$;

-- Verify row counts
DO $$
DECLARE
    table_name TEXT;
    row_count BIGINT;
BEGIN
    FOR table_name IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'faers_combined'
        AND table_name NOT LIKE 'ERROR_LOG'
    LOOP
        EXECUTE format('SELECT COUNT(*) FROM faers_combined.%I', table_name) INTO row_count;
        IF row_count = 0 THEN
            RAISE WARNING 'Table faers_combined.% is empty', table_name;
        ELSE
            RAISE NOTICE 'Table faers_combined.% has % rows', table_name, row_count;
        END IF;
    END LOOP;
END $$;

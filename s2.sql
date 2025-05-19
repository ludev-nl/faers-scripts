-- Ensure this file is saved in UTF-8 encoding without BOM

/****************** CREATE FAERS_A DATABASE  ******************************/
-- Ensure the database exists (run this separately if needed)
-- CREATE DATABASE faers_a;

/****************** CONFIGURE DATABASE  ******************************/
-- Set client encoding to UTF-8
SET client_encoding = 'UTF8';

-- Configure logging in postgresql.conf if needed for performance
-- Note: PostgreSQL doesn't use ALTER DATABASE SET RECOVERY SIMPLE

/*
Function to determine completed year-quarter combinations from the start year to the last completed quarter.
*/
CREATE OR REPLACE FUNCTION get_completed_year_quarters(start_year INT DEFAULT 4)
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

    -- Generate year-quarter pairs from start_year
    y := start_year;
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

/*
Table to store schema configurations for FAERS tables.
*/
DROP TABLE IF EXISTS faers_schema_config;
CREATE TABLE faers_schema_config (
    table_name TEXT PRIMARY KEY,
    schema_json JSONB NOT NULL
);

-- Insert schema definitions for 2013Q1, adjusted to match data files
INSERT INTO faers_schema_config (table_name, schema_json) VALUES
('DEMO', '{
    "2013Q1": {
        "primaryid": "bigint",
        "caseid": "VARCHAR(50)",
        "caseversion": "int",
        "i_f_code": "VARCHAR(1)",
        "event_dt": "VARCHAR(8)",
        "mfr_dt": "VARCHAR(8)",
        "init_fda_dt": "VARCHAR(8)",
        "fda_dt": "VARCHAR(8)",
        "rept_cod": "VARCHAR(3)",
        "mfr_num": "VARCHAR(50)",
        "mfr_sndr": "VARCHAR(100)",
        "age": "VARCHAR(10)",
        "age_cod": "VARCHAR(10)",
        "gndr_cod": "VARCHAR(3)",
        "e_sub": "VARCHAR(1)",
        "wt": "VARCHAR(10)",
        "wt_cod": "VARCHAR(10)",
        "rept_dt": "VARCHAR(8)",
        "to_mfr": "VARCHAR(1)",
        "occp_cod": "VARCHAR(3)",
        "reporter_country": "VARCHAR(50)",
        "occr_country": "VARCHAR(50)"
    }
}'),
('DRUG', '{
    "2013Q1": {
        "primaryid": "bigint",
        "caseid": "bigint",
        "drug_seq": "bigint",
        "role_cod": "VARCHAR(2)",
        "drugname": "VARCHAR(500)",
        "val_vbm": "int",
        "route": "VARCHAR(70)",
        "dose_vbm": "VARCHAR(300)",
        "cum_dose_chr": "FLOAT",
        "cum_dose_unit": "VARCHAR(8)",
        "dechal": "VARCHAR(2)",
        "rechal": "VARCHAR(2)",
        "lot_num": "VARCHAR(565)",
        "exp_dt": "VARCHAR(200)",
        "nda_num": "VARCHAR(200)",
        "dose_amt": "VARCHAR(15)",
        "dose_unit": "VARCHAR(20)",
        "dose_form": "VARCHAR(100)",
        "dose_freq": "VARCHAR(20)"
    }
}'),
('INDI', '{
    "2013Q1": {
        "primaryid": "bigint",
        "caseid": "bigint",
        "indi_drug_seq": "bigint",
        "indi_pt": "VARCHAR(200)"
    }
}'),
('OUTC', '{
    "2013Q1": {
        "primaryid": "bigint",
        "caseid": "bigint",
        "outc_cod": "VARCHAR(4)"
    }
}'),
('REAC', '{
    "2013Q1": {
        "primaryid": "bigint",
        "caseid": "bigint",
        "pt": "VARCHAR(200)",
        "drug_rec_act": "VARCHAR(200)"
    }
}'),
('RPSR', '{
    "2013Q1": {
        "primaryid": "bigint",
        "caseid": "bigint",
        "rpsr_cod": "VARCHAR(50)"
    }
}'),
('THER', '{
    "2013Q1": {
        "primaryid": "bigint",
        "caseid": "bigint",
        "dsg_drug_seq": "bigint",
        "start_dt": "VARCHAR(8)",
        "end_dt": "VARCHAR(8)",
        "dur": "bigint",
        "dur_cod": "VARCHAR(10)"
    }
}');

/*
Function to retrieve schema for a given table and period.
*/
CREATE OR REPLACE FUNCTION get_schema_for_period(p_table_name TEXT, year INT, quarter INT)
RETURNS TEXT AS $func$
DECLARE
    schema_rec RECORD;
    target TEXT := LPAD(year::TEXT, 4, '0') || 'Q' || quarter;
    best_key TEXT := NULL;
    key TEXT;
    def JSONB;
    col_spec TEXT := '';
BEGIN
    SELECT schema_json INTO def
    FROM faers_schema_config
    WHERE table_name = UPPER(p_table_name);

    IF def IS NULL THEN
        RAISE EXCEPTION 'No schema found for table %', p_table_name;
    END IF;

    FOR key IN SELECT jsonb_object_keys(def) LOOP
        IF key <= target AND (best_key IS NULL OR key > best_key) THEN
            best_key := key;
        END IF;
    END LOOP;

    IF best_key IS NULL THEN
        RAISE EXCEPTION 'No schema version available for table % and period %', p_table_name, target;
    END IF;

    FOR schema_rec IN SELECT * FROM jsonb_each_text(def -> best_key) LOOP
        col_spec := col_spec || format('%I %s, ', schema_rec.key, schema_rec.value);
    END LOOP;

    RETURN RTRIM(col_spec, ', ');
END;
$func$ LANGUAGE plpgsql;

/****************** CREATE AND LOAD RAW DATA TABLES ******************************/

CREATE OR REPLACE FUNCTION load_all_faers_core_tables(root_dir TEXT, start_year INT DEFAULT 4)
RETURNS void AS $func$
DECLARE
    rec RECORD;
    period_upper TEXT;
    period_lower TEXT;
    core_table TEXT;
    table_prefix TEXT;
    file_prefix TEXT;
    schema_def TEXT;
    sql_stmt TEXT;
    file_path TEXT;
BEGIN
    FOR core_table IN SELECT DISTINCT table_name FROM faers_schema_config LOOP
        table_prefix := lower(core_table);
        file_prefix := upper(core_table);

        FOR rec IN SELECT * FROM get_completed_year_quarters(start_year) LOOP
            period_upper := LPAD(rec.year::TEXT, 2, '0') || 'Q' || rec.quarter::TEXT;
            period_lower := LPAD(rec.year::TEXT, 2, '0') || 'q' || rec.quarter::TEXT;
            file_path := root_dir || 'ascii/' || file_prefix || period_upper || '.txt';

            schema_def := get_schema_for_period(core_table, rec.year + 2000, rec.quarter);

            BEGIN
                sql_stmt := format($sql$
                    DROP TABLE IF EXISTS %I;
                    CREATE TABLE %I (%s);
                    COPY %I %s
                    FROM %L
                    WITH (
                        FORMAT csv,
                        HEADER true,
                        DELIMITER E'\$',
                        QUOTE '"',
                        NULL '',
                        ENCODING 'UTF8'
                    );
                $sql$,
                    table_prefix || period_lower,
                    table_prefix || period_lower,
                    schema_def,
                    table_prefix || period_lower,
                    CASE
                        WHEN core_table = 'DEMO' THEN '(
                            primaryid, caseid, caseversion, i_f_code, event_dt, mfr_dt,
                            init_fda_dt, fda_dt, rept_cod, mfr_num, mfr_sndr, age,
                            age_cod, gndr_cod, e_sub, wt, wt_cod, rept_dt, to_mfr,
                            occp_cod, reporter_country, occr_country
                        )'
                        WHEN core_table = 'DRUG' THEN '(
                            primaryid, caseid, drug_seq, role_cod, drugname, val_vbm, route,
                            dose_vbm, cum_dose_chr, cum_dose_unit, dechal, rechal, lot_num,
                            exp_dt, nda_num, dose_amt, dose_unit, dose_form, dose_freq
                        )'
                        WHEN core_table = 'INDI' THEN '(
                            primaryid, caseid, indi_drug_seq, indi_pt
                        )'
                        WHEN core_table = 'OUTC' THEN '(
                            primaryid, caseid, outc_cod
                        )'
                        WHEN core_table = 'REAC' THEN '(
                            primaryid, caseid, pt, drug_rec_act
                        )'
                        WHEN core_table = 'RPSR' THEN '(
                            primaryid, caseid, rpsr_cod
                        )'
                        WHEN core_table = 'THER' THEN '(
                            primaryid, caseid, dsg_drug_seq, start_dt, end_dt, dur, dur_cod
                        )'
                        ELSE ''
                    END,
                    file_path
                );
                EXECUTE sql_stmt;
                RAISE NOTICE 'Successfully loaded table % from %', table_prefix || period_lower, file_path;
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Failed to load table % from %: %', table_prefix || period_lower, file_path, SQLERRM;
                CONTINUE;
            END;
        END LOOP;
    END LOOP;
END;
$func$ LANGUAGE plpgsql;

-- Execute the loading function
SELECT load_all_faers_core_tables('C:/Users/xocas/OneDrive/Desktop/faers-scripts/', 13);

/****************** CREATE FAERS_A DATABASE  ******************************/
-- TODO make this dynamic again with fallback
/* IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = '[FAERS_A]') */
/*   BEGIN */
/*     CREATE DATABASE [FAERS_A] */
/*     END */
/*     GO */

/* 	USE [FAERS_A] */
/*     GO */
/* USE [FAERS_A] */
/* GO */

/****************** MINIMIZE DATABASE LOG SIZE  ******************************/

/* ALTER DATABASE [FAERS_A] SET RECOVERY SIMPLE; */

/*
Function to determine the last completed year-quarter combination from the current date.
*/
CREATE OR REPLACE FUNCTION get_completed_year_quarters(start_year INT DEFAULT 4)
RETURNS TABLE (year INT, quarter INT)
AS
$$
DECLARE
    current_year INT;
    current_quarter INT;
    last_year INT;
    last_quarter INT;
    y INT;
    q INT;
BEGIN
    -- Get the current year and quarter
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

    -- Generate year-quarter pairs
    y := start_year;
    WHILE y < last_year OR (y = last_year AND q <= last_quarter) LOOP
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
$$ LANGUAGE plpgsql;

/*
Helper function to load from a config-table the different types of tables to be copied (DEMO, DRUG, REAC, INDI, RPSR, THER)
*/
DROP TABLE IF EXISTS faers_core_config;
CREATE TABLE faers_core_config (
    id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,       -- programmatically checked name, just for readability
    table_prefix TEXT NOT NULL,     -- non-uppercase version of operation type (e.g., 'drug', 'reac', 'demo')
    file_prefix TEXT NOT NULL,      -- uppercase version of operation type (e.g., 'DRUG', 'REAC', 'DEMO')
    table_schema TEXT NOT NULL      -- full table definition text
);

/*
Core operations that are repeated over year-quarter combination; load them into config function
*/
INSERT INTO faers_core_config (table_name, table_prefix, file_prefix, table_schema) VALUES
('drug_from_04q1', 'drug', 'DRUG', $$
     isr bigint, 
     drug_seq bigint, 
     role_cod VARCHAR(2), 
     drugname VARCHAR(500), 
     val_vbm int, 
     route VARCHAR(70), 
     dose_vbm VARCHAR(300), 
     dechal VARCHAR(2), 
     rechal VARCHAR(2), 
     lot_num VARCHAR(565), 
     exp_dt VARCHAR(200), 
     nda_num VARCHAR(200) 
$$),
('drug_from_12q4', 'drug', 'DRUG', $$
     primaryid bigint, 
     caseid bigint, 
     drug_seq bigint, 
     role_cod VARCHAR(2), 
     drugname VARCHAR(500), 
     val_vbm int, 
     route VARCHAR(70), 
     dose_vbm VARCHAR(300), 
     cum_dose_chr FLOAT, 
     cum_dose_unit VARCHAR(8), 
     dechal VARCHAR(2), 
     rechal VARCHAR(2), 
     lot_num VARCHAR(565), 
     exp_dt VARCHAR(200), 
     nda_num VARCHAR(200), 
     dose_amt VARCHAR(15), 
     dose_unit VARCHAR(20), 
     dose_form VARCHAR(100), 
     dose_freq VARCHAR(20) 
$$),
('drug_from_14q3', 'drug', 'DRUG', $$
     primaryid bigint, 
     caseid bigint, 
     drug_seq bigint, 
     role_cod VARCHAR(2), 
     drugname VARCHAR(500), 
     prod_ai VARCHAR(800), 
     val_vbm int, 
     route VARCHAR(70), 
     dose_vbm VARCHAR(300), 
     cum_dose_chr FLOAT, 
     cum_dose_unit VARCHAR(8), 
     dechal VARCHAR(2), 
     rechal VARCHAR(2), 
     lot_num VARCHAR(565), 
     exp_dt VARCHAR(200), 
     nda_num VARCHAR(200), 
     dose_amt VARCHAR(15), 
     dose_unit VARCHAR(20), 
     dose_form VARCHAR(100), 
     dose_freq VARCHAR(20) 
$$),
('drug_from_22q1', 'drug', 'DRUG', $$
     primaryid bigint, 
     caseid bigint, 
     drug_seq bigint, 
     role_cod VARCHAR(2), 
     drugname VARCHAR(500), 
     prod_ai VARCHAR(800), 
     val_vbm int, 
     route VARCHAR(70), 
     dose_vbm VARCHAR(800), 
     cum_dose_chr FLOAT, 
     cum_dose_unit VARCHAR(8), 
     dechal VARCHAR(2), 
     rechal VARCHAR(2), 
     lot_num VARCHAR(565), 
     exp_dt VARCHAR(200), 
     nda_num VARCHAR(200), 
     dose_amt VARCHAR(15), 
     dose_unit VARCHAR(20), 
     dose_form VARCHAR(100), 
     dose_freq VARCHAR(20) 
$$),
('reac', 'reac', 'REAC', $$
    isr bigint,
    pt VARCHAR(100)
$$),
('demo', 'demo', 'DEMO', $$
    isr BIGINT,
    "case" BIGINT,
    i_f_cod VARCHAR(1),
    foll_seq VARCHAR(50),
    image VARCHAR(10),
    event_dt INT,
    mfr_dt INT,
    fda_dt INT,
    rept_cod VARCHAR(10),
    mfr_num VARCHAR(100),
    mfr_sndr VARCHAR(100),
    age VARCHAR(28),
    age_cod VARCHAR(3),
    gndr_cod VARCHAR(3),
    e_sub VARCHAR(1),
    wt VARCHAR(25),
    wt_cod VARCHAR(20),
    rept_dt INT,
    occp_cod VARCHAR(10),
    death_dt VARCHAR(1),
    to_mfr VARCHAR(1),
    confid VARCHAR(10)
$$),
('ther', 'ther', 'THER', $$
    isr bigint,
    drug_seq bigint,
    start_dt bigint,
    end_dt bigint,
    dur VARCHAR(50),
    dur_cod VARCHAR(50)
$$),
('rpsr', 'rpsr', 'RPSR', $$
    isr bigint,
    rpsr_cod VARCHAR(100)
$$),
('outc', 'outc', 'OUTC', $$
    isr bigint,
    outc_cod VARCHAR(20)
$$),
('indi', 'indi', 'INDI', $$
    isr bigint,
    drug_seq bigint,
    indi_pt VARCHAR(200)
$$);

/******************  CREATING RAW DATA TABLES, AND BULK INSERTION ******************************
>>>JUST REPLACE THE FOLDER DIRECTORY, TO THE DATA LOCATION IN YOUR PC, BEFORE STARTING<<<

WHAT THIS CODE DOES: 
- For all drugs, adversary reactions, indications, demographic and outcomes, create a table and bulk insert the data for each quarter
  from 2004Q1 to 2023Q1
- Combine all quarterly entries for each of the above into a single table, such as DRUG_COMBINED, DEMO_COMBINED, etc.

************************************************************************************************/

/********************************* CREATE TABLES FOR RAW FILES ******************************************/
/********************************* 
For all core tables from 2004Q1 to 2023Q1 , create a table and copy (bulk insert before postgresql) the data from the FAERS_MAK folder
******************************************/

-- TODO
-- I have been debugging this file by having the file buffer open in 1 window (vim / vs code), and having another
-- terminal on noah@lacdrvm:/faers-scripts/, inside a virenv with psycopyg such that i can run python3 s2.py . this gives
-- an error, i try to fix it, repeat.

-- current error should be psycopg.errors.BadCopyFileFormat: extra data after last expected column.
-- this is because, previously, your general "drug" schema was just the [04q1,12q4) schema, which fails
-- for the quarter 13q1 for instance. we see this in the generated SQL statement the error gives.

-- added table_name to fears_core_config, we can use this to get all the different versions of a single type (e.g. all drug table variations)

-- we need to add some logic to this function in order to discern which version of a schema we have to use.
-- since the awk script returned that for multiple types there exist different variations of schema's, we
-- would have to add some convoluted case statement to choose a different schema in this function, if
-- we want to keep it general. (OPTION A) Its probably better if we split this into multiple functions, and some
-- "arrays" discerning some type.
-- one for drugs, does (drugs_from_04q1,...,drugs_from_22q1)
-- one for different reac,
-- etc.
-- one for the rest, maybe there are some where the schema never changes?

-- (OPTION B), actually, maybe using case statements wouldnt be so bad, you woudl just have logic like
-- IF config.table_prefix = drug && period_full in [2004q1,2012q4), THEN schema = drug_from_04q1
-- this line a lot of times.

-- (OPTION C) something else? idk

-- up to you to choose a solution. good luck :)
-- let there be little errors after this big obstacle.

CREATE OR REPLACE FUNCTION load_all_faers_core_tables(start_year INT DEFAULT 4)
RETURNS void AS
$$
DECLARE
    config RECORD;
    rec RECORD;
    period_upper TEXT;
    period_lower TEXT;
    period_full TEXT;
    root_dir TEXT := '/faers/data/';
    sql TEXT;
    schema_to_use TEXT;
BEGIN
    -- Loop over each configured FAERS core type
    FOR config IN SELECT * FROM faers_core_config ORDER BY id LOOP
        -- Loop over each year-quarter
        FOR rec IN SELECT * FROM get_completed_year_quarters(start_year) LOOP
            period_upper := LPAD(rec.year::TEXT, 2, '0') || 'Q' || rec.quarter::TEXT;
            period_lower := LPAD(rec.year::TEXT, 2, '0') || 'q' || rec.quarter::TEXT;
            period_full := '20' || LPAD(rec.year::TEXT, 2, '0') || 'Q' || rec.quarter::TEXT;

            -- Select schema based on table_prefix and year-quarter
            IF config.table_prefix = 'drug' THEN
                IF (rec.year < 12) OR (rec.year = 12 AND rec.quarter <= 3) THEN
                    schema_to_use := (SELECT table_schema FROM faers_core_config WHERE table_name = 'drug_from_04q1');
                ELSIF (rec.year = 12 AND rec.quarter = 4) OR (rec.year = 13) OR (rec.year = 14 AND rec.quarter <= 2) THEN
                    schema_to_use := (SELECT table_schema FROM faers_core_config WHERE table_name = 'drug_from_12q4');
                ELSIF (rec.year = 14 AND rec.quarter >= 3) OR (rec.year >= 15 AND rec.year <= 21) THEN
                    schema_to_use := (SELECT table_schema FROM faers_core_config WHERE table_name = 'drug_from_14q3');
                ELSE
                    schema_to_use := (SELECT table_schema FROM faers_core_config WHERE table_name = 'drug_from_22q1');
                END IF;
            ELSE
                schema_to_use := config.table_schema; -- Use default schema for non-drug tables
            END IF;

            sql := format($sql$
                DROP TABLE IF EXISTS %I;
                CREATE TABLE %I (%s);
                COPY %I
                FROM %L
                WITH (
                    FORMAT csv,
                    HEADER true,
                    DELIMITER '$',
                    QUOTE E'\b'
                );
            $sql$,
                config.table_prefix || period_lower,
                config.table_prefix || period_lower,
                schema_to_use,
                config.table_prefix || period_lower,
                root_dir || 'faers_ascii_' || period_full || '/' || config.file_prefix || period_upper || '.txt'
            );
            EXECUTE sql;
        END LOOP;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

/********************************* COMBINE RAW FILES ******************************************/
/* For each category (DRUG, REAC, INDI, DEMO, OUTCOME, RPSR) take the quarterly data and add it to the combined data under 'PERIOD' */

CREATE OR REPLACE FUNCTION load_drug_combined()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    period TEXT;
BEGIN
    -- Iterate over completed year-quarters from 2004 (year = 4)
    FOR rec IN SELECT * FROM get_completed_year_quarters(4)
    LOOP
        period := LPAD(rec.year::TEXT, 2, '0') || 'Q' || rec.quarter::TEXT;

        IF (rec.year < 12) OR (rec.year = 12 AND rec.quarter <= 3) THEN
            -- Legacy
            EXECUTE format('
              INSERT INTO DRUG_Combined
              (primaryid, DRUG_SEQ, ROLE_COD, DRUGNAME, VAL_VBM, ROUTE, DOSE_VBM, DECHAL, RECHAL, LOT_NUM, EXP_DT, NDA_NUM, PERIOD)
              SELECT 
              ISR, DRUG_SEQ, ROLE_COD, DRUGNAME, VAL_VBM, ROUTE, DOSE_VBM, DECHAL, RECHAL, LOT_NUM, EXP_DT, NDA_NUM, %L 
              FROM %I
              ',
              period,
              'drug' || period
            );
        ELSE
            -- Current
            EXECUTE format('
              INSERT INTO DRUG_Combined
              (primaryid, caseid, DRUG_SEQ, ROLE_COD, DRUGNAME, VAL_VBM, ROUTE, DOSE_VBM, cum_dose_chr, cum_dose_unit,
                DECHAL, RECHAL, LOT_NUM, EXP_DT, NDA_NUM, dose_amt, dose_unit, dose_form, dose_freq, PERIOD)
              SELECT 
              primaryid, caseid, DRUG_SEQ, ROLE_COD, DRUGNAME, VAL_VBM, ROUTE, DOSE_VBM, cum_dose_chr, cum_dose_unit,
              DECHAL, RECHAL, LOT_NUM, EXP_DT, NDA_NUM, dose_amt, dose_unit, dose_form, dose_freq, %L
              FROM %I
              ',
              period,
              'drug' || period
            );
        END IF;
    END LOOP;

    -- Final cleanup
    UPDATE DRUG_Combined
    SET NDA_NUM = trim(' $ ' FROM REPLACE(REPLACE(REPLACE(NDA_NUM, CHR(10), ''), CHR(13), ''), CHR(9), ''))
    WHERE NDA_NUM IS NOT NULL;
END;
$$;

-----------------

CREATE OR REPLACE FUNCTION load_demo_combined()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    period TEXT;
    period_lower TEXT;
BEGIN
    -- Iterate over completed year-quarters from 2004 (year = 4)
    FOR rec IN SELECT * FROM get_completed_year_quarters(4)
    LOOP
        period := LPAD(rec.year::TEXT, 2, '0') || 'Q' || rec.quarter::TEXT;
        period_lower := LPAD(rec.year::TEXT, 2, '0') || 'Q' || rec.quarter::TEXT;

        IF (rec.year < 12) OR (rec.year = 12 AND rec.quarter <= 3) THEN
          -- Legacy
          EXECUTE format('
            INSERT INTO DEMO_Combined
            (primaryid, caseid, caseversion, I_F_COD, FOLL_SEQ, IMAGE, EVENT_DT, MFR_DT, FDA_DT, REPT_COD, 
              MFR_NUM, MFR_SNDR, AGE, AGE_COD, SEX, E_SUB, WT, wt_COD, REPT_DT, OCCP_COD, DEATH_DT, TO_MFR, CONFID, PERIOD)
            SELECT 
            ISR, "CASE", 0, I_F_COD, FOLL_SEQ, IMAGE, EVENT_DT, MFR_DT, FDA_DT, REPT_COD, 
            MFR_NUM, MFR_SNDR, AGE, AGE_COD, GNDR_COD, E_SUB, WT, wt_COD, REPT_DT, OCCP_COD, DEATH_DT, TO_MFR, CONFID, %L
            FROM %I
            ',
            period,
            'demo' || period_lower
          );
        ELSE
          -- Current
          EXECUTE format('
            INSERT INTO DEMO_Combined
            (primaryid, caseid, caseversion, I_F_COD, EVENT_DT, MFR_DT, init_fda_dt, FDA_DT, REPT_COD, 
              MFR_NUM, MFR_SNDR, AGE, AGE_COD, SEX, E_SUB, WT, wt_COD, REPT_DT, TO_MFR, occp_cod, REPORTER_COUNTRY, PERIOD)
            SELECT 
            primaryid, caseid, caseversion, I_F_COD, EVENT_DT, MFR_DT, init_fda_dt, FDA_DT, REPT_COD, 
            MFR_NUM, MFR_SNDR, AGE, AGE_COD, gndr_cod, E_SUB, WT, wt_COD, REPT_DT, TO_MFR, occp_cod, REPORTER_COUNTRY, %L
            FROM %I
            ',
            period,
            'demo' || period_lower
          );
        END IF;
    END LOOP;

    -- Final cleanup
    UPDATE DEMO_Combined
    SET CONFID = trim(' $ ' FROM REPLACE(REPLACE(REPLACE(CONFID, CHR(10), ''), CHR(13), ''), CHR(9), ''))
    WHERE CONFID IS NOT NULL;

    UPDATE DEMO_Combined
    SET REPORTER_COUNTRY = trim(' $ ' FROM REPLACE(REPLACE(REPLACE(REPORTER_COUNTRY, CHR(10), ''), CHR(13), ''), CHR(9), ''))
    WHERE REPORTER_COUNTRY IS NOT NULL;

    UPDATE DEMO_Combined
    SET occr_country = trim(' $ ' FROM REPLACE(REPLACE(REPLACE(occr_country, CHR(10), ''), CHR(13), ''), CHR(9), ''))
    WHERE occr_country IS NOT NULL;
END;
$$;

--------------------------------------------

CREATE OR REPLACE FUNCTION load_ther_combined()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    period TEXT;
    period_lower TEXT;
BEGIN
    -- Iterate over completed year-quarters starting from 2004
    FOR rec IN SELECT * FROM get_completed_year_quarters(4)
    LOOP
        period := LPAD(rec.year::TEXT, 2, '0') || 'Q' || rec.quarter::TEXT;
        period_lower := LPAD(rec.year::TEXT, 2, '0') || 'Q' || rec.quarter::TEXT;

        IF (rec.year < 12) OR (rec.year = 12 AND rec.quarter <= 3) THEN
          -- Legacy structure
          EXECUTE format('
            INSERT INTO THER_Combined
            (primaryid, dsg_drug_seq, START_DT, END_DT, DUR, DUR_COD, PERIOD)
            SELECT 
            ISR, DRUG_SEQ, START_DT, END_DT, DUR, DUR_COD, %L
            FROM %I
            ',
            period,
            'ther' || period_lower
          );
        ELSE
          -- Current structure
          EXECUTE format('
            INSERT INTO THER_Combined
            (primaryid, caseid, dsg_drug_seq, START_DT, END_DT, DUR, DUR_COD, PERIOD)
            SELECT 
            primaryid, caseid, dsg_drug_seq, START_DT, END_DT, DUR, DUR_COD, %L
            FROM %I
            ',
            period,
            'ther' || period_lower
          );
      END IF;
    END LOOP;

    -- Final cleanup
    UPDATE THER_Combined
    SET DUR_COD = trim(' $ ' FROM REPLACE(REPLACE(REPLACE(DUR_COD, CHR(10), ''), CHR(13), ''), CHR(9), ''))
    WHERE DUR_COD IS NOT NULL;
END;
$$;

----------------------------

CREATE OR REPLACE FUNCTION load_rpsr_combined()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    period TEXT;
    period_lower TEXT;
BEGIN
    -- Iterate over completed year-quarters starting from 2004
    FOR rec IN SELECT * FROM get_completed_year_quarters(4)
    LOOP
      period := LPAD(rec.year::TEXT, 2, '0') || 'Q' || rec.quarter::TEXT;
      period_lower := LPAD(rec.year::TEXT, 2, '0') || 'q' || rec.quarter::TEXT;

      IF (rec.year < 12) OR (rec.year = 12 AND rec.quarter <= 3) THEN
        -- Legacy structure
        EXECUTE format('
          INSERT INTO RPSR_Combined
          (primaryid, RPSR_COD, PERIOD)
          SELECT 
          ISR, RPSR_COD, %L
          FROM %I
          ',
          period,
          'rpsr' || period_lower
        );
      ELSE
        -- Current structure
        EXECUTE format('
          INSERT INTO RPSR_Combined
          (primaryid, caseid, RPSR_COD, PERIOD)
          SELECT 
          primaryid, caseid, RPSR_COD, %L
          FROM %I
          ',
          period,
          'rpsr' || period_lower
        );
      END IF;
  END LOOP;

    -- Final cleanup
    UPDATE RPSR_Combined
    SET RPSR_COD = trim(' $ ' FROM REPLACE(REPLACE(REPLACE(RPSR_COD, CHR(10), ''), CHR(13), ''), CHR(9), ''))
    WHERE RPSR_COD IS NOT NULL;
END;
$$;

---------------

CREATE OR REPLACE FUNCTION load_outc_combined()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    period TEXT;
    period_lower TEXT;
BEGIN
    -- Iterate over completed year-quarters starting from 2004
    FOR rec IN SELECT * FROM get_completed_year_quarters(4)
    LOOP
        period := LPAD(rec.year::TEXT, 2, '0') || 'Q' || rec.quarter::TEXT;
        period_lower := LPAD(rec.year::TEXT, 2, '0') || 'Q' || rec.quarter::TEXT;

        IF (rec.year < 12) OR (rec.year = 12 AND rec.quarter <= 3) THEN
          -- Legacy structure
          EXECUTE format('
            INSERT INTO OUTC_Combined
            (primaryid, OUTC_COD, PERIOD)
            SELECT 
            ISR, OUTC_COD, %L
            FROM %I
            ',
            period,
            'outc' || period_lower
          );
        ELSE
          -- Current structure (note: OUTC_CODE instead of OUTC_COD)
          EXECUTE format('
            INSERT INTO OUTC_Combined
            (primaryid, caseid, OUTC_COD, PERIOD)
            SELECT 
            primaryid, caseid, OUTC_CODE, %L
            FROM %I
            ',
            period,
            'outc' || period_lower
          );
      END IF;
  END LOOP;

    -- Final cleanup
    UPDATE OUTC_Combined
    SET OUTC_COD = trim(' $ ' FROM REPLACE(REPLACE(REPLACE(OUTC_COD, CHR(10), ''), CHR(13), ''), CHR(9), ''))
    WHERE OUTC_COD IS NOT NULL;
END;
$$;

----------------------

CREATE OR REPLACE FUNCTION load_indi_combined()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    period TEXT;
    period_lower TEXT;
BEGIN
    -- Iterate over completed year-quarters starting from 2004
    FOR rec IN SELECT * FROM get_completed_year_quarters(4)
    LOOP
        period := LPAD(rec.year::TEXT, 2, '0') || 'Q' || rec.quarter::TEXT;
        period_lower := LPAD(rec.year::TEXT, 2, '0') || 'q' || rec.quarter::TEXT;

        IF (rec.year < 12) OR (rec.year = 12 AND rec.quarter <= 3) THEN
          -- Legacy structure
          EXECUTE format('
            INSERT INTO INDI_Combined
            (primaryid, indi_drug_seq, INDI_PT, PERIOD)
            SELECT 
            ISR, DRUG_SEQ, INDI_PT, %L
            FROM %I
            ',
            period,
            'indi' || period_lower
          );
        ELSE
          -- Modern structure
          EXECUTE format('
            INSERT INTO INDI_Combined
            (primaryid, caseid, indi_drug_seq, INDI_PT, PERIOD)
            SELECT 
            primaryid, caseid, indi_drug_seq, INDI_PT, %L
            FROM %I
            ',
            period,
            'indi' || period_lower
          );
      END IF;
  END LOOP;
END;
$$;

------------------

CREATE OR REPLACE FUNCTION load_reac_combined()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    period TEXT;
    period_lower TEXT;
BEGIN
    -- Iterate over completed year-quarters starting from 2004
    FOR rec IN SELECT * FROM get_completed_year_quarters(3)
    LOOP
        period := LPAD(rec.year::TEXT, 2, '0') || 'Q' || rec.quarter::TEXT;
        period_lower := LPAD(rec.year::TEXT, 2, '0') || 'Q' || rec.quarter::TEXT;

        IF (rec.year < 12) OR (rec.year = 12 AND rec.quarter <= 3) THEN
          -- Legacy structure (04Q1 - 12Q3)
          EXECUTE format('
            INSERT INTO REAC_Combined
            (primaryid, PT, PERIOD)
            SELECT 
            ISR, PT, %L
            FROM %I
            ',
            period,
            'reac' || period_lower
          );

        ELSIF (rec.year = 12 AND rec.quarter >= 4) OR (rec.year BETWEEN 13 AND 14 AND rec.quarter <= 2) THEN
        -- 12Q4 - 14Q2
        EXECUTE format('
          INSERT INTO REAC_Combined
          (primaryid, caseid, PT, PERIOD)
          SELECT 
          primaryid, caseid, PT, %L
          FROM %I
          ',
          period,
          'reac' || period_lower
        );

      ELSE
        -- Modern structure (14Q3 - current)
        EXECUTE format('
          INSERT INTO REAC_Combined
          (primaryid, caseid, PT, drug_rec_act, PERIOD)
          SELECT 
          primaryid, caseid, PT, drug_rec_act, %L
          FROM %I
          ',
          period,
          'reac' || period_lower
        );
        END IF;
    END LOOP;

    -- Final cleanup
    EXECUTE format('
      UPDATE REAC_Combined
      SET PT = trim(''$'' FROM REPLACE(REPLACE(REPLACE(PT, CHAR(10), ''''), CHAR(13), ''''), CHAR(9), ''''))
      WHERE PT IS NOT NULL
    ');
END;
$$;

/* CREATE OR REPLACE FUNCTION load_all_faers_core_tables(start_year INT DEFAULT 4) */
/* PERFORM load_all_faers_core_tables(13); */
/* PERFORM (SELECT load_all_faers_core_tables(13)); */
/* load_all_faers_core_tables(13); */

-- TODO
-- FUNCTION
/* select count(*) from faers_core_config; */
/* EXECUTE format('load_all_faers_core_tables(13)'); */

SELECT load_all_faers_core_tables(13::INT);

























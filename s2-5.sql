-- s2-5.sql: Merge FAERS quarterly data into combined tables in faers_combined schema
SET search_path TO faers_combined, faers_a, public;
SET work_mem = '128MB';
SET statement_timeout = '300s';

-- Create schema
CREATE SCHEMA IF NOT EXISTS faers_combined;

-- Create DEMO_Combined
DROP TABLE IF EXISTS faers_combined."DEMO_Combined";
CREATE TABLE faers_combined."DEMO_Combined" (
    "DEMO_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    primaryid BIGINT,
    caseid BIGINT,
    caseversion INTEGER,
    "I_F_COD" VARCHAR(3),
    event_dt DATE,
    mfr_dt DATE,
    init_fda_dt DATE,
    fda_dt DATE,
    rept_cod VARCHAR(10),
    auth_num VARCHAR(50),
    mfr_num VARCHAR(50),
    mfr_sndr VARCHAR(100),
    lit_ref TEXT,
    age VARCHAR(10),
    age_cod VARCHAR(10),
    age_grp VARCHAR(5),
    sex VARCHAR(3),
    e_sub VARCHAR(1),
    wt VARCHAR(10),
    wt_cod VARCHAR(10),
    rept_dt DATE,
    to_mfr VARCHAR(1),
    occp_cod VARCHAR(10),
    reporter_country VARCHAR(50),
    occr_country VARCHAR(50),
    "PERIOD" VARCHAR(10)
);

-- Create DRUG_Combined
DROP TABLE IF EXISTS faers_combined."DRUG_Combined";
CREATE TABLE faers_combined."DRUG_Combined" (
    "DRUG_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    primaryid BIGINT,
    caseid BIGINT,
    "DRUG_SEQ" BIGINT,
    "ROLE_COD" VARCHAR(2),
    "DRUGNAME" TEXT,
    prod_ai TEXT,
    "VAL_VBM" INTEGER,
    "ROUTE" VARCHAR(70),
    "DOSE_VBM" TEXT,
    cum_dose_chr FLOAT,
    cum_dose_unit VARCHAR(8),
    "DECHAL" VARCHAR(2),
    "RECHAL" VARCHAR(2),
    "LOT_NUM" TEXT,
    "EXP_DT" VARCHAR(200),
    "NDA_NUM" VARCHAR(200),
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
    "INDI_PT" TEXT,
    "PERIOD" VARCHAR(10)
);

-- Create THER_Combined
DROP TABLE IF EXISTS faers_combined."THER_Combined";
CREATE TABLE faers_combined."THER_Combined" (
    "THER_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    primaryid BIGINT,
    caseid BIGINT,
    dsg_drug_seq BIGINT,
    "START_DT" DATE,
    "END_DT" DATE,
    "DUR" VARCHAR(10),
    "DUR_COD" VARCHAR(10),
    "PERIOD" VARCHAR(10)
);

-- Create REAC_Combined
DROP TABLE IF EXISTS faers_combined."REAC_Combined";
CREATE TABLE faers_combined."REAC_Combined" (
    primaryid BIGINT,
    caseid BIGINT,
    "PT" VARCHAR(100),
    drug_rec_act VARCHAR(100),
    "PERIOD" VARCHAR(10)
);

-- Create RPSR_Combined
DROP TABLE IF EXISTS faers_combined."RPSR_Combined";
CREATE TABLE faers_combined."RPSR_Combined" (
    "RPSR_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    primaryid BIGINT,
    caseid BIGINT,
    "RPSR_COD" VARCHAR(10),
    "PERIOD" VARCHAR(10)
);

-- Create OUTC_Combined
DROP TABLE IF EXISTS faers_combined."OUTC_Combined";
CREATE TABLE faers_combined."OUTC_Combined" (
    "OUTC_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    primaryid BIGINT,
    caseid BIGINT,
    "OUTC_COD" VARCHAR(10),
    "PERIOD" VARCHAR(10)
);

-- Create COMBINED_DELETED_CASES_REPORTS (placeholder)
DROP TABLE IF EXISTS faers_combined."COMBINED_DELETED_CASES_REPORTS";
CREATE TABLE faers_combined."COMBINED_DELETED_CASES_REPORTS" (
    "Field1" BIGINT
);

-- Merge data using dynamic SQL
DO $$
DECLARE
    rec RECORD;
    tbl_name TEXT;
BEGIN
    -- DEMO_Combined
    FOR rec IN (
        SELECT year, quarter
        FROM get_completed_year_quarters(4)
        WHERE EXISTS (
            SELECT 1
            FROM information_schema.tables t
            WHERE t.table_schema = 'faers_a'
            AND t.table_name = 'demo' || LPAD(year - 2000, 2, '0') || 'q' || quarter
        )
    ) LOOP
        tbl_name := 'faers_a.demo' || LPAD(rec.year - 2000, 2, '0') || 'q' || rec.quarter;
        EXECUTE format('
            INSERT INTO faers_combined."DEMO_Combined" (
                primaryid, caseid, caseversion, "I_F_COD", event_dt, mfr_dt, init_fda_dt, fda_dt,
                rept_cod, auth_num, mfr_num, mfr_sndr, lit_ref, age, age_cod, age_grp, sex,
                e_sub, wt, wt_cod, rept_dt, to_mfr, occp_cod, reporter_country, occr_country, "PERIOD"
            )
            SELECT
                COALESCE(primaryid, "ISR") AS primaryid,
                caseid,
                caseversion,
                "I_F_COD",
                event_dt,
                mfr_dt,
                init_fda_dt,
                fda_dt,
                rept_cod,
                auth_num,
                mfr_num,
                mfr_sndr,
                lit_ref,
                age,
                age_cod,
                age_grp,
                sex,
                e_sub,
                wt,
                wt_cod,
                rept_dt,
                to_mfr,
                occp_cod,
                reporter_country,
                occr_country,
                %L
            FROM %s',
            rec.year || 'Q' || rec.quarter, tbl_name);
    END LOOP;

    -- DRUG_Combined
    FOR rec IN (
        SELECT year, quarter
        FROM get_completed_year_quarters(4)
        WHERE EXISTS (
            SELECT 1
            FROM information_schema.tables t
            WHERE t.table_schema = 'faers_a'
            AND t.table_name = 'drug' || LPAD(year - 2000, 2, '0') || 'q' || quarter
        )
    ) LOOP
        tbl_name := 'faers_a.drug' || LPAD(rec.year - 2000, 2, '0') || 'q' || rec.quarter;
        EXECUTE format('
            INSERT INTO faers_combined."DRUG_Combined" (
                primaryid, caseid, "DRUG_SEQ", "ROLE_COD", "DRUGNAME", prod_ai, "VAL_VBM", "ROUTE",
                "DOSE_VBM", cum_dose_chr, cum_dose_unit, "DECHAL", "RECHAL", "LOT_NUM", "EXP_DT",
                "NDA_NUM", dose_amt, dose_unit, dose_form, dose_freq, "PERIOD"
            )
            SELECT
                COALESCE(primaryid, "ISR") AS primaryid,
                caseid,
                "DRUG_SEQ",
                "ROLE_COD",
                "DRUGNAME",
                prod_ai,
                "VAL_VBM",
                "ROUTE",
                "DOSE_VBM",
                cum_dose_chr,
                cum_dose_unit,
                "DECHAL",
                "RECHAL",
                "LOT_NUM",
                "EXP_DT",
                "NDA_NUM",
                dose_amt,
                dose_unit,
                dose_form,
                dose_freq,
                %L
            FROM %s',
            rec.year || 'Q' || rec.quarter, tbl_name);
    END LOOP;

    -- INDI_Combined
    FOR rec IN (
        SELECT year, quarter
        FROM get_completed_year_quarters(4)
        WHERE EXISTS (
            SELECT 1
            FROM information_schema.tables t
            WHERE t.table_schema = 'faers_a'
            AND t.table_name = 'indi' || LPAD(year - 2000, 2, '0') || 'q' || quarter
        )
    ) LOOP
        tbl_name := 'faers_a.indi' || LPAD(rec.year - 2000, 2, '0') || 'q' || rec.quarter;
        EXECUTE format('
            INSERT INTO faers_combined."INDI_Combined" (
                primaryid, caseid, indi_drug_seq, "INDI_PT", "PERIOD"
            )
            SELECT
                COALESCE(primaryid, "ISR") AS primaryid,
                caseid,
                indi_drug_seq,
                "INDI_PT",
                %L
            FROM %s',
            rec.year || 'Q' || rec.quarter, tbl_name);
    END LOOP;

    -- THER_Combined
    FOR rec IN (
        SELECT year, quarter
        FROM get_completed_year_quarters(4)
        WHERE EXISTS (
            SELECT 1
            FROM information_schema.tables t
            WHERE t.table_schema = 'faers_a'
            AND t.table_name = 'ther' || LPAD(year - 2000, 2, '0') || 'q' || quarter
        )
    ) LOOP
        tbl_name := 'faers_a.ther' || LPAD(rec.year - 2000, 2, '0') || 'q' || rec.quarter;
        EXECUTE format('
            INSERT INTO faers_combined."THER_Combined" (
                primaryid, caseid, dsg_drug_seq, "START_DT", "END_DT", "DUR", "DUR_COD", "PERIOD"
            )
            SELECT
                COALESCE(primaryid, "ISR") AS primaryid,
                caseid,
                dsg_drug_seq,
                "START_DT",
                "END_DT",
                "DUR",
                "DUR_COD",
                %L
            FROM %s',
            rec.year || 'Q' || rec.quarter, tbl_name);
    END LOOP;

    -- REAC_Combined
    FOR rec IN (
        SELECT year, quarter
        FROM get_completed_year_quarters(4)
        WHERE EXISTS (
            SELECT 1
            FROM information_schema.tables t
            WHERE t.table_schema = 'faers_a'
            AND t.table_name = 'reac' || LPAD(year - 2000, 2, '0') || 'q' || quarter
        )
    ) LOOP
        tbl_name := 'faers_a.reac' || LPAD(rec.year - 2000, 2, '0') || 'q' || rec.quarter;
        EXECUTE format('
            INSERT INTO faers_combined."REAC_Combined" (
                primaryid, caseid, "PT", drug_rec_act, "PERIOD"
            )
            SELECT
                COALESCE(primaryid, "ISR") AS primaryid,
                caseid,
                "PT",
                drug_rec_act,
                %L
            FROM %s',
            rec.year || 'Q' || rec.quarter, tbl_name);
    END LOOP;

    -- RPSR_Combined
    FOR rec IN (
        SELECT year, quarter
        FROM get_completed_year_quarters(4)
        WHERE EXISTS (
            SELECT 1
            FROM information_schema.tables t
            WHERE t.table_schema = 'faers_a'
            AND t.table_name = 'rpsr' || LPAD(year - 2000, 2, '0') || 'q' || quarter
        )
    ) LOOP
        tbl_name := 'faers_a.rpsr' || LPAD(rec.year - 2000, 2, '0') || 'q' || rec.quarter;
        EXECUTE format('
            INSERT INTO faers_combined."RPSR_Combined" (
                primaryid, caseid, "RPSR_COD", "PERIOD"
            )
            SELECT
                COALESCE(primaryid, "ISR") AS primaryid,
                caseid,
                "RPSR_COD",
                %L
            FROM %s',
            rec.year || 'Q' || rec.quarter, tbl_name);
    END LOOP;

    -- OUTC_Combined
    FOR rec IN (
        SELECT year, quarter
        FROM get_completed_year_quarters(4)
        WHERE EXISTS (
            SELECT 1
            FROM information_schema.tables t
            WHERE t.table_schema = 'faers_a'
            AND t.table_name = 'outc' || LPAD(year - 2000, 2, '0') || 'q' || quarter
        )
    ) LOOP
        tbl_name := 'faers_a.outc' || LPAD(rec.year - 2000, 2, '0') || 'q' || rec.quarter;
        EXECUTE format('
            INSERT INTO faers_combined."OUTC_Combined" (
                primaryid, caseid, "OUTC_COD", "PERIOD"
            )
            SELECT
                COALESCE(primaryid, "ISR") AS primaryid,
                caseid,
                COALESCE("OUTC_CODE", "OUTC_COD") AS "OUTC_COD",
                %L
            FROM %s',
            rec.year || 'Q' || rec.quarter, tbl_name);
    END LOOP;
END $$;

-- Clean data (remove $ and control characters)
UPDATE faers_combined."RPSR_Combined"
SET "RPSR_COD" = TRIM(' $ ' FROM REPLACE(REPLACE(REPLACE("RPSR_COD", CHR(10), ''), CHR(13), ''), CHR(9), ''))
WHERE "RPSR_COD" IS NOT NULL;

UPDATE faers_combined."OUTC_Combined"
SET "OUTC_COD" = TRIM(' $ ' FROM REPLACE(REPLACE(REPLACE("OUTC_COD", CHR(10), ''), CHR(13), ''), CHR(9), ''))
WHERE "OUTC_COD" IS NOT NULL;

UPDATE faers_combined."REAC_Combined"
SET "PT" = TRIM(' $ ' FROM REPLACE(REPLACE(REPLACE("PT", CHR(10), ''), CHR(13), ''), CHR(9), ''))
WHERE "PT" IS NOT NULL;

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_demo_combined_primaryid ON faers_combined."DEMO_Combined" (primaryid);
CREATE INDEX IF NOT EXISTS idx_drug_combined_primaryid ON faers_combined."DRUG_Combined" (primaryid);
CREATE INDEX IF NOT EXISTS idx_indi_combined_primaryid ON faers_combined."INDI_Combined" (primaryid);
CREATE INDEX IF NOT EXISTS idx_ther_combined_primaryid ON faers_combined."THER_Combined" (primaryid);
CREATE INDEX IF NOT EXISTS idx_reac_combined_primaryid ON faers_combined."REAC_Combined" (primaryid);
CREATE INDEX IF NOT EXISTS idx_rpsr_combined_primaryid ON faers_combined."RPSR_Combined" (primaryid);
CREATE INDEX IF NOT EXISTS idx_outc_combined_primaryid ON faers_combined."OUTC_Combined" (primaryid);


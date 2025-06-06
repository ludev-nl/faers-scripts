-- s11.sql: Create dataset tables for FAERS analysis in faers_b schema

-- Verify database context
DO $$
BEGIN
    IF current_database() != 'faersdatabase' THEN
        RAISE EXCEPTION 'Must be connected to faersdatabase, current database is %', current_database();
    END IF;
END $$;

-- Ensure faers_b schema exists
CREATE SCHEMA IF NOT EXISTS faers_b AUTHORIZATION postgres;

-- Verify schema exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_namespace WHERE nspname = 'faers_b') THEN
        RAISE EXCEPTION 'Schema faers_b failed to create';
    END IF;
END $$;

-- Grant privileges
GRANT ALL ON SCHEMA faers_b TO postgres;

-- Set search path
SET search_path TO faers_b, faers_combined, public;

-- Ensure remapping_log exists
CREATE TABLE IF NOT EXISTS faers_b.remapping_log (
    log_id SERIAL PRIMARY KEY,
    step VARCHAR(50),
    message TEXT,
    log_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create DRUGS_STANDARDIZED
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_3'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUGS_STANDARDIZED', 'Table faers_b.drug_mapper_3 does not exist, skipping');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_combined' AND table_name = 'aligned_demo_drug_reac_indi_ther'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUGS_STANDARDIZED', 'Table faers_combined.aligned_demo_drug_reac_indi_ther does not exist, skipping');
        RETURN;
    END IF;

    DROP TABLE IF EXISTS faers_b.drugs_standardized;
    CREATE TABLE faers_b.drugs_standardized (
        primaryid BIGINT,
        drug_id INTEGER,
        drug_seq BIGINT,
        role_cod VARCHAR(2),
        period VARCHAR(4),
        rxaui BIGINT,
        drug VARCHAR(3000)
    );

    INSERT INTO faers_b.drugs_standardized
    SELECT dm.primaryid, CAST(dm.drug_id AS INTEGER), dm.drug_seq, dm.role_cod, dm.period, 
           CAST(dm.remapping_rxaui AS BIGINT) AS rxaui, dm.remapping_str AS drug
    FROM faers_b.drug_mapper_3 dm
    INNER JOIN faers_combined.aligned_demo_drug_reac_indi_ther ad
        ON dm.primaryid = ad.primaryid
    WHERE dm.remapping_rxaui IS NOT NULL
      AND dm.remapping_rxaui != '92683486'; -- Excludes 'UNKNOWN STR'

    GET DIAGNOSTICS row_count = ROW_COUNT;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUGS_STANDARDIZED', 'No rows inserted, table faers_b.drugs_standardized is empty');
    ELSE
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUGS_STANDARDIZED', 'Created faers_b.drugs_standardized with ' || row_count || ' rows');
    END IF;

    CREATE INDEX IF NOT EXISTS idx_drugs_standardized_primaryid ON faers_b.drugs_standardized(primaryid);
    CREATE INDEX IF NOT EXISTS idx_drugs_standardized_rxaui ON faers_b.drugs_standardized(rxaui);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUGS_STANDARDIZED', 'Error: ' || SQLERRM);
END $$;

-- Create ADVERSE_REACTIONS
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_combined' AND table_name = 'reac_combined'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create ADVERSE_REACTIONS', 'Table faers_combined.reac_combined does not exist, skipping');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_combined' AND table_name = 'aligned_demo_drug_reac_indi_ther'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create ADVERSE_REACTIONS', 'Table faers_combined.aligned_demo_drug_reac_indi_ther does not exist, skipping');
        RETURN;
    END IF;

    DROP TABLE IF EXISTS faers_b.adverse_reactions;
    CREATE TABLE faers_b.adverse_reactions (
        primaryid BIGINT,
        period VARCHAR(4),
        adverse_event VARCHAR(1000)
    );

    WITH cte AS (
        SELECT rc.primaryid, rc.period, rc.pt AS meddra_code
        FROM faers_combined.reac_combined rc
        INNER JOIN faers_combined.aligned_demo_drug_reac_indi_ther ad
            ON rc.primaryid = ad.primaryid
    ),
    cte_2 AS (
        SELECT pt_name AS adverse_event 
        FROM faers_combined.pref_term
        UNION
        SELECT llt_name 
        FROM faers_combined.low_level_term
    )
    INSERT INTO faers_b.adverse_reactions
    SELECT cte.primaryid, cte.period, cte_2.adverse_event
    FROM cte
    INNER JOIN cte_2 ON cte.meddra_code = cte_2.adverse_event;

    GET DIAGNOSTICS row_count = ROW_COUNT;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create ADVERSE_REACTIONS', 'No rows inserted, table faers_b.adverse_reactions is empty');
    ELSE
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create ADVERSE_REACTIONS', 'Created faers_b.adverse_reactions with ' || row_count || ' rows');
    END IF;

    CREATE INDEX IF NOT EXISTS idx_adverse_reactions_primaryid ON faers_b.adverse_reactions(primaryid);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create ADVERSE_REACTIONS', 'Error: ' || SQLERRM);
END $$;

-- Create DRUG_ADVERSE_REACTIONS_Pairs
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drugs_standardized'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUG_ADVERSE_REACTIONS_Pairs', 'Table faers_b.drugs_standardized does not exist, skipping');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'adverse_reactions'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUG_ADVERSE_REACTIONS_Pairs', 'Table faers_b.adverse_reactions does not exist, skipping');
        RETURN;
    END IF;

    DROP TABLE IF EXISTS faers_b.drug_adverse_reactions_pairs;
    CREATE TABLE faers_b.drug_adverse_reactions_pairs (
        primaryid BIGINT,
        rxaui BIGINT,
        drug VARCHAR(3000),
        adverse_event VARCHAR(1000)
    );

    INSERT INTO faers_b.drug_adverse_reactions_pairs
    SELECT DISTINCT ds.primaryid, ds.rxaui, ds.drug, ar.adverse_event
    FROM faers_b.drugs_standardized ds
    INNER JOIN faers_b.adverse_reactions ar ON ds.primaryid = ar.primaryid;

    GET DIAGNOSTICS row_count = ROW_COUNT;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUG_ADVERSE_REACTIONS_Pairs', 'No rows inserted, table faers_b.drug_adverse_reactions_pairs is empty');
    ELSE
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUG_ADVERSE_REACTIONS_Pairs', 'Created faers_b.drug_adverse_reactions_pairs with ' || row_count || ' rows');
    END IF;

    CREATE INDEX IF NOT EXISTS idx_drug_adverse_pairs_rxaui ON faers_b.drug_adverse_reactions_pairs(rxaui);
    CREATE INDEX IF NOT EXISTS idx_drug_adverse_pairs_adverse_event ON faers_b.drug_adverse_reactions_pairs(adverse_event);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUG_ADVERSE_REACTIONS_Pairs', 'Error: ' || SQLERRM);
END $$;

-- Create DRUG_ADVERSE_REACTIONS_COUNT
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_adverse_reactions_pairs'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUG_ADVERSE_REACTIONS_COUNT', 'Table faers_b.drug_adverse_reactions_pairs does not exist, skipping');
        RETURN;
    END IF;

    DROP TABLE IF EXISTS faers_b.drug_adverse_reactions_count;
    CREATE TABLE faers_b.drug_adverse_reactions_count (
        rxaui BIGINT,
        drug VARCHAR(3000),
        adverse_event VARCHAR(1000),
        count_of_reaction BIGINT
    );

    INSERT INTO faers_b.drug_adverse_reactions_count
    SELECT rxaui, drug, adverse_event, COUNT(*) AS count_of_reaction
    FROM faers_b.drug_adverse_reactions_pairs
    GROUP BY rxaui, drug, adverse_event;

    GET DIAGNOSTICS row_count = ROW_COUNT;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUG_ADVERSE_REACTIONS_COUNT', 'No rows inserted, table faers_b.drug_adverse_reactions_count is empty');
    ELSE
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUG_ADVERSE_REACTIONS_COUNT', 'Created faers_b.drug_adverse_reactions_count with ' || row_count || ' rows');
    END IF;

    CREATE INDEX IF NOT EXISTS idx_drug_adverse_count_rxaui ON faers_b.drug_adverse_reactions_count(rxaui);
    CREATE INDEX IF NOT EXISTS idx_drug_adverse_count_adverse_event ON faers_b.drug_adverse_reactions_count(adverse_event);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUG_ADVERSE_REACTIONS_COUNT', 'Error: ' || SQLERRM);
END $$;

-- Create DRUG_INDICATIONS
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_combined' AND table_name = 'indi_combined'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUG_INDICATIONS', 'Table faers_combined.indi_combined does not exist, skipping');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_combined' AND table_name = 'aligned_demo_drug_reac_indi_ther'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUG_INDICATIONS', 'Table faers_combined.aligned_demo_drug_reac_indi_ther does not exist, skipping');
        RETURN;
    END IF;

    DROP TABLE IF EXISTS faers_b.drug_indications;
    CREATE TABLE faers_b.drug_indications (
        primaryid BIGINT,
        indi_drug_seq BIGINT,
        period VARCHAR(4),
        drug_indication VARCHAR(1000)
    );

    WITH cte AS (
        SELECT ic.primaryid, ic.indi_drug_seq, ic.period, ic.indi_pt AS meddra_code
        FROM faers_combined.indi_combined ic
        INNER JOIN faers_combined.aligned_demo_drug_reac_indi_ther ad
            ON ic.primaryid = ad.primaryid
        WHERE ic.indi_pt NOT IN ('10070592', '10057097') -- Excludes specific codes
    ),
    cte_2 AS (
        SELECT pt_name AS drug_indication 
        FROM faers_combined.pref_term
        UNION
        SELECT llt_name 
        FROM faers_combined.low_level_term
    )
    INSERT INTO faers_b.drug_indications
    SELECT cte.primaryid, cte.indi_drug_seq, cte.period, cte_2.drug_indication
    FROM cte
    INNER JOIN cte_2 ON cte.meddra_code = cte_2.drug_indication;

    GET DIAGNOSTICS row_count = ROW_COUNT;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUG_INDICATIONS', 'No rows inserted, table faers_b.drug_indications is empty');
    ELSE
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUG_INDICATIONS', 'Created faers_b.drug_indications with ' || row_count || ' rows');
    END IF;

    CREATE INDEX IF NOT EXISTS idx_drug_indications_primaryid ON faers_b.drug_indications(primaryid);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUG_INDICATIONS', 'Error: ' || SQLERRM);
END $$;

-- Create DEMOGRAPHICS
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_combined' AND table_name = 'aligned_demo_drug_reac_indi_ther'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DEMOGRAPHICS', 'Table faers_combined.aligned_demo_drug_reac_indi_ther does not exist, skipping');
        RETURN;
    END IF;

    DROP TABLE IF EXISTS faers_b.demographics;
    CREATE TABLE faers_b.demographics (
        caseid BIGINT,
        primaryid BIGINT,
        caseversion INTEGER,
        fda_dt DATE,
        i_f_cod VARCHAR(2),
        event_dt DATE,
        age FLOAT,
        gender VARCHAR(3),
        country_code VARCHAR(3),
        period VARCHAR(4)
    );

    INSERT INTO faers_b.demographics
    SELECT caseid, primaryid, caseversion, 
           TO_DATE(NULLIF(fda_dt, ''), 'YYYYMMDD') AS fda_dt, 
           i_f_cod, 
           TO_DATE(NULLIF(event_dt, ''), 'YYYYMMDD') AS event_dt,
           CASE 
               WHEN age ~ '^[0-9]+$' THEN CAST(age AS FLOAT)
               ELSE NULL
           END AS age, 
           gndr_cod AS gender, 
           occr_country AS country_code, 
           period
    FROM faers_combined.aligned_demo_drug_reac_indi_ther;

    GET DIAGNOSTICS row_count = ROW_COUNT;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DEMOGRAPHICS', 'No rows inserted, table faers_b.demographics is empty');
    ELSE
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DEMOGRAPHICS', 'Created faers_b.demographics with ' || row_count || ' rows');
    END IF;

    CREATE INDEX IF NOT EXISTS idx_demographics_primaryid ON faers_b.demographics(primaryid);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DEMOGRAPHICS', 'Error: ' || SQLERRM);
END $$;

-- Create CASE_OUTCOMES
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_combined' AND table_name = 'outc_combined'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create CASE_OUTCOMES', 'Table faers_combined.outc_combined does not exist, skipping');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_combined' AND table_name = 'aligned_demo_drug_reac_indi_ther'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create CASE_OUTCOMES', 'Table faers_combined.aligned_demo_drug_reac_indi_ther does not exist, skipping');
        RETURN;
    END IF;

    DROP TABLE IF EXISTS faers_b.case_outcomes;
    CREATE TABLE faers_b.case_outcomes (
        primaryid BIGINT,
        outc_cod VARCHAR(2),
        period VARCHAR(4)
    );

    INSERT INTO faers_b.case_outcomes
    SELECT DISTINCT oc.primaryid, oc.outc_cod, oc.period
    FROM faers_combined.outc_combined oc
    INNER JOIN faers_combined.aligned_demo_drug_reac_indi_ther ad
        ON oc.primaryid = ad.primaryid;

    GET DIAGNOSTICS row_count = ROW_COUNT;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create CASE_OUTCOMES', 'No rows inserted, table faers_b.case_outcomes is empty');
    ELSE
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create CASE_OUTCOMES', 'Created faers_b.case_outcomes with ' || row_count || ' rows');
    END IF;

    CREATE INDEX IF NOT EXISTS idx_case_outcomes_primaryid ON faers_b.case_outcomes(primaryid);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create CASE_OUTCOMES', 'Error: ' || SQLERRM);
END $$;

-- Create THERAPY_DATES
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_combined' AND table_name = 'ther_combined'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create THERAPY_DATES', 'Table faers_combined.ther_combined does not exist, skipping');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_combined' AND table_name = 'aligned_demo_drug_reac_indi_ther'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create THERAPY_DATES', 'Table faers_combined.aligned_demo_drug_reac_indi_ther does not exist, skipping');
        RETURN;
    END IF;

    DROP TABLE IF EXISTS faers_b.therapy_dates;
    CREATE TABLE faers_b.therapy_dates (
        primaryid BIGINT,
        dsg_drug_seq BIGINT,
        start_dt DATE,
        end_dt DATE,
        dur VARCHAR(10),
        dur_cod VARCHAR(3),
        period VARCHAR(4)
    );

    INSERT INTO faers_b.therapy_dates
    SELECT tc.primaryid, tc.dsg_drug_seq, 
           TO_DATE(NULLIF(tc.start_dt, ''), 'YYYYMMDD') AS start_dt, 
           TO_DATE(NULLIF(tc.end_dt, ''), 'YYYYMMDD') AS end_dt, 
           tc.dur,
           NULLIF(tc.dur_cod, '') AS dur_cod, 
           tc.period
    FROM faers_combined.ther_combined tc
    INNER JOIN faers_combined.aligned_demo_drug_reac_indi_ther ad
        ON tc.primaryid = ad.primaryid;

    GET DIAGNOSTICS row_count = ROW_COUNT;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create THERAPY_DATES', 'No rows inserted, table faers_b.therapy_dates is empty');
    ELSE
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create THERAPY_DATES', 'Created faers_b.therapy_dates with ' || row_count || ' rows');
    END IF;

    CREATE INDEX IF NOT EXISTS idx_therapy_dates_primaryid ON faers_b.therapy_dates(primaryid);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create THERAPY_DATES', 'Error: ' || SQLERRM);
END $$;

-- Create REPORT_SOURCES
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_combined' AND table_name = 'rpsr_combined'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create REPORT_SOURCES', 'Table faers_combined.rpsr_combined does not exist, skipping');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_combined' AND table_name = 'aligned_demo_drug_reac_indi_ther'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create REPORT_SOURCES', 'Table faers_combined.aligned_demo_drug_reac_indi_ther does not exist, skipping');
        RETURN;
    END IF;

    DROP TABLE IF EXISTS faers_b.report_sources;
    CREATE TABLE faers_b.report_sources (
        primaryid BIGINT,
        rpsr_cod VARCHAR(3),
        period VARCHAR(4)
    );

    INSERT INTO faers_b.report_sources
    SELECT rc.primaryid, rc.rpsr_cod, rc.period
    FROM faers_combined.rpsr_combined rc
    INNER JOIN faers_combined.aligned_demo_drug_reac_indi_ther ad
        ON rc.primaryid = ad.primaryid;

    GET DIAGNOSTICS row_count = ROW_COUNT;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create REPORT_SOURCES', 'No rows inserted, table faers_b.report_sources is empty');
    ELSE
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create REPORT_SOURCES', 'Created faers_b.report_sources with ' || row_count || ' rows');
    END IF;

    CREATE INDEX IF NOT EXISTS idx_report_sources_primaryid ON faers_b.report_sources(primaryid);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create REPORT_SOURCES', 'Error: ' || SQLERRM);
END $$;

-- Create DRUG_MARGIN
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_adverse_reactions_count'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUG_MARGIN', 'Table faers_b.drug_adverse_reactions_count does not exist, skipping');
        RETURN;
    END IF;

    DROP TABLE IF EXISTS faers_b.drug_margin;
    CREATE TABLE faers_b.drug_margin (
        rxaui BIGINT,
        margin BIGINT
    );

    INSERT INTO faers_b.drug_margin
    SELECT rxaui, SUM(count_of_reaction) AS margin
    FROM faers_b.drug_adverse_reactions_count
    GROUP BY rxaui;

    GET DIAGNOSTICS row_count = ROW_COUNT;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUG_MARGIN', 'No rows inserted, table faers_b.drug_margin is empty');
    ELSE
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUG_MARGIN', 'Created faers_b.drug_margin with ' || row_count || ' rows');
    END IF;

    CREATE INDEX IF NOT EXISTS idx_drug_margin_rxaui ON faers_b.drug_margin(rxaui);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create DRUG_MARGIN', 'Error: ' || SQLERRM);
END $$;

-- Create EVENT_MARGIN
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_adverse_reactions_count'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create EVENT_MARGIN', 'Table faers_b.drug_adverse_reactions_count does not exist, skipping');
        RETURN;
    END IF;

    DROP TABLE IF EXISTS faers_b.event_margin;
    CREATE TABLE faers_b.event_margin (
        adverse_event VARCHAR(1000),
        margin BIGINT
    );

    INSERT INTO faers_b.event_margin
    SELECT adverse_event, SUM(count_of_reaction) AS margin
    FROM faers_b.drug_adverse_reactions_count
    GROUP BY adverse_event;

    GET DIAGNOSTICS row_count = ROW_COUNT;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create EVENT_MARGIN', 'No rows inserted, table faers_b.event_margin is empty');
    ELSE
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create EVENT_MARGIN', 'Created faers_b.event_margin with ' || row_count || ' rows');
    END IF;

    CREATE INDEX IF NOT EXISTS idx_event_margin_adverse_event ON faers_b.event_margin(adverse_event);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create EVENT_MARGIN', 'Error: ' || SQLERRM);
END $$;

-- Create TOTAL_COUNT
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_adverse_reactions_count'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create TOTAL_COUNT', 'Table faers_b.drug_adverse_reactions_count does not exist, skipping');
        RETURN;
    END IF;

    DROP TABLE IF EXISTS faers_b.total_count;
    CREATE TABLE faers_b.total_count (
        n BIGINT
    );

    INSERT INTO faers_b.total_count
    SELECT SUM(count_of_reaction) AS n
    FROM faers_b.drug_adverse_reactions_count;

    GET DIAGNOSTICS row_count = ROW_COUNT;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create TOTAL_COUNT', 'No rows inserted, table faers_b.total_count is empty');
    ELSE
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create TOTAL_COUNT', 'Created faers_b.total_count with ' || row_count || ' rows');
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create TOTAL_COUNT', 'Error: ' || SQLERRM);
END $$;

-- Create CONTINGENCY_TABLE
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_adverse_reactions_count'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create CONTINGENCY_TABLE', 'Table faers_b.drug_adverse_reactions_count does not exist, skipping');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_margin'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create CONTINGENCY_TABLE', 'Table faers_b.drug_margin does not exist, skipping');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'event_margin'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create CONTINGENCY_TABLE', 'Table faers_b.event_margin does not exist, skipping');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'total_count'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create CONTINGENCY_TABLE', 'Table faers_b.total_count does not exist, skipping');
        RETURN;
    END IF;

    DROP TABLE IF EXISTS faers_b.contingency_table;
    CREATE TABLE faers_b.contingency_table (
        id SERIAL PRIMARY KEY,
        rxaui BIGINT,
        drug VARCHAR(3000),
        adverse_event VARCHAR(1000),
        a FLOAT,
        b FLOAT,
        c FLOAT,
        d FLOAT
    );

    INSERT INTO faers_b.contingency_table (rxaui, drug, adverse_event, a, b, c, d)
    SELECT darc.rxaui, darc.drug, darc.adverse_event,
           darc.count_of_reaction AS a,
           (em.margin - darc.count_of_reaction) AS b,
           (dm.margin - darc.count_of_reaction) AS c,
           (SELECT n FROM faers_b.total_count) - em.margin - (dm.margin - darc.count_of_reaction) AS d
    FROM faers_b.drug_adverse_reactions_count darc
    INNER JOIN faers_b.drug_margin dm ON darc.rxaui = dm.rxaui
    INNER JOIN faers_b.event_margin em ON darc.adverse_event = em.adverse_event;

    GET DIAGNOSTICS row_count = ROW_COUNT;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create CONTINGENCY_TABLE', 'No rows inserted, table faers_b.contingency_table is empty');
    ELSE
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create CONTINGENCY_TABLE', 'Created faers_b.contingency_table with ' || row_count || ' rows');
    END IF;

    CREATE INDEX IF NOT EXISTS idx_contingency_table_rxaui ON faers_b.contingency_table(rxaui);
    CREATE INDEX IF NOT EXISTS idx_contingency_table_adverse_event ON faers_b.contingency_table(adverse_event);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create CONTINGENCY_TABLE', 'Error: ' || SQLERRM);
END $$;

-- Create PROPORTIONATE_ANALYSIS
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'contingency_table'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create PROPORTIONATE_ANALYSIS', 'Table faers_b.contingency_table does not exist, skipping');
        RETURN;
    END IF;

    DROP TABLE IF EXISTS faers_b.proportionate_analysis;
    CREATE TABLE faers_b.proportionate_analysis (
        id SERIAL PRIMARY KEY,
        rxaui BIGINT,
        drug VARCHAR(3000),
        adverse_event VARCHAR(1000),
        a FLOAT,
        n_expected FLOAT,
        prr FLOAT,
        prr_lb FLOAT,
        prr_ub FLOAT,
        chi_squared_yates FLOAT,
        ror FLOAT,
        ror_lb FLOAT,
        ror_ub FLOAT,
        ic FLOAT,
        ic025 FLOAT,
        ic975 FLOAT
    );

    INSERT INTO faers_b.proportionate_analysis
    SELECT ct.rxaui, ct.drug, ct.adverse_event, ct.a,
           ((ct.a + ct.b) * (ct.a + ct.c)) / NULLIF((ct.a + ct.b + ct.c + ct.d), 0) AS n_expected,
           (ct.a / NULLIF((ct.a + ct.c), 0)) / NULLIF((ct.b / NULLIF((ct.b + ct.d), 0)), 0) AS prr,
           EXP(LN((ct.a / NULLIF((ct.a + ct.c), 0)) / NULLIF((ct.b / NULLIF((ct.b + ct.d), 0)), 0)) 
               - 1.96 * SQRT((1.0 / ct.a) - (1.0 / (ct.a + ct.c)) + (1.0 / ct.b) - (1.0 / (ct.b + ct.d)))) AS prr_lb,
           EXP(LN((ct.a / NULLIF((ct.a + ct.c), 0)) / NULLIF((ct.b / NULLIF((ct.b + ct.d), 0)), 0)) 
               + 1.96 * SQRT((1.0 / ct.a) - (1.0 / (ct.a + ct.c)) + (1.0 / ct.b) - (1.0 / (ct.b + ct.d)))) AS prr_ub,
           ROUND(CAST((ct.a + ct.b + ct.c + ct.d) * 
                 POWER(ABS((ct.a * ct.d) - (ct.b * ct.c)) - ((ct.a + ct.b + ct.c + ct.d) / 2.0), 2) / 
                 NULLIF(((ct.a + ct.c) * (ct.b + ct.d) * (ct.a + ct.b) * (ct.c + ct.d)), 0) AS NUMERIC), 8) AS chi_squared_yates,
           ROUND(CAST(((ct.a / NULLIF(ct.c, 0)) / NULLIF((ct.b / NULLIF(ct.d, 0)), 0)) AS NUMERIC), 8) AS ror,
           EXP(LN((ct.a / NULLIF(ct.c, 0)) / NULLIF((ct.b / NULLIF(ct.d, 0)), 0)) 
               - 1.96 * SQRT((1.0 / ct.a) + (1.0 / ct.b) + (1.0 / ct.c) + (1.0 / (ct.b + ct.d)))) AS ror_lb,
           EXP(LN((ct.a / NULLIF(ct.c, 0)) / NULLIF((ct.b / NULLIF(ct.d, 0)), 0)) 
               + 1.96 * SQRT((1.0 / ct.a) + (1.0 / ct.b) + (1.0 / ct.c) + (1.0 / (ct.b + ct.d)))) AS ror_ub,
           LOG(2, (ct.a + 0.5) / NULLIF((((ct.a + ct.b) * (ct.a + ct.c)) / (ct.a + ct.b + ct.c + ct.d) + 0.5), 0)) AS ic,
           LOG(2, (ct.a + 0.5) / NULLIF((((ct.a + ct.b) * (ct.a + ct.c)) / (ct.a + ct.b + ct.c + ct.d) + 0.5), 0)) 
               - (3.3 * POWER((ct.a + 0.5), -0.5)) - (2.0 * POWER((ct.a + 0.5), -1.5)) AS ic025,
           LOG(2, (ct.a + 0.5) / NULLIF((((ct.a + ct.b) * (ct.a + ct.c)) / (ct.a + ct.b + ct.c + ct.d) + 0.5), 0)) 
               + (2.4 * POWER((ct.a + 0.5), -0.5)) - (0.5 * POWER((ct.a + 0.5), -1.5)) AS ic975
    FROM faers_b.contingency_table ct
    WHERE ct.a > 0 AND ct.b > 0 AND ct.c > 0 AND ct.d > 0; -- Avoid division by zero

    GET DIAGNOSTICS row_count = ROW_COUNT;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create PROPORTIONATE_ANALYSIS', 'No rows inserted, table faers_b.proportionate_analysis is empty');
    ELSE
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create PROPORTIONATE_ANALYSIS', 'Created faers_b.proportionate_analysis with ' || row_count || ' rows');
    END IF;

    CREATE INDEX IF NOT EXISTS idx_proportionate_analysis_rxaui ON faers_b.proportionate_analysis(rxaui);
    CREATE INDEX IF NOT EXISTS idx_proportionate_analysis_adverse_event ON faers_b.proportionate_analysis(adverse_event);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) 
        VALUES ('Create PROPORTIONATE_ANALYSIS', 'Error: ' || SQLERRM);
END $$;
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

-- Create FAERS_Analysis_Summary table
DROP TABLE IF EXISTS faers_b."FAERS_Analysis_Summary";
CREATE TABLE faers_b."FAERS_Analysis_Summary" (
    "SUMMARY_ID" SERIAL PRIMARY KEY,
    "RXCUI" VARCHAR(8),
    "DRUGNAME" TEXT,
    "REACTION_PT" VARCHAR(100),
    "OUTCOME_CODE" VARCHAR(20),
    "EVENT_COUNT" BIGINT,
    "REPORTING_PERIOD" VARCHAR(10),
    "ANALYSIS_DATE" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Check if source tables exist
DO $$
DECLARE
    table_exists BOOLEAN;
BEGIN
    -- Check faers_b.DRUG_RxNorm_Mapping
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'DRUG_RxNorm_Mapping'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_b.DRUG_RxNorm_Mapping does not exist, skipping INSERT into FAERS_Analysis_Summary';
        RETURN;
    END IF;

    -- Check faers_combined.REAC_Combined
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_combined') 
        AND relname = 'REAC_Combined'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_combined.REAC_Combined does not exist, skipping INSERT into FAERS_Analysis_Summary';
        RETURN;
    END IF;

    -- Check faers_combined.OUTC_Combined
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_combined') 
        AND relname = 'OUTC_Combined'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_combined.OUTC_Combined does not exist, skipping INSERT into FAERS_Analysis_Summary';
        RETURN;
    END IF;
END $$;

-- Insert data into FAERS_Analysis_Summary
INSERT INTO faers_b."FAERS_Analysis_Summary" (
    "RXCUI", "DRUGNAME", "REACTION_PT", "OUTCOME_CODE", "EVENT_COUNT", "REPORTING_PERIOD"
)
SELECT 
    drm."RXCUI",
    drm."DRUGNAME",
    rc.pt AS "REACTION_PT",
    oc.outc_cod AS "OUTCOME_CODE",
    COUNT(*) AS "EVENT_COUNT",
    rc."PERIOD" AS "REPORTING_PERIOD"
FROM faers_b."DRUG_RxNorm_Mapping" drm
INNER JOIN faers_combined."REAC_Combined" rc
    ON drm."primaryid" = rc."primaryid"
INNER JOIN faers_combined."OUTC_Combined" oc
    ON drm."primaryid" = oc."primaryid"
GROUP BY drm."RXCUI", drm."DRUGNAME", rc.pt, oc.outc_cod, rc."PERIOD"
ON CONFLICT DO NOTHING;

-- Create indexes
CREATE INDEX IF NOT EXISTS "idx_analysis_rxcui" ON faers_b."FAERS_Analysis_Summary" ("RXCUI");
CREATE INDEX IF NOT EXISTS "idx_analysis_reaction" ON faers_b."FAERS_Analysis_Summary" ("REACTION_PT");
CREATE INDEX IF NOT EXISTS "idx_analysis_outcome" ON faers_b."FAERS_Analysis_Summary" ("OUTCOME_CODE");

-- Optional: Export results (commented out)
-- \copy faers_b."FAERS_Analysis_Summary" TO '/data/faers/FAERS_MAK/2.LoadDataToDatabase/analysis_summary.csv' WITH (FORMAT CSV, DELIMITER ',', NULL '', HEADER TRUE);
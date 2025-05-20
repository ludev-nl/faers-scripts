
ALTER TABLE "DEMO_Combined" ADD COLUMN IF NOT EXISTS "AGE_Years_fixed" FLOAT;

WITH AgeConversion AS (
    SELECT
        "DEMO_ID",
        CASE
            WHEN "AGE_COD" = 'DEC' THEN ROUND(CAST("AGE" AS FLOAT) * 12, 2)
            WHEN "AGE_COD" IN ('YR', 'YEAR') THEN ROUND("AGE", 2)
            WHEN "AGE_COD" = 'MON' THEN ROUND(CAST("AGE" AS FLOAT) / 12, 2)
            WHEN "AGE_COD" IN ('WK', 'WEEK') THEN ROUND(CAST("AGE" AS FLOAT) / 52, 2)
            WHEN "AGE_COD" IN ('DY', 'DAY') THEN ROUND(CAST("AGE" AS FLOAT) / 365, 2)
            WHEN "AGE_COD" IN ('HR', 'HOUR') THEN ROUND(CAST("AGE" AS FLOAT) / 8760, 2)
            ELSE NULL  -- Handle unknown age codes explicitly
        END AS "AGE_Years_fixed"
    FROM "DEMO_Combined"
    WHERE "AGE" ~ '^[0-9\.]+$'  -- Ensure AGE is numeric
)
UPDATE "DEMO_Combined"
SET "AGE_Years_fixed" = ac."AGE_Years_fixed"
FROM AgeConversion ac
WHERE "DEMO_Combined"."DEMO_ID" = ac."DEMO_ID";

-- Debug: Check AGE_Years_fixed after update
SELECT COUNT(*) FROM "DEMO_Combined" WHERE "AGE_Years_fixed" IS NOT NULL;

-- 2. Standardize DEMO_Combined Country Code
ALTER TABLE "DEMO_Combined" ADD COLUMN IF NOT EXISTS "COUNTRY_CODE" VARCHAR(2);

UPDATE "DEMO_Combined"
SET "COUNTRY_CODE" = CASE
    WHEN LENGTH(reporter_country) = 2 THEN reporter_country
    ELSE NULL
END;

-- Debug: Check COUNTRY_CODE after update
SELECT COUNT(*) FROM "DEMO_Combined" WHERE "COUNTRY_CODE" IS NOT NULL;

-- 3. Standardize DEMO_Combined Gender
ALTER TABLE "DEMO_Combined" ADD COLUMN IF NOT EXISTS "Gender" VARCHAR(3);

UPDATE "DEMO_Combined"
SET "Gender" = CASE
    WHEN sex IN ('M', 'F') THEN sex  -- Keep only 'M' and 'F'
    ELSE NULL
END;

-- Debug: Check Gender after updates
SELECT COUNT(*) FROM "DEMO_Combined" WHERE "Gender" IS NOT NULL;

-- 4. Prepare for De-duplication: Combine Data
DROP TABLE IF EXISTS "Aligned_DEMO_DRUG_REAC_INDI_THER";

CREATE TABLE "Aligned_DEMO_DRUG_REAC_INDI_THER" AS
WITH CombinedData AS (
    SELECT
        d."DEMO_ID",
        d.caseid,
        d.primaryid,
        d.caseversion,
        d.fda_dt,
        d."I_F_COD",
        d.event_dt,
        d."AGE_Years_fixed",
        d."GENDER",
        d."COUNTRY_CODE",
        d.OCCP_COD,
        d."PERIOD",
        STRING_AGG(DISTINCT dr.drugname, '/' ORDER BY dr.DRUG_SEQ) AS Aligned_drugs,
        STRING_AGG(DISTINCT ic.MEDDRA_CODE::TEXT, '/' ORDER BY ic.indi_drug_seq, ic.MEDDRA_CODE) FILTER (WHERE ic.MEDDRA_CODE NOT IN (10070592, 10057097)) AS Aligned_INDI,
        STRING_AGG(DISTINCT th.START_DT::TEXT, '/' ORDER BY th.dsg_drug_seq, th.START_DT) AS Aligned_START_DATE,
        STRING_AGG(DISTINCT rc.MEDDRA_CODE::TEXT, '/' ORDER BY rc.MEDDRA_CODE) AS ALIGNED_REAC
    FROM "DEMO_Combined" d
    LEFT JOIN "DRUG_Combined" dr ON d.primaryid = dr.primaryid
    LEFT JOIN "INDI_Combined" ic ON d.primaryid = ic.primaryid
    LEFT JOIN "THER_Combined" th ON d.primaryid = th.primaryid
    LEFT JOIN "REAC_Combined" rc ON d.primaryid = rc.primaryid
    GROUP BY d."DEMO_ID", d.caseid, d.primaryid, d.caseversion, d.fda_dt, d."I_F_COD", d.event_dt, d."AGE_Years_fixed", d."GENDER", d."COUNTRY_CODE", d.OCCP_COD, d."PERIOD"
)
SELECT
    cd.*
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY caseid ORDER BY primaryid DESC, "PERIOD" DESC, caseversion DESC, fda_dt DESC, "I_F_COD" DESC, event_dt DESC) AS row_num
    FROM CombinedData
) cd
WHERE cd.row_num = 1;

-- Debug: Check ALIGNED_DEMO_DRUG_REAC_INDI_THER after creation
SELECT COUNT(*) FROM "ALIGNED_DEMO_DRUG_REAC_INDI_THER";

-- 5. De-duplication Steps (Full Match and Partial Matches)

-- Full match (all criteria)
WITH RankedRows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY event_dt, "AGE_Years_fixed", "GENDER", "COUNTRY_CODE", Aligned_drugs, Aligned_INDI, Aligned_START_DATE, ALIGNED_REAC ORDER BY primaryid DESC, "PERIOD" DESC) AS row_num
    FROM "Aligned_DEMO_DRUG_REAC_INDI_THER"
)
DELETE FROM "Aligned_DEMO_DRUG_REAC_INDI_THER"
WHERE (event_dt, "AGE_Years_fixed", "GENDER", "COUNTRY_CODE", Aligned_drugs, Aligned_INDI, Aligned_START_DATE, ALIGNED_REAC, primaryid, "PERIOD") IN (SELECT event_dt, "AGE_Years_fixed", "GENDER", "COUNTRY_CODE", Aligned_drugs, Aligned_INDI, Aligned_START_DATE, ALIGNED_REAC, primaryid, "PERIOD" FROM RankedRows WHERE row_num > 1);

-- Debug: Check after first de-duplication
SELECT COUNT(*) FROM "ALIGNED_DEMO_DRUG_REAC_INDI_THER";

-- Partial match (excluding event_dt)
WITH RankedRows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY "AGE_Years_fixed", "GENDER", "COUNTRY_CODE", Aligned_drugs, Aligned_INDI, Aligned_START_DATE, ALIGNED_REAC ORDER BY primaryid DESC, "PERIOD" DESC) AS row_num
    FROM "Aligned_DEMO_DRUG_REAC_INDI_THER"
)
DELETE FROM "Aligned_DEMO_DRUG_REAC_INDI_THER"
WHERE ("AGE_Years_fixed", "GENDER", "COUNTRY_CODE", Aligned_drugs, Aligned_INDI, Aligned_START_DATE, ALIGNED_REAC, primaryid, "PERIOD") IN (SELECT "AGE_Years_fixed", "GENDER", "COUNTRY_CODE", Aligned_drugs, Aligned_INDI, Aligned_START_DATE, ALIGNED_REAC, primaryid, "PERIOD" FROM RankedRows WHERE row_num > 1);

-- Debug: Check after second de-duplication
SELECT COUNT(*) FROM "ALIGNED_DEMO_DRUG_REAC_INDI_THER";

-- Partial match (excluding AGE_Years_fixed)
WITH RankedRows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY event_dt, "GENDER", "COUNTRY_CODE", Aligned_drugs, Aligned_INDI, Aligned_START_DATE, ALIGNED_REAC ORDER BY primaryid DESC, "PERIOD" DESC) AS row_num
    FROM "Aligned_DEMO_DRUG_REAC_INDI_THER"
)
DELETE FROM "Aligned_DEMO_DRUG_REAC_INDI_THER"
WHERE (event_dt, "GENDER", "COUNTRY_CODE", Aligned_drugs, Aligned_INDI, Aligned_START_DATE, ALIGNED_REAC, primaryid, "PERIOD") IN (SELECT event_dt, "GENDER", "COUNTRY_CODE", Aligned_drugs, Aligned_INDI, Aligned_START_DATE, ALIGNED_REAC, primaryid, "PERIOD" FROM RankedRows WHERE row_num > 1);

-- Debug: Check after third de-duplication
SELECT COUNT(*) FROM "ALIGNED_DEMO_DRUG_REAC_INDI_THER";

-- Partial match (excluding GENDER)
WITH RankedRows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY event_dt, "AGE_Years_fixed", "COUNTRY_CODE", Aligned_drugs, Aligned_INDI, Aligned_START_DATE, ALIGNED_REAC ORDER BY primaryid DESC, "PERIOD" DESC) AS row_num
    FROM "Aligned_DEMO_DRUG_REAC_INDI_THER"
)
DELETE FROM "Aligned_DEMO_DRUG_REAC_INDI_THER"
WHERE (event_dt, "AGE_Years_fixed", "COUNTRY_CODE", Aligned_drugs, Aligned_INDI, Aligned_START_DATE, ALIGNED_REAC, primaryid, "PERIOD") IN (SELECT event_dt, "AGE_Years_fixed", "COUNTRY_CODE", Aligned_drugs, Aligned_INDI, Aligned_START_DATE, ALIGNED_REAC, primaryid, "PERIOD" FROM RankedRows WHERE row_num > 1);

-- Debug: Check after fourth de-duplication
SELECT COUNT(*) FROM "ALIGNED_DEMO_DRUG_REAC_INDI_THER";

-- Partial match (excluding COUNTRY_CODE)
WITH RankedRows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY event_dt, "AGE_Years_fixed", "GENDER", Aligned_drugs, Aligned_INDI, Aligned_START_DATE, ALIGNED_REAC ORDER BY primaryid DESC, "PERIOD" DESC) AS row_num
    FROM "Aligned_DEMO_DRUG_REAC_INDI_THER"
)
DELETE FROM "Aligned_DEMO_DRUG_REAC_INDI_THER"
WHERE (event_dt, "AGE_Years_fixed", "GENDER", Aligned_drugs, Aligned_INDI, Aligned_START_DATE, ALIGNED_REAC, primaryid, "PERIOD") IN (SELECT event_dt, "AGE_Years_fixed", "GENDER", Aligned_drugs, Aligned_INDI, Aligned_START_DATE, ALIGNED_REAC, primaryid, "PERIOD" FROM RankedRows WHERE row_num > 1);

-- Debug: Check after fifth de-duplication
SELECT COUNT(*) FROM "ALIGNED_DEMO_DRUG_REAC_INDI_THER";

-- Partial match (excluding Aligned_INDI)
WITH RankedRows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY event_dt, "AGE_Years_fixed", "GENDER", "COUNTRY_CODE", Aligned_drugs, Aligned_START_DATE, ALIGNED_REAC ORDER BY primaryid DESC, "PERIOD" DESC) AS row_num
    FROM "Aligned_DEMO_DRUG_REAC_INDI_THER"
)
DELETE FROM "Aligned_DEMO_DRUG_REAC_INDI_THER"
WHERE (event_dt, "AGE_Years_fixed", "GENDER", "COUNTRY_CODE", Aligned_drugs, Aligned_START_DATE, ALIGNED_REAC, primaryid, "PERIOD") IN (SELECT event_dt, "AGE_Years_fixed", "GENDER", "COUNTRY_CODE", Aligned_drugs, Aligned_START_DATE, ALIGNED_REAC, primaryid, "PERIOD" FROM RankedRows WHERE row_num > 1);

-- Debug: Check after sixth de-duplication
SELECT COUNT(*) FROM "ALIGNED_DEMO_DRUG_REAC_INDI_THER";

-- Partial match (excluding Aligned_START_DATE)
WITH RankedRows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY event_dt, "AGE_Years_fixed", "GENDER", "COUNTRY_CODE", Aligned_drugs, Aligned_INDI, ALIGNED_REAC ORDER BY primaryid DESC, "PERIOD" DESC) AS row_num
    FROM "Aligned_DEMO_DRUG_REAC_INDI_THER"
)
DELETE FROM "Aligned_DEMO_DRUG_REAC_INDI_THER"
WHERE (event_dt, "AGE_Years_fixed", "GENDER", "COUNTRY_CODE", Aligned_drugs, Aligned_INDI, ALIGNED_REAC, primaryid, "PERIOD") IN (SELECT event_dt, "AGE_Years_fixed", "GENDER", "COUNTRY_CODE", Aligned_drugs, Aligned_INDI, ALIGNED_REAC, primaryid, "PERIOD" FROM RankedRows WHERE row_num > 1);

-- Debug: Check after seventh de-duplication
SELECT COUNT(*) FROM "ALIGNED_DEMO_DRUG_REAC_INDI_THER";

-- 6. Delete Cases from Deleted Reports
DELETE FROM "Aligned_DEMO_DRUG_REAC_INDI_THER"
WHERE CASEID IN (SELECT Field1 FROM "COMBINED_DELETED_CASES_REPORTS");

-- Debug: Check after deleting cases
SELECT COUNT(*) FROM "ALIGNED_DEMO_DRUG_REAC_INDI_THER";

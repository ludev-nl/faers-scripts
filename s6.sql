\set ON_ERROR_STOP 0
SET search_path TO faers_b, public;
/* Set schema search path */

-- Create helper function for consistent string cleaning
CREATE OR REPLACE FUNCTION clean_string(input TEXT)
RETURNS TEXT AS $$
DECLARE
    output TEXT := input;
BEGIN
    -- Remove patterns like /00174601/
    IF output SIMILAR TO '%/[0-9]{5}%/%' THEN
        output := REGEXP_REPLACE(output, '/[0-9]{5}.*?/', '');
    END IF;

    -- Extract content from parentheses if present
    IF POSITION('(' IN output) > 0 AND POSITION(')' IN output) > POSITION('(' IN output) THEN
        output := SUBSTRING(output FROM POSITION('(' IN output) + 1 FOR 
                           POSITION(')' IN output) - POSITION('(' IN output) - 1);
    END IF;

    -- Clean special characters and trim
    output := TRIM(BOTH ' :.,?/`~!@#$%^&*-_=+ ' FROM output);

    -- Replace common patterns
    output := REPLACE(output, ';', ' / ');
    output := REGEXP_REPLACE(output, '\s+', ' ');

    RETURN output;
END;
$$ LANGUAGE plpgsql;

-- Create indexes for performance optimization
-- Note: Partial indexes are handled in queries where needed
CREATE INDEX IF NOT EXISTS idx_products_at_fda_applno ON products_at_fda (applno);
CREATE INDEX IF NOT EXISTS idx_products_at_fda_rxaui ON products_at_fda (rxaui);
CREATE INDEX IF NOT EXISTS idx_drug_mapper_nda_num ON drug_mapper (nda_num);
CREATE INDEX IF NOT EXISTS idx_drug_mapper_notes ON drug_mapper (notes);
CREATE INDEX IF NOT EXISTS idx_rxnconso_str_sab_tty ON rxnconso (str, sab, tty);

-- Create products_at_fda table with optimized structure
DROP TABLE IF EXISTS products_at_fda;
CREATE TABLE products_at_fda (
    applno VARCHAR(10),
    productno VARCHAR(10),
    form TEXT,
    strength TEXT,
    referencedrug INTEGER,
    drugname TEXT,
    activeingredient TEXT,
    referencestandard INTEGER,
    rxaui VARCHAR(8),
    ai_2 TEXT
);

-- Load data into products_at_fda
COPY products_at_fda (
    applno, productno, form, strength, referencedrug, drugname, 
    activeingredient, referencestandard, rxaui, ai_2
)
FROM '/data/faers/FAERS_MAK/2.LoadDataToDatabase/drugsatfda20230627/Products.txt'
WITH (FORMAT CSV, DELIMITER '\t', NULL '', HEADER TRUE);

-- Process activeingredient in a single update
UPDATE products_at_fda
SET ai_2 = clean_string(activeingredient);

-- Consolidated rxaui mapping updates
BEGIN;
    -- First try with strict conditions
    UPDATE products_at_fda
    SET rxaui = rxnconso.rxaui
    FROM rxnconso
    WHERE products_at_fda.ai_2 = rxnconso.str
      AND rxnconso.sab = 'RXNORM'
      AND rxnconso.tty IN ('IN', 'MIN')
      AND products_at_fda.rxaui IS NULL;

    -- Then try with relaxed conditions
    UPDATE products_at_fda
    SET rxaui = rxnconso.rxaui
    FROM rxnconso
    WHERE products_at_fda.ai_2 = rxnconso.str
      AND products_at_fda.rxaui IS NULL;
COMMIT;

-- Start optimized mapping drugs to RxNorm
BEGIN;
    -- Mapping by NDA number first (most reliable)
    UPDATE drug_mapper
    SET rxaui = c.rxaui,
        rxcui = c.rxcui,
        notes = '1.0',
        sab = c.sab,
        tty = c.tty,
        str = c.str,
        code = c.code
    FROM products_at_fda b
    JOIN rxnconso c ON b.rxaui = c.rxaui
    WHERE drug_mapper.notes IS NULL
      AND drug_mapper.nda_num ~ '^[0-9]+$'
      AND b.rxaui IS NOT NULL
      AND POSITION('.' IN drug_mapper.nda_num) = 0
      AND LENGTH(drug_mapper.nda_num) < 6
      AND (CASE 
               WHEN LEFT(drug_mapper.nda_num, 1) = '0' 
               THEN RIGHT(drug_mapper.nda_num, LENGTH(drug_mapper.nda_num) - 1)
               ELSE drug_mapper.nda_num 
           END) = b.applno;

    -- Consolidated mapping by drug name and active ingredient
    -- Create temp table for cleaned names to avoid repeated calculations
    CREATE TEMP TABLE cleaned_drugs (
        id INTEGER,
        drugname TEXT,
        prod_ai TEXT,
        clean_drugname TEXT,
        clean_prodai TEXT
    );

    INSERT INTO cleaned_drugs
    SELECT id, 
           drugname, 
           prod_ai, 
           clean_string(drugname) AS clean_drugname, 
           clean_string(prod_ai) AS clean_prodai
    FROM drug_mapper
    WHERE notes IS NULL;

    CREATE INDEX idx_cleaned_drugs ON cleaned_drugs (id, clean_drugname, clean_prodai);

    -- Mapping by drug name with different conditions
    UPDATE drug_mapper
    SET rxaui = r.rxaui,
        rxcui = r.rxcui,
        notes = CASE 
            WHEN r.sab = 'RXNORM' AND r.tty = 'IN' THEN '1.1'
            WHEN r.sab = 'RXNORM' AND r.tty = 'MIN' THEN '1.2'
            WHEN r.sab = 'RXNORM' AND r.tty = 'PIN' THEN '1.2.2'
            WHEN r.sab = 'MTHSPL' THEN '1.3'
            WHEN r.tty = 'IN' THEN '1.4'
            WHEN r.sab = 'RXNORM' THEN '1.5'
            ELSE '1.6'
        END,
        sab = r.sab,
        tty = r.tty,
        str = r.str,
        code = r.code
    FROM cleaned_drugs cd
    JOIN rxnconso r ON cd.clean_drugname = r.str
    WHERE drug_mapper.id = cd.id
      AND drug_mapper.notes IS NULL
      AND (
          (r.sab = 'RXNORM' AND r.tty IN ('IN', 'MIN', 'PIN')) OR
          (r.sab = 'MTHSPL') OR
          (r.tty = 'IN') OR
          (r.sab = 'RXNORM')
      );

    -- Mapping by product active ingredient
    UPDATE drug_mapper
    SET rxaui = r.rxaui,
        rxcui = r.rxcui,
        notes = CASE 
            WHEN r.sab = 'RXNORM' AND r.tty = 'IN' THEN '2.1'
            WHEN r.sab = 'RXNORM' AND r.tty = 'MIN' THEN '2.2'
            WHEN r.sab = 'RXNORM' AND r.tty = 'PIN' THEN '2.2.2'
            WHEN r.sab = 'MTHSPL' THEN '2.3'
            WHEN r.tty = 'IN' THEN '2.4'
            WHEN r.sab = 'RXNORM' THEN '2.5'
            ELSE '2.6'
        END,
        sab = r.sab,
        tty = r.tty,
        str = r.str,
        code = r.code
    FROM cleaned_drugs cd
    JOIN rxnconso r ON cd.clean_prodai = r.str
    WHERE drug_mapper.id = cd.id
      AND drug_mapper.notes IS NULL
      AND (
          (r.sab = 'RXNORM' AND r.tty IN ('IN', 'MIN', 'PIN')) OR
          (r.sab = 'MTHSPL') OR
          (r.tty = 'IN') OR
          (r.sab = 'RXNORM')
      );

    -- Clean up temp table
    DROP TABLE cleaned_drugs;
COMMIT;

-- Optimized IDD mapping
DROP TABLE IF EXISTS idd;
CREATE TABLE idd (
    drugname TEXT,
    rxaui INTEGER,
    rxcui INTEGER,
    str TEXT,
    sab VARCHAR(50),
    tty VARCHAR(10),
    code VARCHAR(50)
);

-- Load data into idd
COPY idd (
    drugname, rxaui, rxcui, str, sab, tty, code
)
FROM '/data/faers/FAERS_MAK/2.LoadDataToDatabase/9nmgzttxhm-1/IDD_V.1.txt'
WITH (FORMAT CSV, DELIMITER '\t', NULL '', HEADER TRUE);

CREATE INDEX idx_idd_drugname ON idd (drugname);
CREATE INDEX idx_idd_rxaui ON idd (rxaui);

-- Consolidated IDD mapping updates
BEGIN;
    -- Mapping by drug name with different conditions
    UPDATE drug_mapper
    SET notes = CASE 
            WHEN i.sab = 'RXNORM' AND i.tty IN ('IN', 'MIN', 'PIN') THEN '6.1'
            WHEN i.tty = 'IN' THEN '6.4'
            ELSE '6.5'
        END,
        rxaui = i.rxaui,
        rxcui = i.rxcui,
        sab = i.sab,
        tty = i.tty,
        str = i.str,
        code = i.code
    FROM idd i
    WHERE drug_mapper.drugname = i.drugname
      AND drug_mapper.notes IS NULL
      AND i.rxaui IS NOT NULL;

    -- Mapping by product active ingredient
    UPDATE drug_mapper
    SET notes = CASE 
            WHEN i.sab = 'RXNORM' AND i.tty IN ('IN', 'MIN', 'PIN') THEN '6.2'
            WHEN i.tty = 'IN' THEN '6.4'
            ELSE '6.6'
        END,
        rxaui = i.rxaui,
        rxcui = i.rxcui,
        sab = i.sab,
        tty = i.tty,
        str = i.str,
        code = i.code
    FROM idd i
    WHERE drug_mapper.prod_ai = i.drugname
      AND drug_mapper.notes IS NULL
      AND i.rxaui IS NOT NULL;
COMMIT;

-- Create optimized manual mapping table
DROP TABLE IF EXISTS manual_mapping;
CREATE TABLE manual_mapping (
    drugname TEXT,
    count INTEGER,
    rxaui BIGINT,
    rxcui BIGINT,
    sab VARCHAR(20),
    tty VARCHAR(20),
    str TEXT,
    code VARCHAR(50),
    notes TEXT
);

-- Insert unmapped drugs with high counts
INSERT INTO manual_mapping (count, drugname)
SELECT COUNT(drugname) AS count, drugname
FROM drug_mapper
WHERE notes IS NULL
GROUP BY drugname
HAVING COUNT(drugname) > 199;

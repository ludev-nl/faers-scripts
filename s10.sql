SET search_path TO faers_b;

-- Create logging table for errors and progress
CREATE TABLE IF NOT EXISTS remapping_log (
    log_id SERIAL PRIMARY KEY,
    step VARCHAR(50),
    message TEXT,
    log_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_rxnconso_rxcui ON rxnconso(rxcui) INCLUDE (rxaui, str, sab, tty, code);
CREATE INDEX IF NOT EXISTS idx_rxnrel_rxcui ON rxnrel(rxcui1, rxcui2) INCLUDE (rxaui1, rxaui2, rela);
CREATE INDEX IF NOT EXISTS idx_drug_mapper_2_remapping ON drug_mapper_2(remapping_rxcui, remapping_rxaui) INCLUDE (drug_id, remapping_notes);

-- Step 1: Initial RXNORM Update
CREATE OR REPLACE FUNCTION step_1() RETURNS VOID AS $$
BEGIN
    UPDATE drug_mapper
    SET remapping_rxcui = rxcui, remapping_rxaui = rxaui, remapping_str = str,
        remapping_sab = sab, remapping_tty = tty, remapping_code = code, remapping_notes = '1'
    WHERE sab = 'RXNORM' AND tty = 'IN' AND remapping_notes IS NULL;
    INSERT INTO remapping_log (step, message) VALUES ('Initial RXNORM Update', 'Step 1 completed successfully.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('Initial RXNORM Update', 'Error in step 1: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Step 2: Create drug_mapper_2
CREATE OR REPLACE FUNCTION step_2() RETURNS VOID AS $$
BEGIN
    DROP TABLE IF EXISTS drug_mapper_2;
    CREATE TABLE drug_mapper_2 AS
    SELECT drug_id, primaryid, drug_seq, role_cod, period, drugname, prod_ai, notes,
           c.rxaui, c.rxcui, c.str, c.sab, c.tty, c.code,
           CASE WHEN a.rxaui IS NULL THEN remapping_notes ELSE '2' END AS remapping_notes,
           b.rela,
           CASE WHEN a.rxaui IS NULL THEN remapping_rxaui ELSE a.rxaui END AS remapping_rxaui,
           CASE WHEN a.rxcui IS NULL THEN remapping_rxcui ELSE a.rxcui END AS remapping_rxcui,
           CASE WHEN a.str IS NULL THEN remapping_str ELSE a.str END AS remapping_str,
           CASE WHEN a.sab IS NULL THEN remapping_sab ELSE a.sab END AS remapping_sab,
           CASE WHEN a.tty IS NULL THEN remapping_tty ELSE a.tty END AS remapping_tty,
           CASE WHEN a.code IS NULL THEN remapping_code ELSE a.code END AS remapping_code
    FROM rxnconso AS a
    INNER JOIN rxnrel AS b ON a.rxcui = b.rxcui1 AND a.tty = 'IN' AND a.sab = 'RXNORM'
    RIGHT OUTER JOIN drug_mapper AS c ON b.rxcui2 = c.rxcui
    WHERE remapping_notes IS NULL;
    INSERT INTO remapping_log (step, message) VALUES ('Create drug_mapper_2', 'Step 2 completed successfully.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('Create drug_mapper_2', 'Error in step 2: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Step 3: Manual Remapping Update
CREATE OR REPLACE FUNCTION step_3() RETURNS VOID AS $$
BEGIN
    UPDATE drug_mapper_2
    SET remapping_notes = 'MAN_REM /', remapping_rxaui = h.last_rxaui,
        remapping_rxcui = r.rxcui, remapping_str = r.str, remapping_sab = r.sab,
        remapping_tty = r.tty, remapping_code = r.code
    FROM hopefully_last_one_5_7_2021 h
    INNER JOIN rxnconso r ON h.last_rxaui = r.rxaui
    WHERE drug_mapper_2.str = h.str AND drug_mapper_2.rxaui = h.rxaui
    AND drug_mapper_2.remapping_notes IS NULL;
    INSERT INTO remapping_log (step, message) VALUES ('Manual Remapping Update', 'Step MAN_REM / completed successfully.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('Manual Remapping Update', 'Error in step MAN_REM /: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Step 4: Manual Remapping Insert
CREATE OR REPLACE FUNCTION step_4() RETURNS VOID AS $$
BEGIN
    INSERT INTO drug_mapper_2
    (drug_id, primaryid, drug_seq, role_cod, period, drugname, prod_ai, notes, rxaui, rxcui,
     str, sab, tty, code, remapping_notes, rela, remapping_rxaui, remapping_rxcui, remapping_sab,
     remapping_tty, remapping_code, remapping_str)
    SELECT d.drug_id, d.primaryid, d.drug_seq, d.role_cod, d.period, d.drugname, d.prod_ai, d.notes,
           d.rxaui, d.rxcui, d.str, d.sab, d.tty, d.code, 'MAN2/' || d.remapping_notes, d.rela,
           a.last_rxaui AS remapping_rxaui, r.rxcui AS remapping_rxcui, r.sab AS remapping_sab,
           r.tty AS remapping_tty, r.code AS remapping_code, r.str AS remapping_str
    FROM drug_mapper_2 d
    INNER JOIN hopefully_last_one_5_7_2021 a ON a.rxaui = d.rxaui AND a.str = d.str
    INNER JOIN rxnconso r ON a.last_rxaui = r.rxaui
    WHERE a.last_rxaui IS NOT NULL AND d.remapping_notes LIKE 'MAN_REM /%';
    INSERT INTO remapping_log (step, message) VALUES ('Manual Remapping Insert', 'Step MAN2/ completed successfully.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('Manual Remapping Insert', 'Error in step MAN2/: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Step 5: Manual Remapping Delete
CREATE OR REPLACE FUNCTION step_5() RETURNS VOID AS $$
BEGIN
    DELETE FROM drug_mapper_2 WHERE remapping_notes LIKE 'MAN_REM /%';
    INSERT INTO remapping_log (step, message) VALUES ('Manual Remapping Delete', 'Step MAN_REM / completed successfully.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('Manual Remapping Delete', 'Error in step MAN_REM /: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Step 6: VANDF Relationships
CREATE OR REPLACE FUNCTION step_6() RETURNS VOID AS $$
BEGIN
    INSERT INTO drug_mapper_2
    (drug_id, primaryid, drug_seq, role_cod, period, drugname, prod_ai, notes, rxaui, rxcui, str, sab,
     tty, code, rela, remapping_notes, remapping_rxaui, remapping_rxcui, remapping_str, remapping_sab,
     remapping_tty, remapping_code)
    SELECT e.drug_id, e.primaryid, e.drug_seq, e.role_cod, e.period, e.drugname, e.prod_ai, e.notes,
           e.rxaui, e.rxcui, e.str, e.sab, e.tty, e.code, e.rela, '3',
           a.rxaui AS remapping_rxaui, a.rxcui AS remapping_rxcui, a.str AS remapping_str,
           a.sab AS remapping_sab, a.tty AS remapping_tty, a.code AS remapping_code
    FROM rxnconso a
    INNER JOIN rxnrel b ON a.rxcui = b.rxcui1
    INNER JOIN rxnconso c ON b.rxcui2 = c.rxcui
    INNER JOIN rxnrel d ON c.rxcui = d.rxcui1 AND d.rela = 'HAS_INGREDIENTS' AND c.sab = 'VANDF' AND c.tty = 'IN'
    INNER JOIN drug_mapper_2 e ON d.rxcui2 = e.rxcui AND e.remapping_notes IS NULL;
    INSERT INTO remapping_log (step, message) VALUES ('VANDF Relationships', 'Step 3 completed successfully.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('VANDF Relationships', 'Error in step 3: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Step 7: MMSL to RXNORM Insert
CREATE OR REPLACE FUNCTION step_7() RETURNS VOID AS $$
BEGIN
    INSERT INTO drug_mapper_2
    (drug_id, primaryid, drug_seq, role_cod, period, drugname, prod_ai, notes, rxaui, rxcui, str, sab,
     tty, code, remapping_notes, remapping_rxaui, remapping_rxcui, remapping_str, remapping_sab,
     remapping_tty, remapping_code)
    SELECT DISTINCT e.drug_id, e.primaryid, e.drug_seq, e.role_cod, e.period, e.drugname, e.prod_ai, e.notes,
           e.rxaui, e.rxcui, e.str, e.sab, e.tty, e.code, '7',
           c1.rxaui AS remapping_rxaui, c1.rxcui AS remapping_rxcui, c1.str AS remapping_str,
           c1.sab AS remapping_sab, c1.tty AS remapping_tty, c1.code AS remapping_code
    FROM drug_mapper_2 e
    INNER JOIN rxnrel r ON e.rxcui = r.rxcui2
    INNER JOIN rxnconso c ON r.rxcui1 = c.rxcui
    INNER JOIN rxnrel r1 ON r.rxcui1 = r1.rxcui2
    INNER JOIN rxnconso c1 ON r1.rxcui1 = c1.rxcui
    WHERE e.sab = 'MMSL' AND c1.sab = 'RXNORM' AND c1.tty = 'IN' AND c.sab = 'RXNORM'
    AND c.tty = 'SCDC' AND c1.rxaui != 11794211 AND e.remapping_notes IS NULL;
    INSERT INTO remapping_log (step, message) VALUES ('MMSL to RXNORM Insert', 'Step 7 completed successfully.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('MMSL to RXNORM Insert', 'Error in step 7: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Step 8: RXNORM SCDC to IN Insert
CREATE OR REPLACE FUNCTION step_8() RETURNS VOID AS $$
BEGIN
    INSERT INTO drug_mapper_2
    (drug_id, primaryid, drug_seq, role_cod, period, drugname, prod_ai, notes, rxaui, rxcui, str, sab,
     tty, code, remapping_notes, remapping_rxaui, remapping_rxcui, remapping_str, remapping_sab,
     remapping_tty, remapping_code)
    SELECT DISTINCT e.drug_id, e.primaryid, e.drug_seq, e.role_cod, e.period, e.drugname, e.prod_ai, e.notes,
           e.rxaui, e.rxcui, e.str, e.sab, e.tty, e.code, '8',
           c1.rxaui AS remapping_rxaui, c1.rxcui AS remapping_rxcui, c1.str AS remapping_str,
           c1.sab AS remapping_sab, c1.tty AS remapping_tty, c1.code AS remapping_code
    FROM drug_mapper_2 e
    INNER JOIN rxnrel r ON e.rxcui = r.rxcui2
    INNER JOIN rxnconso c ON r.rxcui1 = c.rxcui
    INNER JOIN rxnrel r1 ON r.rxcui1 = r1.rxcui2
    INNER JOIN rxnconso c1 ON r1.rxcui1 = c1.rxcui
    WHERE c.sab = 'RXNORM' AND c.tty = 'SCDC' AND c1.sab = 'RXNORM' AND c1.tty = 'IN'
    AND e.remapping_notes IS NULL;
    INSERT INTO remapping_log (step, message) VALUES ('RXNORM SCDC to IN Insert', 'Step 8 completed successfully.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('RXNORM SCDC to IN Insert', 'Error in step 8: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Step 9: RXNORM IN Update with Notes
CREATE OR REPLACE FUNCTION step_9() RETURNS VOID AS $$
BEGIN
    UPDATE drug_mapper_2
    SET remapping_rxcui = c.rxcui, remapping_rxaui = c.rxaui, remapping_str = c.str,
        remapping_sab = c.sab, remapping_tty = c.tty, remapping_notes = '9'
    FROM rxnconso c
    RIGHT OUTER JOIN rxnrel r ON c.rxaui = r.rxaui1
    RIGHT OUTER JOIN drug_mapper_2 ON r.rxaui2 = drug_mapper_2.rxaui
    WHERE c.sab = 'RXNORM' AND c.tty = 'IN' AND drug_mapper_2.notes IS NOT NULL
    AND drug_mapper_2.remapping_notes IS NULL;
    INSERT INTO remapping_log (step, message) VALUES ('RXNORM IN Update with Notes', 'Step 9 completed successfully.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('RXNORM IN Update with Notes', 'Error in step 9: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Step 10: MTHSPL to RXNORM IN Insert
CREATE OR REPLACE FUNCTION step_10() RETURNS VOID AS $$
BEGIN
    INSERT INTO drug_mapper_2
    (drug_id, primaryid, drug_seq, role_cod, period, drugname, prod_ai, notes, rxaui, rxcui, str, sab,
     tty, code, remapping_notes, remapping_rxaui, remapping_rxcui, remapping_str, remapping_sab,
     remapping_tty, remapping_code)
    SELECT DISTINCT e.drug_id, e.primaryid, e.drug_seq, e.role_cod, e.period, e.drugname, e.prod_ai, e.notes,
           e.rxaui, e.rxcui, e.str, e.sab, e.tty, e.code, '9',
           c1.rxaui AS remapping_rxaui, c1.rxcui AS remapping_rxcui, c1.str AS remapping_str,
           c1.sab AS remapping_sab, c1.tty AS remapping_tty, c1.code AS remapping_code
    FROM drug_mapper_2 e
    INNER JOIN rxnrel r ON e.rxaui = r.rxaui2
    INNER JOIN rxnconso c ON r.rxaui1 = c.rxaui
    INNER JOIN rxnrel r1 ON r.rxaui1 = r1.rxaui2 AND r.rela = 'HAS_ACTIVE_MOIETY'
    INNER JOIN rxnconso c1 ON r1.rxaui1 = c1.rxaui
    WHERE c1.sab = 'RXNORM' AND c1.tty = 'IN' AND c.sab = 'MTHSPL'
    AND e.remapping_notes IS NULL;
    INSERT INTO remapping_log (step, message) VALUES ('MTHSPL to RXNORM IN Insert', 'Step 9 completed successfully.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('MTHSPL to RXNORM IN Insert', 'Error in step 9: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Step 11: RXNORM IN Update
CREATE OR REPLACE FUNCTION step_11() RETURNS VOID AS $$
BEGIN
    UPDATE drug_mapper_2
    SET remapping_rxcui = c.rxcui, remapping_rxaui = c.rxaui, remapping_str = c.str,
        remapping_sab = c.sab, remapping_tty = c.tty, remapping_code = c.code, remapping_notes = '10'
    FROM rxnconso c
    INNER JOIN drug_mapper_2 ON drug_mapper_2.rxcui = c.rxcui
    WHERE c.tty = 'IN' AND drug_mapper_2.remapping_notes IS NULL;
    INSERT INTO remapping_log (step, message) VALUES ('RXNORM IN Update', 'Step 10 completed successfully.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('RXNORM IN Update', 'Error in step 10: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Step 12: MMSL to RXNORM IN Insert with Exclusions
CREATE OR REPLACE FUNCTION step_12() RETURNS VOID AS $$
BEGIN
    INSERT INTO drug_mapper_2
    (drug_id, primaryid, drug_seq, role_cod, period, drugname, prod_ai, notes, rxaui, rxcui, str, sab,
     tty, code, remapping_notes, remapping_rxaui, remapping_rxcui, remapping_str, remapping_sab,
     remapping_tty, remapping_code)
    SELECT DISTINCT e.drug_id, e.primaryid, e.drug_seq, e.role_cod, e.period, e.drugname, e.prod_ai, e.notes,
           e.rxaui, e.rxcui, e.str, e.sab, e.tty, e.code, '11',
           c1.rxaui AS remapping_rxaui, c1.rxcui AS remapping_rxcui, c1.str AS remapping_str,
           c1.sab AS remapping_sab, c1.tty AS remapping_tty, c1.code AS remapping_code
    FROM drug_mapper_2 e
    INNER JOIN rxnrel r ON e.rxaui = r.rxaui2
    INNER JOIN rxnconso c ON r.rxaui1 = c.rxaui
    INNER JOIN rxnrel r1 ON r.rxaui1 = r1.rxaui2
    INNER JOIN rxnconso c1 ON r1.rxaui1 = c1.rxaui
    WHERE e.sab = 'MMSL' AND c1.sab = 'RXNORM' AND c1.tty = 'IN'
    AND c1.rxaui NOT IN (2604414, 1182299, 1173735, 1287235) AND e.remapping_notes IS NULL;
    INSERT INTO remapping_log (step, message) VALUES ('MMSL to RXNORM IN Insert with Exclusions', 'Step 11 completed successfully scalar expression expected');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('MMSL to RXNORM IN Insert with Exclusions', 'Error in step 11: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Step 13: RXNORM Cleanup Update
CREATE OR REPLACE FUNCTION step_13() RETURNS VOID AS $$
BEGIN
    UPDATE drug_mapper_2
    SET remapping_notes = '12', remapping_rxaui = c.rxaui, remapping_rxcui = c.rxcui,
        remapping_str = c.str, remapping_sab = c.sab, remapping_tty = c.tty, remapping_code = c.code
    FROM drug_mapper_2
    INNER JOIN rxnconso c ON c.str = CASE
        WHEN POSITION('(' IN remapping_str) > 0 AND POSITION(')' IN remapping_str) > 0
        AND POSITION('(' IN remapping_str) < POSITION(')' IN remapping_str)
        THEN SUBSTRING(remapping_str FROM 1 FOR POSITION('(' IN remapping_str) - 1)
             || SUBSTRING(remapping_str FROM POSITION(')' IN remapping_str) + 1)
        ELSE remapping_str
        END
    WHERE c.sab = 'RXNORM' AND c.tty IN ('IN', 'MIN', 'PIN')
    AND remapping_notes IN ('9', '10', '11');
    INSERT INTO remapping_log (step, message) VALUES ('RXNORM Cleanup Update', 'Step 12 completed successfully.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('RXNORM Cleanup Update', 'Error in step 12: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Step 14: Mark for Deletion
CREATE OR REPLACE FUNCTION step_14() RETURNS VOID AS $$
BEGIN
    UPDATE drug_mapper_2
    SET remapping_notes = 'TO BE DELETED'
    FROM drug_mapper_2
    INNER JOIN rxnrel ON drug_mapper_2.remapping_rxcui = rxnrel.rxcui2
    INNER JOIN rxnconso ON rxnrel.rxcui1 = rxnconso.rxcui
    WHERE rxnconso.sab = 'RXNORM' AND rxnconso.tty = 'IN' AND remapping_notes = '12';
    INSERT INTO remapping_log (step, message) VALUES ('Mark for Deletion', 'Step TO BE DELETED completed successfully.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('Mark for Deletion', 'Error in step TO BE DELETED: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Step 15: Reinsert from Deleted
CREATE OR REPLACE FUNCTION step_15() RETURNS VOID AS $$
BEGIN
    INSERT INTO drug_mapper_2
    (drug_id, primaryid, drug_seq, role_cod, period, drugname, prod_ai, notes, rxaui, rxcui, str, sab,
     tty, code, rela, remapping_notes, remapping_rxcui, remapping_sab, remapping_tty, remapping_code,
     remapping_str, remapping_rxaui)
    SELECT d.drug_id, d.primaryid, d.drug_seq, d.role_cod, d.period, d.drugname, d.prod_ai, d.notes,
           d.rxaui, d.rxcui, d.str, d.sab, d.tty, d.code, d.rela, '13',
           c.rxcui AS remapping_rxcui, c.sab AS remapping_sab, c.tty AS remapping_tty,
           c.code AS remapping_code, c.str AS remapping_str, c.rxaui AS remapping_rxaui
    FROM drug_mapper_2 d
    INNER JOIN rxnrel r ON d.remapping_rxcui = r.rxcui2
    INNER JOIN rxnconso c ON r.rxcui1 = c.rxcui
    WHERE d.remapping_notes = 'TO BE DELETED' AND c.sab = 'RXNORM' AND c.tty = 'IN';
    INSERT INTO remapping_log (step, message) VALUES ('Reinsert from Deleted', 'Step 13 completed successfully.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('Reinsert from Deleted', 'Error in step 13: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Step 16: Delete Marked Rows
CREATE OR REPLACE FUNCTION step_16() RETURNS VOID AS $$
BEGIN
    DELETE FROM drug_mapper_2 WHERE remapping_notes = 'TO BE DELETED';
    INSERT INTO remapping_log (step, message) VALUES ('Delete Marked Rows', 'Step TO BE DELETED completed successfully.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('Delete Marked Rows', 'Error in step TO BE DELETED: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Step 17: Clean Duplicates
CREATE OR REPLACE FUNCTION step_17() RETURNS VOID AS $$
BEGIN
    DELETE FROM drug_mapper_2
    WHERE (drug_id, rxaui, remapping_rxaui) IN (
        SELECT drug_id, rxaui, remapping_rxaui
        FROM (
            SELECT drug_id, rxaui, remapping_rxaui,
                   ROW_NUMBER() OVER (PARTITION BY drug_id, rxaui, remapping_rxaui ORDER BY drug_id, rxaui, remapping_rxaui) AS row_num
            FROM drug_mapper_2
        ) t
        WHERE row_num > 1
    );
    INSERT INTO remapping_log (step, message) VALUES ('Clean Duplicates', 'Step N/A completed successfully.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('Clean Duplicates', 'Error in step N/A: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Step 18: Update RXAUI Mappings
CREATE OR REPLACE FUNCTION step_18() RETURNS VOID AS $$
BEGIN
    UPDATE drug_mapper_2
    SET remapping_rxcui = c.rxcui, remapping_str = c.str, remapping_sab = c.sab,
        remapping_tty = c.tty, remapping_code = c.code
    FROM rxnconso c
    INNER JOIN drug_mapper_2 ON drug_mapper_2.remapping_rxaui = c.rxaui
    WHERE remapping_rxaui IS NOT NULL;
    INSERT INTO remapping_log (step, message) VALUES ('Update RXAUI Mappings', 'Step N/A completed successfully.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('Update RXAUI Mappings', 'Error in step N/A: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Step 19: Non-RXNORM SAB Update
CREATE OR REPLACE FUNCTION step_19() RETURNS VOID AS $$
BEGIN
    UPDATE drug_mapper_2
    SET remapping_rxaui = c.rxaui, remapping_rxcui = c.rxcui, remapping_str = c.str,
        remapping_sab = c.sab, remapping_tty = c.tty, remapping_code = c.code, remapping_notes = '14'
    FROM rxnconso c
    INNER JOIN drug_mapper_2 ON drug_mapper_2.remapping_rxcui = c.rxcui
    WHERE c.sab = 'RXNORM' AND c.tty = 'IN' AND drug_mapper_2.sab != 'RXNORM';
    INSERT INTO remapping_log (step, message) VALUES ('Non-RXNORM SAB Update', 'Step 14 completed successfully.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('Non-RXNORM SAB Update', 'Error in step 14: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Step 20: RXNORM SAB Specific Update
CREATE OR REPLACE FUNCTION step_20() RETURNS VOID AS $$
BEGIN
    UPDATE drug_mapper_2
    SET remapping_rxaui = c.rxaui, remapping_rxcui = c.rxcui, remapping_str = c.str,
        remapping_sab = c.sab, remapping_tty = c.tty, remapping_code = c.code, remapping_notes = '15'
    FROM rxnconso c
    INNER JOIN drug_mapper_2 ON drug_mapper_2.remapping_rxcui = c.rxcui
    WHERE c.sab = 'RXNORM' AND c.tty = 'IN' AND drug_mapper_2.sab = 'RXNORM'
    AND drug_mapper_2.tty NOT IN ('IN', 'MIN');
    INSERT INTO remapping_log (step, message) VALUES ('RXNORM SAB Specific Update', 'Step 15 completed successfully.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('RXNORM SAB Specific Update', 'Error in step 15: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Function to clean duplicates (retained as a separate function)
CREATE OR REPLACE FUNCTION clean_duplicates() RETURNS VOID AS $$
BEGIN
    DELETE FROM drug_mapper_2
    WHERE (drug_id, rxaui, remapping_rxaui) IN (
        SELECT drug_id, rxaui, remapping_rxaui
        FROM (
            SELECT drug_id, rxaui, remapping_rxaui,
                   ROW_NUMBER() OVER (PARTITION BY drug_id, rxaui, remapping_rxaui ORDER BY drug_id, rxaui, remapping_rxaui) AS row_num
            FROM drug_mapper_2
        ) t
        WHERE row_num > 1
    );
    INSERT INTO remapping_log (step, message) VALUES ('CleanDuplicates', 'Duplicates removed from drug_mapper_2.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('CleanDuplicates', 'Error: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Function to populate manual_remapper
CREATE OR REPLACE FUNCTION populate_manual_remapper() RETURNS VOID AS $$
BEGIN
    CREATE TABLE IF NOT EXISTS manual_remapper (
        count INTEGER,
        source_drugname VARCHAR(3000),
        source_rxaui VARCHAR(8),
        source_rxcui VARCHAR(8),
        source_sab VARCHAR(20),
        source_tty VARCHAR(20),
        final_rxaui BIGINT,
        notes VARCHAR(100)
    );
    INSERT INTO manual_remapper (count, source_drugname, source_rxaui, source_rxcui, source_sab, source_tty)
    SELECT COUNT(drugname) AS count, drugname, remapping_rxaui, remapping_rxcui, remapping_sab, remapping_tty
    FROM drug_mapper_2
    WHERE remapping_notes IS NOT NULL
    GROUP BY drugname, remapping_rxaui, remapping_rxcui, remapping_sab, remapping_tty;
    INSERT INTO remapping_log (step, message) VALUES ('PopulateManualRemapper', 'manual_remapper populated.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('PopulateManualRemapper', 'Error: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Function to merge manual remappings into drug_mapper_3
CREATE OR REPLACE FUNCTION merge_manual_remappings() RETURNS VOID AS $$
BEGIN
    DROP TABLE IF EXISTS drug_mapper_3;
    CREATE TABLE drug_mapper_3 AS
    SELECT d.drug_id, d.primaryid, d.drug_seq, d.role_cod, d.period, d.drugname, d.prod_ai, d.notes,
           d.rxaui, d.rxcui, d.str, d.sab, d.tty, d.code, m.final_rxaui,
           r.rxcui AS remapping_rxcui, r.str AS remapping_str, r.sab AS remapping_sab,
           r.tty AS remapping_tty, r.code AS remapping_code
    FROM manual_remapper m
    INNER JOIN rxnconso r ON m.final_rxaui = r.rxaui
    RIGHT OUTER JOIN drug_mapper_2 d ON m.source_rxcui = d.remapping_rxcui;
    DELETE FROM drug_mapper_3
    WHERE (drug_id, drug_seq, final_rxaui) IN (
        SELECT drug_id, drug_seq, final_rxaui
        FROM (
            SELECT drug_id, drug_seq, final_rxaui,
                   ROW_NUMBER() OVER (PARTITION BY drug_id, drug_seq, final_rxaui ORDER BY period) AS row_num
            FROM drug_mapper_3
        ) t
        WHERE row_num > 1
    );
    INSERT INTO remapping_log (step, message) VALUES ('MergeManualRemappings', 'drug_mapper_3 created and duplicates removed.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('MergeManualRemappings', 'Error: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Driver function to orchestrate remapping
CREATE OR REPLACE FUNCTION run_all_remapping_steps() RETURNS VOID AS $$
BEGIN
    PERFORM step_1();
    PERFORM step_2();
    PERFORM step_3();
    PERFORM step_4();
    PERFORM step_5();
    PERFORM step_6();
    PERFORM step_7();
    PERFORM step_8();
    PERFORM step_9();
    PERFORM step_10();
    PERFORM step_11();
    PERFORM step_12();
    PERFORM step_13();
    PERFORM step_14();
    PERFORM step_15();
    PERFORM step_16();
    PERFORM step_17();
    PERFORM step_18();
    PERFORM step_19();
    PERFORM step_20();
    PERFORM clean_duplicates();
    PERFORM populate_manual_remapper();
    -- Note: Manual remapping in MS Access occurs here, then load back to PostgreSQL
    PERFORM merge_manual_remappings();
    INSERT INTO remapping_log (step, message) VALUES ('RunRemapping', 'All remapping steps completed.');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO remapping_log (step, message) VALUES ('RunRemapping', 'Error: ' || SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Execute the driver function
SELECT run_all_remapping_steps();
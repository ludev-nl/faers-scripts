-- Ensure schema exists
CREATE SCHEMA IF NOT EXISTS faers_b;

-- Set the working schema
SET search_path TO faers_b, public;

-- Only define and execute the functions if DRUG_Mapper table exists
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = 'drug_mapper' 
        AND table_schema IN ('faers_b', 'public')
    ) THEN

        -- Define clearnumericcharacters function
        CREATE OR REPLACE FUNCTION clearnumericcharacters(input_text TEXT) 
        RETURNS TEXT AS $func$ 
        BEGIN   
            RETURN regexp_replace(input_text, '[0-9]', '', 'g'); 
        END; 
        $func$ LANGUAGE plpgsql;

        -- Define main processing function
        CREATE OR REPLACE FUNCTION process_drug_data() 
        RETURNS void AS $func$ 
        DECLARE     
            phase_data JSONB;     
            stmt RECORD;     
            phase_name TEXT;
        BEGIN    
            -- Initial temp table     
            DROP TABLE IF EXISTS DRUG_Mapper_Temp;     
            CREATE TABLE DRUG_Mapper_Temp AS     
            SELECT DISTINCT DRUGNAME, PROD_AI, CLEANED_DRUGNAME, CLEANED_PROD_AI       
            FROM DRUG_Mapper     
            WHERE NOTES IS NULL;      

            -- ── PHASE 1: UNITS_OF_MEASUREMENT_DRUGNAME ──
            phase_name := 'UNITS_OF_MEASUREMENT_DRUGNAME';
            phase_data := pg_read_file('config_s8.json')::jsonb;
            FOR stmt IN SELECT * FROM jsonb_array_elements(phase_data -> (phase_name)::text -> 'replacements') LOOP
                EXECUTE format(
                    'UPDATE %I SET %I = REPLACE(%I, %L, %L)',
                    stmt->>'table',
                    stmt->>'set_column',
                    stmt->>'replace_column',
                    stmt->>'find',
                    stmt->>'replace'
                );
            END LOOP;
            
            -- Hard-coded operations after UNITS_OF_MEASUREMENT_DRUGNAME
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_DRUGNAME = clearnumericcharacters(CLEANED_DRUGNAME);
            
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_DRUGNAME = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_DRUGNAME, CHR(10), ''), CHR(13), ''), CHR(9), '')));

            -- ── PHASE 2: MANUFACTURER_NAMES_DRUGNAME ──
            phase_name := 'UNITS_OF_MEASUREMENT_DRUGNAME';
            phase_data := pg_read_file('config_s8.json')::jsonb;
            FOR stmt IN SELECT * FROM jsonb_array_elements(phase_data -> (phase_name)::text -> 'replacements') LOOP
                EXECUTE format(
                    'UPDATE %I SET %I = REPLACE(%I, %L, %L)',
                    stmt->>'table',
                    stmt->>'set_column',
                    stmt->>'replace_column',
                    stmt->>'find',
                    stmt->>'replace'
                );
            END LOOP;
            
            -- Hard-coded operations after MANUFACTURER_NAMES_DRUGNAME
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_DRUGNAME = clearnumericcharacters(CLEANED_DRUGNAME);

            -- ── PHASE 3: WORDS_TO_VITAMIN_B_DRUGNAME ──
            phase_name := 'WORDS_TO_VITAMIN_B_DRUGNAME';
            phase_data := pg_read_file('config_s8.json')::jsonb;
            FOR stmt IN SELECT * FROM jsonb_array_elements(phase_data -> (phase_name)::text -> 'replacements') LOOP
                EXECUTE format(
                    'UPDATE %I SET %I = REPLACE(%I, %L, %L)',
                    stmt->>'table',
                    stmt->>'set_column',
                    stmt->>'replace_column',
                    stmt->>'find',
                    stmt->>'replace'
                );
            END LOOP;
            
            -- No hard-coded operations after WORDS_TO_VITAMIN_B_DRUGNAME

            -- ── PHASE 4: FORMAT_DRUGNAME ──
            phase_name := 'FORMAT_DRUGNAME';
            phase_data := pg_read_file('config_s8.json')::jsonb;
            FOR stmt IN SELECT * FROM jsonb_array_elements(phase_data -> (phase_name)::text -> 'replacements') LOOP
                EXECUTE format(
                    'UPDATE %I SET %I = REPLACE(%I, %L, %L)',
                    stmt->>'table',
                    stmt->>'set_column',
                    stmt->>'replace_column',
                    stmt->>'find',
                    stmt->>'replace'
                );
            END LOOP;
            
            -- Hard-coded operations after FORMAT_DRUGNAME
            -- Trim special characters (this is fine and duplicated intentionally)
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_DRUGNAME = TRIM(BOTH ' ":.,?/\`~!@#$%^&*-_=+ ' FROM CLEANED_DRUGNAME);
            
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_DRUGNAME = TRIM(BOTH ' ":.,?/\`~!@#$%^&*-_=+ ' FROM CLEANED_DRUGNAME);

            -- Whitespace and control character cleanup (corrected)
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_DRUGNAME = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_DRUGNAME, CHR(10), ' '), CHR(13), ' '), CHR(9), ' ')));
            
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_DRUGNAME = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_DRUGNAME, CHR(10), ' '), CHR(13), ' '), CHR(9), ' ')));
            
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_DRUGNAME = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_DRUGNAME, CHR(10), ''), CHR(13), ''), CHR(9), '')));

            -- ── PHASE 5: CLEANING_DRUGNAME ──
            phase_name := 'CLEANING_DRUGNAME';
            phase_data := pg_read_file('config_s8.json')::jsonb;
            FOR stmt IN SELECT * FROM jsonb_array_elements(phase_data -> (phase_name)::text -> 'replacements') LOOP
                EXECUTE format(
                    'UPDATE %I SET %I = REPLACE(%I, %L, %L)',
                    stmt->>'table',
                    stmt->>'set_column',
                    stmt->>'replace_column',
                    stmt->>'find',
                    stmt->>'replace'
                );
            END LOOP;
            
            -- Hard-coded operations after CLEANING_DRUGNAME (suffix removal for DRUGNAME)
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_DRUGNAME = CASE 
                WHEN RIGHT(CLEANED_DRUGNAME, 5) = ' JELL' THEN LEFT(CLEANED_DRUGNAME, LENGTH(CLEANED_DRUGNAME)-5)
                WHEN RIGHT(CLEANED_DRUGNAME, 4) = ' NOS' THEN LEFT(CLEANED_DRUGNAME, LENGTH(CLEANED_DRUGNAME)-4)
                WHEN RIGHT(CLEANED_DRUGNAME, 4) = ' GEL' THEN LEFT(CLEANED_DRUGNAME, LENGTH(CLEANED_DRUGNAME)-4)
                WHEN RIGHT(CLEANED_DRUGNAME, 4) = ' CAP' THEN LEFT(CLEANED_DRUGNAME, LENGTH(CLEANED_DRUGNAME)-4)
                WHEN RIGHT(CLEANED_DRUGNAME, 4) = ' TAB' THEN LEFT(CLEANED_DRUGNAME, LENGTH(CLEANED_DRUGNAME)-4)
                WHEN RIGHT(CLEANED_DRUGNAME, 4) = ' FOR' THEN LEFT(CLEANED_DRUGNAME, LENGTH(CLEANED_DRUGNAME)-4)
                WHEN RIGHT(CLEANED_DRUGNAME, 2) = '//' THEN LEFT(CLEANED_DRUGNAME, LENGTH(CLEANED_DRUGNAME)-2)
                WHEN RIGHT(CLEANED_DRUGNAME, 1) = '/' THEN LEFT(CLEANED_DRUGNAME, LENGTH(CLEANED_DRUGNAME)-1)
                ELSE CLEANED_DRUGNAME
            END
            WHERE RIGHT(CLEANED_DRUGNAME, 5) = ' JELL'
            OR RIGHT(CLEANED_DRUGNAME, 4) IN (' NOS', ' GEL', ' CAP', ' TAB', ' FOR')
            OR RIGHT(CLEANED_DRUGNAME, 2) = '//'
            OR RIGHT(CLEANED_DRUGNAME, 1) = '/';

            -- Initial PROD_AI whitespace cleanup (from original sequence)
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_PROD_AI, CHR(10), ''), CHR(13), ''), CHR(9), '')));
            
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_PROD_AI, CHR(10), ''), CHR(13), ''), CHR(9), '')));
            
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_PROD_AI, CHR(10), ''), CHR(13), ''), CHR(9), '')));

            -- ── PHASE 6: UNITS_MEASUREMENT_PROD_AI ──
            phase_name := 'UNITS_MEASUREMENT_PROD_AI';
            phase_data := pg_read_file('config_s8.json')::jsonb;
            FOR stmt IN SELECT * FROM jsonb_array_elements(phase_data -> (phase_name)::text -> 'replacements') LOOP
                EXECUTE format(
                    'UPDATE %I SET %I = REPLACE(%I, %L, %L)',
                    stmt->>'table',
                    stmt->>'set_column',
                    stmt->>'replace_column',
                    stmt->>'find',
                    stmt->>'replace'
                );
            END LOOP;
            
            -- Hard-coded operations after UNITS_MEASUREMENT_PROD_AI
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = clearnumericcharacters(CLEANED_PROD_AI);
            
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_PROD_AI, CHR(10), ''), CHR(13), ''), CHR(9), '')));

            -- ── PHASE 7: MANUFACTURER_NAMES_PROD_AI ──
            phase_name := 'MANUFACTURER_NAMES_PROD_AI';
            phase_data := pg_read_file('config_s8.json')::jsonb;
            FOR stmt IN SELECT * FROM jsonb_array_elements(phase_data -> (phase_name)::text -> 'replacements') LOOP
                EXECUTE format(
                    'UPDATE %I SET %I = REPLACE(%I, %L, %L)',
                    stmt->>'table',
                    stmt->>'set_column',
                    stmt->>'replace_column',
                    stmt->>'find',
                    stmt->>'replace'
                );
            END LOOP;
            
            -- Hard-coded operations after MANUFACTURER_NAMES_PROD_AI
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = clearnumericcharacters(CLEANED_PROD_AI);

            -- ── PHASE 8: WORDS_TO_VITAMIN_B_PROD_AI ──
            phase_name := 'WORDS_TO_VITAMIN_B_PROD_AI';
            phase_data := pg_read_file('config_s8.json')::jsonb;
            FOR stmt IN SELECT * FROM jsonb_array_elements(phase_data -> (phase_name)::text -> 'replacements') LOOP
                EXECUTE format(
                    'UPDATE %I SET %I = REPLACE(%I, %L, %L)',
                    stmt->>'table',
                    stmt->>'set_column',
                    stmt->>'replace_column',
                    stmt->>'find',
                    stmt->>'replace'
                );
            END LOOP;
            
            -- No hard-coded operations after WORDS_TO_VITAMIN_B_PROD_AI

            -- ── PHASE 9: FORMAT_PROD_AI ──
            phase_name := 'FORMAT_PROD_AI';
            phase_data := pg_read_file('config_s8.json')::jsonb;
            FOR stmt IN SELECT * FROM jsonb_array_elements(phase_data -> (phase_name)::text -> 'replacements') LOOP
                EXECUTE format(
                    'UPDATE %I SET %I = REPLACE(%I, %L, %L)',
                    stmt->>'table',
                    stmt->>'set_column',
                    stmt->>'replace_column',
                    stmt->>'find',
                    stmt->>'replace'
                );
            END LOOP;
            
            -- Hard-coded operations after FORMAT_PROD_AI
            -- Trim special characters (no error here, both lines are valid and duplicate by design)
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = TRIM(BOTH ' ":.,?/\`~!@#$%^&*-_=+ ' FROM CLEANED_PROD_AI);

            -- Corrected lines with properly matched parentheses
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_PROD_AI, CHR(10), ' '), CHR(13), ' '), CHR(9), ' ')));

            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_PROD_AI, CHR(10), ' '), CHR(13), ' '), CHR(9), ' ')));

            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_PROD_AI, CHR(10), ' '), CHR(13), ' '), CHR(9), ' ')));

            -- Repeat of trim special characters (also fine if intentional)
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = TRIM(BOTH ' ":.,?/\`~!@#$%^&*-_=+ ' FROM CLEANED_PROD_AI);
            
            -- PROD_AI specific slash and dot normalization
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = REPLACE(REPLACE(CLEANED_PROD_AI, '/ /', '/'), '/ /', '/');
            
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = REPLACE(CLEANED_PROD_AI, '///', '/');
            
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = REPLACE(CLEANED_PROD_AI, '/ / /', '/');
            
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = REPLACE(CLEANED_PROD_AI, '////', '/');
            
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = REPLACE(CLEANED_PROD_AI, '/ / / /', '/');
            
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = REPLACE(CLEANED_PROD_AI, '.', '');

            -- ── PHASE 10: CLEANING_PROD_AI ──
            phase_name := 'CLEANING_PROD_AI';
            phase_data := pg_read_file('config_s8.json')::jsonb;
            FOR stmt IN SELECT * FROM jsonb_array_elements(phase_data -> (phase_name)::text -> 'replacements') LOOP
                EXECUTE format(
                    'UPDATE %I SET %I = REPLACE(%I, %L, %L)',
                    stmt->>'table',
                    stmt->>'set_column',
                    stmt->>'replace_column',
                    stmt->>'find',
                    stmt->>'replace'
                );
            END LOOP;
            
            -- Hard-coded operations after CLEANING_PROD_AI
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_PROD_AI, CHR(10), ''), CHR(13), ''), CHR(9), '')));
            
            -- Suffix removal for PROD_AI (includes JELL that DRUGNAME doesn't have)
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = CASE 
                WHEN RIGHT(CLEANED_PROD_AI, 5) = ' JELL' THEN LEFT(CLEANED_PROD_AI, LENGTH(CLEANED_PROD_AI)-5)
                WHEN RIGHT(CLEANED_PROD_AI, 4) = ' NOS' THEN LEFT(CLEANED_PROD_AI, LENGTH(CLEANED_PROD_AI)-4)
                WHEN RIGHT(CLEANED_PROD_AI, 4) = ' GEL' THEN LEFT(CLEANED_PROD_AI, LENGTH(CLEANED_PROD_AI)-4)
                WHEN RIGHT(CLEANED_PROD_AI, 4) = ' CAP' THEN LEFT(CLEANED_PROD_AI, LENGTH(CLEANED_PROD_AI)-4)
                WHEN RIGHT(CLEANED_PROD_AI, 4) = ' TAB' THEN LEFT(CLEANED_PROD_AI, LENGTH(CLEANED_PROD_AI)-4)
                WHEN RIGHT(CLEANED_PROD_AI, 4) = ' FOR' THEN LEFT(CLEANED_PROD_AI, LENGTH(CLEANED_PROD_AI)-4)
                WHEN RIGHT(CLEANED_PROD_AI, 2) = '//' THEN LEFT(CLEANED_PROD_AI, LENGTH(CLEANED_PROD_AI)-2)
                WHEN RIGHT(CLEANED_PROD_AI, 1) = '/' THEN LEFT(CLEANED_PROD_AI, LENGTH(CLEANED_PROD_AI)-1)
                ELSE CLEANED_PROD_AI
            END
            WHERE RIGHT(CLEANED_PROD_AI, 5) = ' JELL'
            OR RIGHT(CLEANED_PROD_AI, 4) IN (' NOS', ' GEL', ' CAP', ' TAB', ' FOR')
            OR RIGHT(CLEANED_PROD_AI, 2) = '//'
            OR RIGHT(CLEANED_PROD_AI, 1) = '/';

            -- Final PROD_AI whitespace cleanup (from original sequence)
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_PROD_AI, CHR(10), ''), CHR(13), ''), CHR(9), '')));
            
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_PROD_AI, CHR(10), ''), CHR(13), ''), CHR(9), '')));

        END; 
        $func$ LANGUAGE plpgsql;
        PERFORM process_drug_data();    
    ELSE
        RAISE NOTICE 'Skipping function creation: DRUG_Mapper table not found.';
    END IF;
END$$;
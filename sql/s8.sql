-- Ensure schema exists
CREATE SCHEMA IF NOT EXISTS faers_b;

-- Set the working schema
SET search_path TO faers_b, public;

ALTER TABLE DRUG_Mapper ADD COLUMN IF NOT EXISTS CLEANED_DRUGNAME TEXT;
ALTER TABLE DRUG_Mapper ADD COLUMN IF NOT EXISTS CLEANED_PROD_AI TEXT;

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
            current_phase TEXT;
        BEGIN  
            -- Step 0: Ensure CLEANED_* columns are initialized from DRUGNAME / PROD_AI if NULL
            UPDATE DRUG_Mapper
            SET CLEANED_DRUGNAME = DRUGNAME
            WHERE CLEANED_DRUGNAME IS NULL AND DRUGNAME IS NOT NULL;

            UPDATE DRUG_Mapper
            SET CLEANED_PROD_AI = PROD_AI
            WHERE CLEANED_PROD_AI IS NULL AND PROD_AI IS NOT NULL;

            -- Step 0.1: Remove numeric suffixes like /00032/
            UPDATE DRUG_Mapper
            SET CLEANED_DRUGNAME = regexp_replace(CLEANED_DRUGNAME, '/[0-9]{5}/', '', 'g')
            WHERE NOTES IS NULL;

            UPDATE DRUG_Mapper
            SET CLEANED_PROD_AI = regexp_replace(CLEANED_PROD_AI, '/[0-9]{5}/', '', 'g')
            WHERE NOTES IS NULL;

            -- Step 0.2: Normalize common delimiters and whitespace
            UPDATE DRUG_Mapper
            SET CLEANED_DRUGNAME = regexp_replace(CLEANED_DRUGNAME, E'[\\n\\r\\t]+', '', 'g')
            WHERE NOTES IS NULL;

            UPDATE DRUG_Mapper
            SET CLEANED_DRUGNAME = regexp_replace(CLEANED_DRUGNAME, '[|,+;\\\\]', '/', 'g')
            WHERE NOTES IS NULL;

            UPDATE DRUG_Mapper
            SET CLEANED_DRUGNAME = regexp_replace(CLEANED_DRUGNAME, '/+', ' / ', 'g')
            WHERE NOTES IS NULL;

            UPDATE DRUG_Mapper
            SET CLEANED_DRUGNAME = regexp_replace(CLEANED_DRUGNAME, '\\s{2,}', ' ', 'g')
            WHERE NOTES IS NULL;

            UPDATE DRUG_Mapper
            SET CLEANED_PROD_AI = regexp_replace(CLEANED_PROD_AI, E'[\\n\\r\\t]+', '', 'g')
            WHERE NOTES IS NULL;

            UPDATE DRUG_Mapper
            SET CLEANED_PROD_AI = regexp_replace(CLEANED_PROD_AI, '[|,+;\\\\]', '/', 'g')
            WHERE NOTES IS NULL;

            UPDATE DRUG_Mapper
            SET CLEANED_PROD_AI = regexp_replace(CLEANED_PROD_AI, '/+', ' / ', 'g')
            WHERE NOTES IS NULL;

            UPDATE DRUG_Mapper
            SET CLEANED_PROD_AI = regexp_replace(CLEANED_PROD_AI, '\\s{2,}', ' ', 'g')
            WHERE NOTES IS NULL;

            -- Step 0.3: Strip parenthesis content iteratively
            FOR i IN 1..5 LOOP
                UPDATE DRUG_Mapper
                SET CLEANED_DRUGNAME = regexp_replace(CLEANED_DRUGNAME, '\\([^()]*\\)', '', 'g')
                WHERE CLEANED_DRUGNAME ~ '\\([^()]*\\)' AND NOTES IS NULL;

                UPDATE DRUG_Mapper
                SET CLEANED_PROD_AI = regexp_replace(CLEANED_PROD_AI, '\\([^()]*\\)', '', 'g')
                WHERE CLEANED_PROD_AI ~ '\\([^()]*\\)' AND NOTES IS NULL;
            END LOOP;

            -- Initial temp table     
            DROP TABLE IF EXISTS DRUG_Mapper_Temp;     
            CREATE TABLE DRUG_Mapper_Temp AS     
            SELECT DISTINCT DRUGNAME, PROD_AI, CLEANED_DRUGNAME, CLEANED_PROD_AI       
            FROM DRUG_Mapper     
            WHERE NOTES IS NULL;      

            -- ── PHASE 1: UNITS_OF_MEASUREMENT_DRUGNAME ──
            current_phase := 'UNITS_OF_MEASUREMENT_DRUGNAME';
            SELECT cfg.config_data INTO phase_data 
            FROM temp_s8_config AS cfg
            WHERE cfg.phase_name = current_phase;
            
            IF phase_data IS NOT NULL THEN
                FOR stmt IN SELECT * FROM jsonb_array_elements(phase_data -> 'replacements') LOOP
                    EXECUTE format(
                        'UPDATE %I SET %I = REPLACE(%I, %L, %L)',
                        stmt.value->>'table',
                        stmt.value->>'set_column',
                        stmt.value->>'replace_column',
                        stmt.value->>'find',
                        stmt.value->>'replace'
                    );
                END LOOP;
            END IF;
            
            -- Hard-coded operations after UNITS_OF_MEASUREMENT_DRUGNAME
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_DRUGNAME = clearnumericcharacters(CLEANED_DRUGNAME);
            
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_DRUGNAME = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_DRUGNAME, CHR(10), ''), CHR(13), ''), CHR(9), '')));

            -- ── PHASE 2: MANUFACTURER_NAMES_DRUGNAME ──
            current_phase := 'MANUFACTURER_NAMES_DRUGNAME';
            SELECT cfg.config_data INTO phase_data 
            FROM temp_s8_config AS cfg
            WHERE cfg.phase_name = current_phase;
            
            IF phase_data IS NOT NULL THEN
                FOR stmt IN SELECT * FROM jsonb_array_elements(phase_data -> 'replacements') LOOP
                    EXECUTE format(
                        'UPDATE %I SET %I = REPLACE(%I, %L, %L)',
                        stmt.value->>'table',
                        stmt.value->>'set_column',
                        stmt.value->>'replace_column',
                        stmt.value->>'find',
                        stmt.value->>'replace'
                    );
                END LOOP;
            END IF;
            
            -- Hard-coded operations after MANUFACTURER_NAMES_DRUGNAME
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_DRUGNAME = clearnumericcharacters(CLEANED_DRUGNAME);

            -- ── PHASE 3: WORDS_TO_VITAMIN_B_DRUGNAME ──
            current_phase := 'WORDS_TO_VITAMIN_B_DRUGNAME';
            SELECT cfg.config_data INTO phase_data 
            FROM temp_s8_config AS cfg
            WHERE cfg.phase_name = current_phase;
            
            IF phase_data IS NOT NULL THEN
                FOR stmt IN SELECT * FROM jsonb_array_elements(phase_data -> 'replacements') LOOP
                    EXECUTE format(
                        'UPDATE %I SET %I = REPLACE(%I, %L, %L)',
                        stmt.value->>'table',
                        stmt.value->>'set_column',
                        stmt.value->>'replace_column',
                        stmt.value->>'find',
                        stmt.value->>'replace'
                    );
                END LOOP;
            END IF;

            -- ── PHASE 4: FORMAT_DRUGNAME ──
            current_phase := 'FORMAT_DRUGNAME';
            SELECT cfg.config_data INTO phase_data 
            FROM temp_s8_config AS cfg
            WHERE cfg.phase_name = current_phase;
            
            IF phase_data IS NOT NULL THEN
                FOR stmt IN SELECT * FROM jsonb_array_elements(phase_data -> 'replacements') LOOP
                    EXECUTE format(
                        'UPDATE %I SET %I = REPLACE(%I, %L, %L)',
                        stmt.value->>'table',
                        stmt.value->>'set_column',
                        stmt.value->>'replace_column',
                        stmt.value->>'find',
                        stmt.value->>'replace'
                    );
                END LOOP;
            END IF;
            
            -- Hard-coded operations after FORMAT_DRUGNAME
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_DRUGNAME = TRIM(BOTH ' ":.,?/\`~!@#$%^&*-_=+ ' FROM CLEANED_DRUGNAME);
            
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_DRUGNAME = TRIM(BOTH ' ":.,?/\`~!@#$%^&*-_=+ ' FROM CLEANED_DRUGNAME);

            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_DRUGNAME = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_DRUGNAME, CHR(10), ' '), CHR(13), ' '), CHR(9), ' ')));
            
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_DRUGNAME = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_DRUGNAME, CHR(10), ' '), CHR(13), ' '), CHR(9), ' ')));
            
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_DRUGNAME = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_DRUGNAME, CHR(10), ''), CHR(13), ''), CHR(9), '')));

            -- ── PHASE 5: CLEANING_DRUGNAME ──
            current_phase := 'CLEANING_DRUGNAME';
            SELECT cfg.config_data INTO phase_data 
            FROM temp_s8_config AS cfg
            WHERE cfg.phase_name = current_phase;
            
            IF phase_data IS NOT NULL THEN
                FOR stmt IN SELECT * FROM jsonb_array_elements(phase_data -> 'replacements') LOOP
                    EXECUTE format(
                        'UPDATE %I SET %I = REPLACE(%I, %L, %L)',
                        stmt.value->>'table',
                        stmt.value->>'set_column',
                        stmt.value->>'replace_column',
                        stmt.value->>'find',
                        stmt.value->>'replace'
                    );
                END LOOP;
            END IF;
            
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

            -- Initial PROD_AI whitespace cleanup
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_PROD_AI, CHR(10), ''), CHR(13), ''), CHR(9), '')));
            
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_PROD_AI, CHR(10), ''), CHR(13), ''), CHR(9), '')));
            
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_PROD_AI, CHR(10), ''), CHR(13), ''), CHR(9), '')));

            -- ── PHASE 6: UNITS_MEASUREMENT_PROD_AI ──
            current_phase := 'UNITS_MEASUREMENT_PROD_AI';
            SELECT cfg.config_data INTO phase_data 
            FROM temp_s8_config AS cfg
            WHERE cfg.phase_name = current_phase;
            
            IF phase_data IS NOT NULL THEN
                FOR stmt IN SELECT * FROM jsonb_array_elements(phase_data -> 'replacements') LOOP
                    EXECUTE format(
                        'UPDATE %I SET %I = REPLACE(%I, %L, %L)',
                        stmt.value->>'table',
                        stmt.value->>'set_column',
                        stmt.value->>'replace_column',
                        stmt.value->>'find',
                        stmt.value->>'replace'
                    );
                END LOOP;
            END IF;
            
            -- Hard-coded operations after UNITS_MEASUREMENT_PROD_AI
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = clearnumericcharacters(CLEANED_PROD_AI);
            
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_PROD_AI, CHR(10), ''), CHR(13), ''), CHR(9), '')));

            -- ── PHASE 7: MANUFACTURER_NAMES_PROD_AI ──
            current_phase := 'MANUFACTURER_NAMES_PROD_AI';
            SELECT cfg.config_data INTO phase_data 
            FROM temp_s8_config AS cfg
            WHERE cfg.phase_name = current_phase;
            
            IF phase_data IS NOT NULL THEN
                FOR stmt IN SELECT * FROM jsonb_array_elements(phase_data -> 'replacements') LOOP
                    EXECUTE format(
                        'UPDATE %I SET %I = REPLACE(%I, %L, %L)',
                        stmt.value->>'table',
                        stmt.value->>'set_column',
                        stmt.value->>'replace_column',
                        stmt.value->>'find',
                        stmt.value->>'replace'
                    );
                END LOOP;
            END IF;
            
            -- Hard-coded operations after MANUFACTURER_NAMES_PROD_AI
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = clearnumericcharacters(CLEANED_PROD_AI);

            -- ── PHASE 8: WORDS_TO_VITAMIN_B_PROD_AI ──
            current_phase := 'WORDS_TO_VITAMIN_B_PROD_AI';
            SELECT cfg.config_data INTO phase_data 
            FROM temp_s8_config AS cfg
            WHERE cfg.phase_name = current_phase;
            
            IF phase_data IS NOT NULL THEN
                FOR stmt IN SELECT * FROM jsonb_array_elements(phase_data -> 'replacements') LOOP
                    EXECUTE format(
                        'UPDATE %I SET %I = REPLACE(%I, %L, %L)',
                        stmt.value->>'table',
                        stmt.value->>'set_column',
                        stmt.value->>'replace_column',
                        stmt.value->>'find',
                        stmt.value->>'replace'
                    );
                END LOOP;
            END IF;

            -- ── PHASE 9: FORMAT_PROD_AI ──
            current_phase := 'FORMAT_PROD_AI';
            SELECT cfg.config_data INTO phase_data 
            FROM temp_s8_config AS cfg
            WHERE cfg.phase_name = current_phase;
            
            IF phase_data IS NOT NULL THEN
                FOR stmt IN SELECT * FROM jsonb_array_elements(phase_data -> 'replacements') LOOP
                    EXECUTE format(
                        'UPDATE %I SET %I = REPLACE(%I, %L, %L)',
                        stmt.value->>'table',
                        stmt.value->>'set_column',
                        stmt.value->>'replace_column',
                        stmt.value->>'find',
                        stmt.value->>'replace'
                    );
                END LOOP;
            END IF;
            
            -- Hard-coded operations after FORMAT_PROD_AI
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = TRIM(BOTH ' ":.,?/\`~!@#$%^&*-_=+ ' FROM CLEANED_PROD_AI);

            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_PROD_AI, CHR(10), ' '), CHR(13), ' '), CHR(9), ' ')));

            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_PROD_AI, CHR(10), ' '), CHR(13), ' '), CHR(9), ' ')));

            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_PROD_AI, CHR(10), ' '), CHR(13), ' '), CHR(9), ' ')));

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
            current_phase := 'CLEANING_PROD_AI';
            SELECT cfg.config_data INTO phase_data 
            FROM temp_s8_config AS cfg
            WHERE cfg.phase_name = current_phase;
            
            IF phase_data IS NOT NULL THEN
                FOR stmt IN SELECT * FROM jsonb_array_elements(phase_data -> 'replacements') LOOP
                    EXECUTE format(
                        'UPDATE %I SET %I = REPLACE(%I, %L, %L)',
                        stmt.value->>'table',
                        stmt.value->>'set_column',
                        stmt.value->>'replace_column',
                        stmt.value->>'find',
                        stmt.value->>'replace'
                    );
                END LOOP;
            END IF;
            
            -- Hard-coded operations after CLEANING_PROD_AI
            UPDATE DRUG_Mapper_Temp 
            SET CLEANED_PROD_AI = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_PROD_AI, CHR(10), ''), CHR(13), ''), CHR(9), '')));
            
            -- Suffix removal for PROD_AI
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

            -- Final PROD_AI whitespace cleanup
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
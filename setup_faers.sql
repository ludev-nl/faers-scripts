-- setup_faers.sql: Initialize FAERS database schemas and functions

-- Set client encoding to UTF-8
SET client_encoding = 'UTF8';

-- Create schemas
CREATE SCHEMA IF NOT EXISTS faers_a;
CREATE SCHEMA IF NOT EXISTS faers_combined;

-- Set search path
SET search_path TO faers_a, faers_combined, public;

-- Function to determine completed year-quarter combinations
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
    y := 2000 + start_year; -- Start year relative to 2000 (e.g., 4 = 2004)
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

-- Log setup completion
DO $$
BEGIN
    RAISE NOTICE 'FAERS database setup completed: schemas faers_a, faers_combined created, function get_completed_year_quarters defined';
END $$;
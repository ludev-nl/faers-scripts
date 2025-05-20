-- Ensure this file is saved in UTF-8 encoding without BOM

/****** CREATE FAERS_A DATABASE  **********/
-- Ensure the database exists (run this separately if needed)
-- CREATE DATABASE faers_a;

/****** CONFIGURE DATABASE  **********/
-- Set client encoding to UTF-8
SET client_encoding = 'UTF8';

/*
Function to determine completed year-quarter combinations from the start year to the last completed quarter.
*/
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
    y := 2000 + start_year; -- Assuming start_year is relative to 2000
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

USE FAERS_B
GO

-- Create a string cleaning function to standardize all cleaning operations
CREATE OR ALTER FUNCTION dbo.CleanDrugString(@input VARCHAR(MAX))
RETURNS VARCHAR(MAX)
AS
BEGIN
    DECLARE @output VARCHAR(MAX) = @input;
    
    -- Remove numeric patterns like /00032601/
    IF PATINDEX('%/[0-9][0-9][0-9][0-9][0-9]%/%', @output) > 0    
        SET @output = LEFT(@output, PATINDEX('%/[0-9][0-9][0-9][0-9][0-9]%/%', @output) - 1);
    
    -- Remove country patterns like /USA/
    IF CHARINDEX('   /', @output) > 0
        SET @output = LEFT(@output, CHARINDEX('   /', @output) - 1);
    
    -- Standardize separators
    SET @output = REPLACE(@output, '|', '/');
    SET @output = REPLACE(@output, ',', '/');
    SET @output = REPLACE(@output, '+', '/');
    SET @output = REPLACE(@output, ';', ' / ');
    SET @output = REPLACE(@output, '\', '/');
    
    -- Clean dashes and hyphens
    SET @output = REPLACE(@output, ' -', ' ');
    SET @output = REPLACE(@output, ' –', ' ');
    SET @output = REPLACE(@output, '–', ' ');
    
    -- Remove control characters
    SET @output = REPLACE(REPLACE(REPLACE(@output, CHAR(10), ''), CHAR(13), ''), CHAR(9), '');
    
    -- Standardize slashes
    SET @output = REPLACE(@output, '/', ' / ');
    
    -- Trim and clean extra spaces
    SET @output = LTRIM(RTRIM(@output));
    SET @output = REPLACE(@output, '  ', ' ');
    
    RETURN @output;
END;
GO

-- Create a function to extract content from parentheses
CREATE OR ALTER FUNCTION dbo.ExtractFromParentheses(@input VARCHAR(MAX))
RETURNS VARCHAR(MAX)
AS
BEGIN
    RETURN CASE 
        WHEN CHARINDEX('(', @input) > 0 AND CHARINDEX(')', @input) > CHARINDEX('(', @input) 
        THEN SUBSTRING(@input, CHARINDEX('(', @input)+1, CHARINDEX(')', @input)-CHARINDEX('(', @input)-1)
        ELSE NULL 
    END;
END;
GO

-- Create a function to remove parentheses and their content
CREATE OR ALTER FUNCTION dbo.RemoveParentheses(@input VARCHAR(MAX))
RETURNS VARCHAR(MAX)
AS
BEGIN
    RETURN CASE 
        WHEN CHARINDEX('(', @input) > 0 AND CHARINDEX(')', @input) > CHARINDEX('(', @input) 
        THEN STUFF(@input, CHARINDEX('(', @input), (CHARINDEX(')', @input) - CHARINDEX('(', @input)) + 1, '')
        ELSE @input
    END;
END;
GO

-- Add cleaned columns with proper data types
ALTER TABLE DRUG_Mapper
ADD CLEANED_DRUGNAME VARCHAR(600),
    CLEANED_PROD_AI VARCHAR(500);
GO

-- Create indexes for performance
CREATE INDEX idx_DRUG_Mapper_Notes ON DRUG_Mapper(NOTES) WHERE NOTES IS NULL;
CREATE INDEX idx_DRUG_Mapper_Cleaned ON DRUG_Mapper(CLEANED_DRUGNAME, CLEANED_PROD_AI) WHERE NOTES IS NULL;
GO

-- Perform all cleaning in a single update per column
BEGIN TRANSACTION;
    -- Clean drug names
    UPDATE DRUG_Mapper
    SET CLEANED_DRUGNAME = dbo.CleanDrugString(DRUGNAME)
    WHERE NOTES IS NULL;
    
    -- Clean product active ingredients
    UPDATE DRUG_Mapper
    SET CLEANED_PROD_AI = dbo.CleanDrugString(PROD_AI)
    WHERE NOTES IS NULL;
COMMIT TRANSACTION;
GO

-- Optimized mapping updates
BEGIN TRANSACTION;
    -- Create temp table for cleaned data to avoid repeated calculations
    SELECT 
        DM.ID,
        DM.CLEANED_DRUGNAME,
        DM.CLEANED_PROD_AI,
        dbo.ExtractFromParentheses(DM.CLEANED_DRUGNAME) AS DrugName_InsideParens,
        dbo.ExtractFromParentheses(DM.CLEANED_PROD_AI) AS ProdAI_InsideParens,
        dbo.RemoveParentheses(DM.CLEANED_DRUGNAME) AS DrugName_NoParens,
        dbo.RemoveParentheses(DM.CLEANED_PROD_AI) AS ProdAI_NoParens
    INTO #CleanedDrugData
    FROM DRUG_Mapper DM
    WHERE DM.NOTES IS NULL;
    
    CREATE INDEX idx_CleanedDrugData ON #CleanedDrugData(ID);
    
    -- Consolidated mapping updates for RXNCONSO
    -- Match with content inside parentheses (RXNORM, TTY IN/MIN/PIN)
    UPDATE DM
    SET RXAUI = R.RXAUI, 
        RXCUI = R.RXCUI, 
        NOTES = '8.1-8.4',
        SAB = R.SAB, 
        TTY = R.TTY, 
        STR = R.STR, 
        CODE = R.CODE
    FROM DRUG_Mapper DM
    JOIN #CleanedDrugData CD ON DM.ID = CD.ID
    JOIN RXNCONSO R ON R.STR = CD.DrugName_InsideParens OR R.STR = CD.ProdAI_InsideParens
    WHERE DM.NOTES IS NULL
    AND R.SAB = 'RXNORM'
    AND R.TTY IN ('MIN', 'IN', 'PIN');
    
    -- Match with content outside parentheses (RXNORM, TTY IN/MIN/PIN)
    UPDATE DM
    SET RXAUI = R.RXAUI, 
        RXCUI = R.RXCUI, 
        NOTES = '8.3-8.4',
        SAB = R.SAB, 
        TTY = R.TTY, 
        STR = R.STR, 
        CODE = R.CODE
    FROM DRUG_Mapper DM
    JOIN #CleanedDrugData CD ON DM.ID = CD.ID
    JOIN RXNCONSO R ON R.STR = CD.DrugName_NoParens OR R.STR = CD.ProdAI_NoParens
    WHERE DM.NOTES IS NULL
    AND R.SAB = 'RXNORM'
    AND R.TTY IN ('MIN', 'IN', 'PIN');
    
    -- Match with content inside parentheses (TTY IN only)
    UPDATE DM
    SET RXAUI = R.RXAUI, 
        RXCUI = R.RXCUI, 
        NOTES = '8.9-8.12',
        SAB = R.SAB, 
        TTY = R.TTY, 
        STR = R.STR, 
        CODE = R.CODE
    FROM DRUG_Mapper DM
    JOIN #CleanedDrugData CD ON DM.ID = CD.ID
    JOIN RXNCONSO R ON R.STR = CD.DrugName_InsideParens OR R.STR = CD.ProdAI_InsideParens
    WHERE DM.NOTES IS NULL
    AND R.TTY = 'IN';
    
    -- Match with content outside parentheses (TTY IN only)
    UPDATE DM
    SET RXAUI = R.RXAUI, 
        RXCUI = R.RXCUI, 
        NOTES = '8.11-8.12',
        SAB = R.SAB, 
        TTY = R.TTY, 
        STR = R.STR, 
        CODE = R.CODE
    FROM DRUG_Mapper DM
    JOIN #CleanedDrugData CD ON DM.ID = CD.ID
    JOIN RXNCONSO R ON R.STR = CD.DrugName_NoParens OR R.STR = CD.ProdAI_NoParens
    WHERE DM.NOTES IS NULL
    AND R.TTY = 'IN';
    
    -- Consolidated IDD mapping updates
    -- Match with content inside parentheses
    UPDATE DM
    SET RXAUI = R.RXAUI, 
        RXCUI = R.RXCUI, 
        NOTES = '8.5-8.8',
        SAB = R.SAB, 
        TTY = R.TTY, 
        STR = R.STR, 
        CODE = R.CODE
    FROM DRUG_Mapper DM
    JOIN #CleanedDrugData CD ON DM.ID = CD.ID
    JOIN IDD ON IDD.DRUGNAME = CD.DrugName_InsideParens OR IDD.DRUGNAME = CD.ProdAI_InsideParens
    JOIN RXNCONSO R ON R.RXAUI = IDD.RXAUI
    WHERE DM.NOTES IS NULL
    AND R.SAB = 'RXNORM'
    AND R.TTY IN ('MIN', 'IN', 'PIN');
    
    -- Match with content outside parentheses
    UPDATE DM
    SET RXAUI = R.RXAUI, 
        RXCUI = R.RXCUI, 
        NOTES = '8.7-8.8',
        SAB = R.SAB, 
        TTY = R.TTY, 
        STR = R.STR, 
        CODE = R.CODE
    FROM DRUG_Mapper DM
    JOIN #CleanedDrugData CD ON DM.ID = CD.ID
    JOIN IDD ON IDD.DRUGNAME = CD.DrugName_NoParens OR IDD.DRUGNAME = CD.ProdAI_NoParens
    JOIN RXNCONSO R ON R.RXAUI = IDD.RXAUI
    WHERE DM.NOTES IS NULL
    AND R.SAB = 'RXNORM'
    AND R.TTY IN ('MIN', 'IN', 'PIN');
    
    -- Clean up temp table
    DROP TABLE #CleanedDrugData;
COMMIT TRANSACTION;
GO

-- Final cleanup of parentheses (replaces the WHILE loops)
BEGIN TRANSACTION;
    -- Remove all parentheses from drug names
    UPDATE DRUG_Mapper
    SET CLEANED_DRUGNAME = dbo.RemoveParentheses(CLEANED_DRUGNAME)
    WHERE NOTES IS NULL;
    
    -- Remove all parentheses from product active ingredients
    UPDATE DRUG_Mapper
    SET CLEANED_PROD_AI = dbo.RemoveParentheses(CLEANED_PROD_AI)
    WHERE NOTES IS NULL;
    
    -- Final whitespace cleanup
    UPDATE DRUG_Mapper
    SET CLEANED_DRUGNAME = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_DRUGNAME, '  ', ' '), CHAR(10), ''), CHAR(13), '')))
    WHERE NOTES IS NULL;
    
    UPDATE DRUG_Mapper
    SET CLEANED_PROD_AI = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(CLEANED_PROD_AI, '  ', ' '), CHAR(10), ''), CHAR(13), '')))
    WHERE NOTES IS NULL;
COMMIT TRANSACTION;
GO
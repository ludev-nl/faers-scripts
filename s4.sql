
--STANDARDIZE DEMO_Combined AGE FILED TO YEARS

USE FAERS_A
GO

ALTER TABLE [DEMO_Combined]
ADD AGE_Years_fixed FLOAT;
GO
-------------
WITH CTE AS (SELECT  [DEMO_ID]

      ,[AGE]
      ,[AGE_COD]

	   , CASE WHEN [AGE_COD] ='DEC' THEN round(CAST ([AGE]AS float)*12 ,2)
			WHEN	  [AGE_COD]='YR' THEN ROUND([AGE] ,2)
			WHEN  	  [AGE_COD]='YEAR' THEN ROUND([AGE] ,2)
			WHEN  	  [AGE_COD]='MON' THEN ROUND(CAST ([AGE]AS float)/12 ,2)
			WHEN   	  [AGE_COD]='WK' THEN ROUND(CAST ([AGE]AS float)/52 ,2)
			WHEN  	  [AGE_COD]='WEEK' THEN ROUND(CAST ([AGE]AS float)/52 ,2)
			WHEN  	  [AGE_COD]='DY' THEN ROUND(CAST ([AGE]AS float)/365 ,2)
			WHEN  	  [AGE_COD]='DAY' THEN ROUND(CAST ([AGE]AS float)/365 ,2)
			WHEN   	  [AGE_COD]='HR' THEN ROUND(CAST ([AGE]AS float)/8760 ,2)
			WHEN   	  [AGE_COD]='HOUR' THEN ROUND(CAST ([AGE]AS float)/8760,2) ELSE AGE END AGE_Years_fixed

  FROM [FAERS_A].[dbo].[DEMO_Combined]
  WHERE ISNUMERIC([AGE]) = 1)
  UPDATE [DEMO_Combined]
  SET [DEMO_Combined].AGE_Years_fixed = cte.AGE_Years_fixed FROM [DEMO_Combined]   INNER JOIN
  CTE ON [DEMO_Combined].DEMO_ID = CTE.DEMO_ID;
GO

--STANDARDIZING DEMO_Combined COUNTRY CODE
ALTER TABLE DEMO_Combined
ADD  COUNTRY_CODE VARCHAR(2);
GO
-----------------------------------
UPDATE DEMO_Combined
  SET COUNTRY_CODE = CASE
--####  FILLMARKER
ELSE CASE WHEN LEN(reporter_country)=2 THEN reporter_country
ELSE NULL

END END
GO
--------------------------------------------------
ALTER TABLE DEMO_Combined
ADD  Gender VARCHAR(3);
GO
--------------------------------------------------
UPDATE DEMO_Combined
SET Gender = sex;
GO
---------------------------------------------------
UPDATE DEMO_Combined
SET Gender = NULL
WHERE Gender = 'UNK';
GO
---------------------------------------------------

UPDATE DEMO_Combined
SET Gender = NULL
WHERE Gender = 'NS';
GO
--------------------------------------------------
UPDATE DEMO_Combined
SET Gender = NULL
WHERE gender = 'YR';
GO
---------------------------------------------------

-------------------------------------
-- PREPARE FOR DEDUPLICATION STEP, COMBINE ALL TOGETHER : DEMO_COMBINED + DRUG_Combined + COMBINED_DRUGS + COMBINED_THER + COMBINED_INDI + COMBINED_REAC
-- GET ONLY THE LATEST CASE REPORT
DROP TABLE IF EXISTS Aligned_DEMO_DRUG_REAC_INDI_THER;


WITH CTE AS (SELECT  x.DEMO_ID, x.caseid, x.primaryid, x.caseversion, x.fda_dt, x.I_F_COD, x.event_dt, x.AGE_Years_fixed, x.GENDER, x.COUNTRY_CODE,x.OCCP_COD, x.PERIOD, Aligned_drugs , Aligned_INDI , Aligned_START_DATE , ALIGNED_REAC
				FROM    DEMO_Combined x  LEFT OUTER JOIN

				(SELECT   primaryid	, STRING_AGG(CAST(drugname AS NVARCHAR(MAX)), '/'  ) WITHIN GROUP(ORDER BY DRUG_SEQ ) AS Aligned_drugs
				FROM DRUG_Combined
				GROUP BY primaryid) a ON x.primaryid = a.primaryid LEFT OUTER JOIN

				(SELECT   primaryid	, STRING_AGG(CAST(MEDDRA_CODE AS NVARCHAR(MAX)), '/'  ) WITHIN GROUP(ORDER BY indi_drug_seq,MEDDRA_CODE ) AS Aligned_INDI
				FROM INDI_Combined WHERE MEDDRA_CODE NOT IN (10070592,10057097)
				GROUP BY primaryid) b ON a.primaryid=b.primaryid LEFT OUTER JOIN

				(SELECT   primaryid	, STRING_AGG(CAST(START_DT AS NVARCHAR(MAX)), '/'  ) WITHIN GROUP(ORDER BY dsg_drug_seq,START_DT ) AS Aligned_START_DATE
				FROM THER_Combined
				GROUP BY primaryid) c ON a.primaryid=c.primaryid LEFT OUTER JOIN

				(SELECT primaryid   , STRING_AGG(CAST(MEDDRA_CODE AS NVARCHAR(MAX)), '/' ) WITHIN GROUP(ORDER BY MEDDRA_CODE ) AS ALIGNED_REAC
				FROM [REAC_Combined]
				GROUP BY primaryid) d ON a.primaryid=d.primaryid
				) ;

SELECT DEMO_ID, caseid, primaryid, caseversion, fda_dt,  I_F_COD, event_dt, AGE_Years_fixed, GENDER, COUNTRY_CODE, OCCP_COD, Period, Aligned_drugs , Aligned_INDI , Aligned_START_DATE , ALIGNED_REAC
INTO ALIGNED_DEMO_DRUG_REAC_INDI_THER
FROM	(SELECT *, ROW_NUMBER() OVER(PARTITION BY caseid ORDER BY primaryid DESC, PERIOD DESC, caseversion DESC, fda_dt DESC,  I_F_COD DESC, event_dt DESC ) AS row_num
FROM CTE
		) a WHERE a.row_num = 1;

-- Full match(ALL CRITERIA MATCHED)
WITH CTE AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY event_dt, AGE_Years_fixed, GENDER, COUNTRY_CODE,  Aligned_drugs , Aligned_INDI , Aligned_START_DATE , ALIGNED_REAC ORDER BY primaryid DESC, Period DESC) AS row_num
	FROM Aligned_DEMO_DRUG_REAC_INDI_THER
	) DELETE FROM cte WHERE row_num > 1;

--(Full match - event_dt)
WITH CTE AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY  AGE_Years_fixed, GENDER, COUNTRY_CODE,  Aligned_drugs , Aligned_INDI , Aligned_START_DATE , ALIGNED_REAC ORDER BY primaryid DESC, Period DESC) AS row_num
	FROM Aligned_DEMO_DRUG_REAC_INDI_THER
		) DELETE FROM cte WHERE row_num > 1;

--(Full match - AGE_Years_fixed)
WITH CTE AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY event_dt, GENDER, COUNTRY_CODE,  Aligned_drugs , Aligned_INDI , Aligned_START_DATE , ALIGNED_REAC ORDER BY primaryid DESC, Period DESC) AS row_num
	FROM Aligned_DEMO_DRUG_REAC_INDI_THER
	) DELETE FROM cte WHERE row_num > 1;

--(Full match -	GENDER)
WITH CTE AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY event_dt, AGE_Years_fixed, COUNTRY_CODE,  Aligned_drugs , Aligned_INDI , Aligned_START_DATE , ALIGNED_REAC ORDER BY primaryid DESC, Period DESC) AS row_num
	FROM Aligned_DEMO_DRUG_REAC_INDI_THER
	) DELETE FROM cte WHERE row_num > 1;

--(Full match - COUNTRY_CODE)
WITH CTE AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY event_dt, AGE_Years_fixed, GENDER,  Aligned_drugs , Aligned_INDI , Aligned_START_DATE , ALIGNED_REAC ORDER BY primaryid DESC, Period DESC) AS row_num
	FROM Aligned_DEMO_DRUG_REAC_INDI_THER
	) DELETE FROM cte WHERE row_num > 1;

--(Full match -	Aligned_INDI)
WITH CTE AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY event_dt, AGE_Years_fixed, GENDER, COUNTRY_CODE,  Aligned_drugs, Aligned_START_DATE , ALIGNED_REAC ORDER BY primaryid DESC, Period DESC) AS row_num
	FROM Aligned_DEMO_DRUG_REAC_INDI_THER
	) DELETE FROM cte WHERE row_num > 1;

--(Full match -	Aligned_START_DATE)
WITH CTE AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY event_dt, AGE_Years_fixed, GENDER, COUNTRY_CODE,  Aligned_drugs , Aligned_INDI , ALIGNED_REAC ORDER BY primaryid DESC, Period DESC) AS row_num
	FROM Aligned_DEMO_DRUG_REAC_INDI_THER
	) DELETE FROM cte WHERE row_num > 1;
---------------------------------------------------------------------------------------------
--DELETE CASES PRESENT IN THE DELETED CASES FILES IN THE FAERS QUARTERLY DATA EXTRACT FILES
-- WE USED MICROSOFT ACCESS DATABASE TO COMBINE THE TABLES TO PRODUCCE COMBINED_DELETED_CASES_REPORTS, KEEP Field1 NAME AS THE FIELD NAME OF THE CASEID

DELETE FROM Aligned_DEMO_DRUG_REAC_INDI_THER
WHERE CASEID IN (SELECT Field1 FROM COMBINED_DELETED_CASES_REPORTS)
---------------------------------------------------------------------------------------------
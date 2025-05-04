# Overview of lines and their respective schema:
37 
`(ISR bigint, DRUG_SEQ bigint, ROLE_COD VARCHAR(2), DRUGNAME VARCHAR(500), VAL_VBM  int, ROUTE VARCHAR(70), DOSE_VBM VARCHAR(300), DECHAL VARCHAR(2), RECHAL VARCHAR(2), LOT_NUM VARCHAR(565), EXP_DT VARCHAR(200), NDA_NUM VARCHAR(200));` \
212 
`(primaryid bigint, caseid bigint, DRUG_SEQ bigint, ROLE_COD VARCHAR(2), DRUGNAME VARCHAR(500), VAL_VBM  int, ROUTE VARCHAR(70), DOSE_VBM VARCHAR(300), cum_dose_chr FLOAT (24),  cum_dose_unit VARCHAR(8), DECHAL VARCHAR(2), RECHAL VARCHAR(2), LOT_NUM VARCHAR(565), EXP_DT VARCHAR(200), NDA_NUM VARCHAR(200), dose_amt VARCHAR(15), dose_unit VARCHAR(20), dose_form VARCHAR(100) , dose_freq VARCHAR(20));` \
247 
`(primaryid bigint, caseid bigint, DRUG_SEQ bigint, ROLE_COD VARCHAR(2), DRUGNAME VARCHAR(500), prod_ai VARCHAR(800),  VAL_VBM  int, ROUTE VARCHAR(70), DOSE_VBM VARCHAR(300), cum_dose_chr FLOAT (24),  cum_dose_unit VARCHAR(8), DECHAL VARCHAR(2), RECHAL VARCHAR(2), LOT_NUM VARCHAR(565), EXP_DT VARCHAR(200), NDA_NUM VARCHAR(200), dose_amt VARCHAR(15), dose_unit VARCHAR(20), dose_form VARCHAR(100) , dose_freq VARCHAR(20));` \
397 
`(primaryid bigint, caseid bigint, DRUG_SEQ bigint, ROLE_COD VARCHAR(2), DRUGNAME VARCHAR(500), prod_ai VARCHAR(800),  VAL_VBM  int, ROUTE VARCHAR(70), DOSE_VBM VARCHAR(800), cum_dose_chr FLOAT (24),  cum_dose_unit VARCHAR(8), DECHAL VARCHAR(2), RECHAL VARCHAR(2), LOT_NUM VARCHAR(565), EXP_DT VARCHAR(200), NDA_NUM VARCHAR(200), dose_amt VARCHAR(15), dose_unit VARCHAR(20), dose_form VARCHAR(100) , dose_freq VARCHAR(20));` \
427 
`(ISR bigint, PT VARCHAR (100) );` \
587 
`(ISR bigint, PT VARCHAR (100));` \
602 
`(primaryid bigint, Caseid bigint,  PT VARCHAR (100) );` \
637 
`(primaryid bigint, Caseid bigint,  PT VARCHAR (100), drug_rec_act VARCHAR(100) );` \
818 
`(ISR BIGINT , [CASE] BIGINT, I_F_COD VARCHAR(1), FOLL_SEQ VARCHAR(50),IMAGE VARCHAR(10), EVENT_DT int , MFR_DT int, FDA_DT int, REPT_COD VARCHAR(10), MFR_NUM VARCHAR(100), MFR_SNDR VARCHAR (100), AGE VARCHAR(28), AGE_COD Varchar(3), GNDR_COD VArchar (3), E_SUB vARCHAR(1), WT VARCHAR(25), wt_COD varchar (20), REPT_DT int, OCCP_COD varchar (10), DEATH_DT varchar (1), TO_MFR varchar (1), CONFID varchar (10) );` \
848 
`(ISR BIGINT , [CASE] BIGINT, I_F_COD VARCHAR(1), FOLL_SEQ VARCHAR(50),IMAGE VARCHAR(10), EVENT_DT int , MFR_DT int, FDA_DT int, REPT_COD VARCHAR(10), MFR_NUM VARCHAR(100), MFR_SNDR VARCHAR (100), AGE VARCHAR(28), AGE_COD Varchar(3), GNDR_COD VArchar (3), E_SUB vARCHAR(1), WT VARCHAR(25), wt_COD varchar (20), REPT_DT int, OCCP_COD varchar (10), DEATH_DT varchar (1), TO_MFR varchar (1), CONFID varchar (10) , REPORTER_COUNTRY VARCHAR(100) );` \
993 
`(primaryid BIGINT , caseid BIGINT, caseversion int, I_F_COD VARCHAR(1),  event_dt int , mfr_dt int, init_fda_dt int, fda_dt int, rept_cod VARCHAR(10), mfr_num VARCHAR(100), mfr_sndr VARCHAR (100), AGE VARCHAR(28), AGE_COD Varchar(3), GNDR_COD VArchar (3), E_SUB vARCHAR(1), WT VARCHAR(25), wt_COD varchar (20), REPT_DT int, OCCP_COD varchar (10),  TO_MFR varchar (10), REPORTER_COUNTRY VARCHAR(100), occr_country VArchar(20));` \
1028 
`(primaryid BIGINT , caseid BIGINT, caseversion int, I_F_COD VARCHAR(1),  event_dt int , mfr_dt int, init_fda_dt int, fda_dt int, rept_cod VARCHAR(10), auth_num VARCHAR(100),mfr_num VARCHAR(100), mfr_sndr VARCHAR (100), lit_ref VARCHAR(600),AGE VARCHAR(28), AGE_COD VARCHAR(3), age_grp Varchar (1), sex Varchar(3), E_SUB VARCHAR(1), WT VARCHAR(25), wt_COD VARCHAR (20), REPT_DT int, OCCP_COD VARCHAR (10),  TO_MFR VARCHAR (10), REPORTER_COUNTRY VARCHAR(100), occr_country VARCHAR(20));`
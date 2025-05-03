/* CREATE DATABASE IF NOT EXISTS FAERS_A; */
/* \c FAERS_A */

-- Create the table if it does not exist
DROP TABLE IF EXISTS DRUG12q4;
CREATE TABLE IF NOT EXISTS DRUG12q4 (
    primaryid bigint,
    caseid bigint,
    DRUG_SEQ bigint,
    ROLE_COD VARCHAR(2),
    DRUGNAME TEXT,
    /* DRUGNAME VARCHAR(500), */
    VAL_VBM int,
    ROUTE VARCHAR(70),
    DOSE_VBM VARCHAR(300),
    cum_dose_chr FLOAT,
    cum_dose_unit VARCHAR(8),
    DECHAL VARCHAR(2),
    RECHAL VARCHAR(2),
    LOT_NUM VARCHAR(565),
    EXP_DT VARCHAR(200),
    NDA_NUM VARCHAR(200),
    dose_amt VARCHAR(15),
    dose_unit VARCHAR(20),
    dose_form VARCHAR(100),
    dose_freq VARCHAR(20)
);

-- Grant privileges to the sa user
GRANT ALL PRIVILEGES ON TABLE DRUG12q4 TO sa;

-- Copy data from the file
/* \copy DRUG12q4 ( primaryid, caseid, DRUG_SEQ, ROLE_COD, DRUGNAME, VAL_VBM, ROUTE, DOSE_VBM, cum_dose_chr, cum_dose_unit, DECHAL, RECHAL, LOT_NUM, EXP_DT, NDA_NUM, dose_amt, dose_unit, dose_form, dose_freq) FROM '/tmp/drug.txt' DELIMITER '$' CSV HEADER; */

-- Copy data from the file
COPY DRUG12q4 (
    primaryid,
    caseid,
    DRUG_SEQ,
    ROLE_COD,
    DRUGNAME,
    VAL_VBM,
    ROUTE,
    DOSE_VBM,
    cum_dose_chr,
    cum_dose_unit,
    DECHAL,
    RECHAL,
    LOT_NUM,
    EXP_DT,
    NDA_NUM,
    dose_amt,
    dose_unit,
    dose_form,
    dose_freq
)
/* FROM '/faers/data/drug.txt' */ 
FROM '/faers/data/faers_ascii_2012Q4/drug12q4.txt' 
DELIMITER '$'
CSV HEADER
QUOTE E'\b'; -- https://stackoverflow.com/a/44120988

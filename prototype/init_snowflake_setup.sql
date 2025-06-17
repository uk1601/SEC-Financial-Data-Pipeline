-- Switch to the ACCOUNTADMIN role for initial setup
USE ROLE ACCOUNTADMIN;

-- 1. Create a dedicated warehouse for dbt
CREATE WAREHOUSE dbt_wh
  WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;

-- 2. Create the dbt database and role if they don't already exist
CREATE DATABASE IF NOT EXISTS dbt_db;
CREATE ROLE IF NOT EXISTS dbt_role;

-- 3. Inspect current grants on the new warehouse
SHOW GRANTS ON WAREHOUSE dbt_wh;

-- 4. Grant the dbt_role to the target user and give it privileges
GRANT ROLE dbt_role TO USER yohanmarkose;
GRANT USAGE ON WAREHOUSE dbt_wh TO ROLE dbt_role;
GRANT ALL ON DATABASE dbt_db TO ROLE dbt_role;

-- 5. Switch to the dbt_role to create schemas and objects
USE ROLE dbt_role;

-- 6. Create a schema for dbt models
CREATE SCHEMA IF NOT EXISTS dbt_db.dbt_schema;

--------------------------------------------------------------------------------
-- 7. Define an external stage pointing to the S3 bucket
--------------------------------------------------------------------------------
CREATE OR REPLACE STAGE dbt_db.dbt_schema.my_s3_stage
  URL = 's3://secdatafiles/'
  CREDENTIALS = (
    AWS_KEY_ID     = '< your_aws_access_key_id >',
    AWS_SECRET_KEY = '< your_aws_secret_access_key >'
  );

--------------------------------------------------------------------------------
-- 8. Define a reusable CSV file format
--------------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT dbt_db.dbt_schema.my_csv_format
  TYPE                   = CSV
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF                = ('', 'NULL')
  SKIP_HEADER            = 1;

--------------------------------------------------------------------------------
-- 9. Create raw staging tables
--------------------------------------------------------------------------------

-- raw_sub: submission metadata
CREATE OR REPLACE TABLE dbt_db.dbt_schema.raw_sub (
  adsh       VARCHAR PRIMARY KEY,  -- Unique submission ID
  cik        VARCHAR,              -- Company identifier
  name       VARCHAR,              -- Company name
  sic        VARCHAR,              -- Industry code
  countryba  VARCHAR,              -- Business address country
  stprba     VARCHAR,              -- Business address state/province
  cityba     VARCHAR,              -- Business address city
  zipba      VARCHAR,              -- Business address ZIP
  bas1       VARCHAR,              -- Business address line 1
  bas2       VARCHAR,              -- Business address line 2
  baph       VARCHAR,              -- Business address phone
  countryma  VARCHAR,              -- Mailing address country
  stprma     VARCHAR,              -- Mailing address state/province
  cityma     VARCHAR,              -- Mailing address city
  zipma      VARCHAR,              -- Mailing address ZIP
  mas1       VARCHAR,              -- Mailing address line 1
  mas2       VARCHAR,              -- Mailing address line 2
  countryinc VARCHAR,              -- Country of incorporation
  stprinc    VARCHAR,              -- State/province of incorporation
  ein        VARCHAR,              -- Employer Identification Number
  former     VARCHAR,              -- Former company name
  changed    VARCHAR,              -- Date of name change
  afs        VARCHAR,              -- Accounting standards
  wksi       VARCHAR,              -- Seasoned issuer flag
  fye        VARCHAR,              -- Fiscal year end
  form       VARCHAR,              -- SEC form type
  period     VARCHAR,              -- Period end date
  fy         VARCHAR,              -- Fiscal year
  fp         VARCHAR,              -- Fiscal period
  filed      VARCHAR,              -- Filing date
  accepted   VARCHAR,              -- Time accepted
  prevrpt    VARCHAR,              -- Previous report flag
  detail     VARCHAR,              -- Detailed submission flag
  instance   VARCHAR,              -- Instance doc name
  nciks      VARCHAR,              -- Number of CIKs
  aciks      VARCHAR               -- Additional CIKs
);

-- raw_num: numeric facts
CREATE OR REPLACE TABLE dbt_db.dbt_schema.raw_num (
  adsh     VARCHAR PRIMARY KEY,
  tag      VARCHAR,
  version  VARCHAR,
  ddate    VARCHAR,
  qtrs     VARCHAR,
  uom      VARCHAR,
  segments VARCHAR,
  coreg    VARCHAR,
  value    VARCHAR,
  footnote VARCHAR
);

-- raw_tag: tag definitions
CREATE OR REPLACE TABLE dbt_db.dbt_schema.raw_tag (
  tag      VARCHAR,
  version  VARCHAR,
  custom   VARCHAR,
  abstract VARCHAR,
  datatype VARCHAR,
  iord     VARCHAR,
  crdr     VARCHAR,
  tlabel   VARCHAR,
  doc      VARCHAR
);

-- raw_pre: presentation metadata
CREATE OR REPLACE TABLE dbt_db.dbt_schema.raw_pre (
  adsh     VARCHAR PRIMARY KEY,
  report   VARCHAR,
  line     VARCHAR,
  stmt     VARCHAR,
  inpth    VARCHAR,
  rfile    VARCHAR,
  tag      VARCHAR,
  version  VARCHAR,
  plabel   VARCHAR,
  negating VARCHAR
);

--------------------------------------------------------------------------------
-- 10. Load data from S3 into staging tables
--------------------------------------------------------------------------------
COPY INTO dbt_db.dbt_schema.raw_sub
  FROM @dbt_db.dbt_schema.my_s3_stage/sec_data/data/2024/3/sub.csv
  FILE_FORMAT = (FORMAT_NAME = my_csv_format)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO dbt_db.dbt_schema.raw_num
  FROM @dbt_db.dbt_schema.my_s3_stage/sec_data/data/2024/3/num.csv
  FILE_FORMAT = (FORMAT_NAME = my_csv_format)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO dbt_db.dbt_schema.raw_pre
  FROM @dbt_db.dbt_schema.my_s3_stage/sec_data/data/2024/3/pre.csv
  FILE_FORMAT = (FORMAT_NAME = my_csv_format)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO dbt_db.dbt_schema.raw_tag
  FROM @dbt_db.dbt_schema.my_s3_stage/sec_data/data/2024/3/tag.csv
  FILE_FORMAT = (FORMAT_NAME = my_csv_format)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

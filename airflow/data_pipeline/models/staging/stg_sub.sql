-- -----------------------------------------------------------------------------
-- File: airflow/data_pipeline/models/staging/stg_sub.sql
-- -----------------------------------------------------------------------------
-- Transformation: raw_sub ➔ stg_sub
-- Casts, renames, and converts raw submission metadata columns for staging.
-- -----------------------------------------------------------------------------
SELECT
    adsh                                                   AS submission_id,   -- Unique SEC submission ID
    TRY_CAST(cik AS NUMBER)                                AS company_id,      -- Central Index Key
    name                                                   AS company_name,    -- Reporting company name
    TRY_CAST(sic AS NUMBER)                                AS sic_code,        -- Industry classification code
    countryba                                              AS business_country,-- Business address country
    stprba                                                 AS business_state,  -- Business address state/province
    cityba                                                 AS business_city,   -- Business address city
    zipba                                                  AS business_zip,    -- Business address ZIP code
    countryma                                              AS mailing_country, -- Mailing address country
    stprma                                                 AS mailing_state,   -- Mailing address state/province
    cityma                                                 AS mailing_city,    -- Mailing address city
    zipma                                                  AS mailing_zip,     -- Mailing address ZIP code
    TRY_CAST(ein AS NUMBER)                                AS employer_id,     -- Employer Identification Number
    TRY_TO_DATE(CAST(period AS STRING), 'YYYYMMDD')        AS period,          -- Reporting period end date
    TRY_TO_DATE(CAST(filed AS STRING), 'YYYYMMDD')         AS filing_date,     -- SEC filing date
    TRY_CAST(fy AS INTEGER)                                AS fiscal_year,     -- Fiscal year
    fp                                                     AS fiscal_period   -- Fiscal period code (Q1–Q4 or FY)
FROM {{ source('sec_source', 'raw_sub') }};

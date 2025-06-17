-- -----------------------------------------------------------------------------
-- File: airflow/data_pipeline/tests/is_duplicates_check.sql
--
-- Test: Ensure no duplicate Income Statement records
-- Validates that the income_statement mart has no repeated rows for the same
-- combination of all relevant columns.
-- -----------------------------------------------------------------------------

SELECT
    COMPANY_NAME,         -- Reporting company name
    COMPANY_ID,           -- Central Index Key (CIK)
    FILING_DATE,          -- SEC filing date
    PERIOD,               -- Reporting period end date
    FISCAL_YEAR,          -- Fiscal year
    FISCAL_PERIOD,        -- Fiscal period code (Q1–Q4 or FY)
    UNIT,                 -- Unit of measure
    PREFERRED_LABEL,      -- Human-readable label for the fact
    TOTAL_REPORTED_AMOUNT,-- Aggregated value reported
    TAG,                  -- Tag identifier
    DATATYPE,             -- Data type of the tag value
    DOCUMENTATION,        -- Documentation string for the tag
    COUNT(*) AS duplicate_count  -- Number of duplicate rows found
FROM {{ ref('income_statement') }}  -- Reference to the income_statement mart model
GROUP BY
    COMPANY_NAME,
    COMPANY_ID,
    FILING_DATE,
    PERIOD,
    FISCAL_YEAR,
    FISCAL_PERIOD,
    UNIT,
    PREFERRED_LABEL,
    TOTAL_REPORTED_AMOUNT,
    TAG,
    DATATYPE,
    DOCUMENTATION
HAVING
    COUNT(*) > 1;         -- Return only groups with more than one record

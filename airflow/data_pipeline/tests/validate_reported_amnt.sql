-- -----------------------------------------------------------------------------
-- File: airflow/data_pipeline/tests/validate_reported_amnt.sql
--
-- Test: Validate reported amounts for common financial tags
-- Ensures revenue and liabilities values meet expected sign criteria:
--   • Revenue (us-gaap:Revenue) must be non-negative
--   • Liabilities (us-gaap:Liabilities) must be non-negative
-- -----------------------------------------------------------------------------

SELECT
    submission_id,           -- Unique identifier linking to submission
    reported_amount          -- Numeric value reported for the tag
FROM {{ ref('stg_num') }}   -- Reference to the staging numeric facts model
WHERE
    (tag = 'us-gaap:Revenue' AND reported_amount < 0)
    OR
    (tag = 'us-gaap:Liabilities' AND reported_amount < 0)  -- Liabilities should not be negative
;

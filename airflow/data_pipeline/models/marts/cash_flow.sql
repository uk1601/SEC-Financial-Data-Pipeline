-- -----------------------------------------------------------------------------
-- File: airflow/data_pipeline/models/marts/cash_flow.sql
-- -----------------------------------------------------------------------------
-- Mart model: Aggregated Cash Flow
-- Joins staging tables to produce total reported amounts for Cash Flow items
-- -----------------------------------------------------------------------------

SELECT
    s.company_name,                       -- Reporting company name
    s.company_id,                         -- Central Index Key (CIK)
    s.filing_date,                        -- Date of SEC filing
    s.period,                             -- Reporting period end date
    s.fiscal_year,                        -- Fiscal year
    s.fiscal_period,                      -- Fiscal period code (Q1–Q4 or FY)
    n.unit,                               -- Unit of measure for values
    p.preferred_label,                    -- Human-readable label for each fact
    SUM(n.reported_amount) AS total_reported_amount,  -- Aggregated numeric value
    t.tag,                                -- Tag identifier
    t.datatype,                           -- Data type of the tag value
    t.documentation                       -- Documentation string for the tag
FROM {{ ref('stg_num') }} AS n          -- Numeric facts staging
JOIN {{ ref('stg_sub') }} AS s          -- Submission metadata staging
  ON n.submission_id = s.submission_id
JOIN {{ ref('stg_pre') }} AS p          -- Presentation metadata staging
  ON n.submission_id = p.submission_id
  AND n.tag     = p.tag
  AND n.version = p.version
JOIN {{ ref('stg_tag') }} AS t          -- Tag definitions staging
  ON n.tag     = t.tag
  AND n.version = t.version
WHERE
    p.statement_type = 'CF'             -- Filter only Cash Flow items
GROUP BY
    s.company_name,
    s.company_id,
    s.filing_date,
    s.period,
    s.fiscal_year,
    s.fiscal_period,
    n.unit,
    p.preferred_label,
    t.tag,
    t.datatype,
    t.documentation
ORDER BY
    s.company_name,
    s.period;                             -- Order by company and period for readability

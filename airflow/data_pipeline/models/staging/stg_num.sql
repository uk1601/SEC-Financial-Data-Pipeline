-- -----------------------------------------------------------------------------
-- File: airflow/data_pipeline/models/staging/stg_num.sql
-- -----------------------------------------------------------------------------
-- Transformation: raw_num ➔ stg_num
-- Casts, renames, and converts raw numeric fact columns for staging.
-- -----------------------------------------------------------------------------
SELECT
    adsh                                       AS submission_id,         -- Link to submission
    tag                                        AS tag,                   -- Tag identifier
    version                                    AS version,               -- Tag version
    TRY_TO_DATE(ddate, 'YYYYMMDD')             AS period_end_date,       -- End of reporting period
    TRY_CAST(qtrs AS NUMBER)                   AS num_quarters_covered,  -- Number of quarters covered
    uom                                        AS unit,                  -- Unit of measure
    segments                                   AS segments,              -- Segment metadata
    coreg                                      AS coreg,                 -- Co-registrant flag
    TRY_CAST(value AS NUMBER)                  AS reported_amount,       -- Numeric value reported
    footnote                                   AS footnote               -- Footnote text or code
FROM {{ source('sec_source', 'raw_num') }};

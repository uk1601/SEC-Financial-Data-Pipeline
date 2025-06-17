-- -----------------------------------------------------------------------------
-- File: airflow/data_pipeline/models/staging/stg_pre.sql
-- -----------------------------------------------------------------------------
-- Transformation: raw_pre ➔ stg_pre
-- Casts, renames, and converts raw presentation metadata columns for staging.
-- -----------------------------------------------------------------------------
SELECT
    adsh                          AS submission_id,       -- Link to submission
    TRY_CAST(report AS NUMBER)    AS report,              -- Report number reference
    TRY_CAST(line AS NUMBER)      AS line,                -- Line number reference in report
    stmt                          AS statement_type,      -- Statement code (BS, IS, CF, etc.)
    TRY_CAST(inpth AS BOOLEAN)    AS directly_reported,   -- Flag: value was directly reported
    rfile                         AS rfile,               -- File reference for layout
    tag                           AS tag,                 -- Tag identifier
    version                       AS version,             -- Tag version
    plabel                        AS preferred_label,     -- Preferred label for presentation
    TRY_CAST(negating AS BOOLEAN) AS negating             -- Flag: negating context for the line
FROM {{ source('sec_source', 'raw_pre') }};

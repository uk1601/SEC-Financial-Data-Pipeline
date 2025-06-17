-- -----------------------------------------------------------------------------
-- File: airflow/data_pipeline/models/staging/stg_tag.sql
-- -----------------------------------------------------------------------------
-- Transformation: raw_tag ➔ stg_tag
-- Casts, renames, and converts raw tag metadata columns for staging.
-- -----------------------------------------------------------------------------
SELECT
    tag                               AS tag,            -- Tag identifier
    version                           AS version,        -- Tag version
    TRY_CAST(custom AS BOOLEAN)       AS custom,         -- Flag: custom tag vs. standard
    TRY_CAST(abstract AS BOOLEAN)     AS abstract,       -- Flag: abstract tag
    datatype                          AS datatype,       -- Data type description
    iord                              AS item_order,     -- Display order index
    crdr                              AS balance_type,   -- Credit/debit indicator
    tlabel                            AS tag_label,      -- Human-readable label
    doc                               AS documentation   -- Documentation for the tag
FROM {{ source('sec_source', 'raw_tag') }};

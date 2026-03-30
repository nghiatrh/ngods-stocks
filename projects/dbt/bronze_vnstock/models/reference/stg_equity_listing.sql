{{
    config(
        materialized = 'table',
        file_format  = 'iceberg'
    )
}}

SELECT *, 'vnstock_vci' AS _source
FROM parquet.`s3a://warehouse/bronze/reference/equity_listing/data.parquet`

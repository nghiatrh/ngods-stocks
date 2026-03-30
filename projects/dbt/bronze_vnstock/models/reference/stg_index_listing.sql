{{
    config(
        materialized = 'table',
        file_format  = 'iceberg'
    )
}}

SELECT *, 'vnstock_vci' AS _source
FROM parquet.`s3a://warehouse/bronze/reference/index_listing/data.parquet`

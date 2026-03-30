{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'append',
        file_format          = 'iceberg',
        partitioned_by       = ['trade_date']
    )
}}

SELECT
    *,
    CAST(date AS DATE) AS trade_date,
    'vnstock_vci'      AS _source
FROM parquet.`s3a://warehouse/bronze/market/equity_ohlcv/`

{% if is_incremental() %}
WHERE CAST(date AS DATE) > (SELECT MAX(trade_date) FROM {{ this }})
{% endif %}

{{
    config(
        materialized = 'table',
        file_format  = 'iceberg'
    )
}}

/*
  Daily index fact table.
  Grain: one row per (trade_date, symbol).

  Enrichments vs bronze:
    - prior_close    : previous session closing value (LAG)
    - pct_change     : day-over-day return %
    - ytd_return_pct : return since first trading day of the calendar year
*/

WITH base AS (
    SELECT
        trade_date,
        symbol,
        CAST(open   AS DOUBLE) AS open,
        CAST(high   AS DOUBLE) AS high,
        CAST(low    AS DOUBLE) AS low,
        CAST(close  AS DOUBLE) AS close,
        CAST(volume AS BIGINT) AS volume,
        YEAR(trade_date)        AS year
    FROM warehouse.bronze.stg_index_ohlcv
),

enriched AS (
    SELECT
        *,
        LAG(close) OVER (
            PARTITION BY symbol ORDER BY trade_date
        )                                                                   AS prior_close,

        -- First close of the calendar year for each index
        FIRST_VALUE(close) OVER (
            PARTITION BY symbol, YEAR(trade_date)
            ORDER BY trade_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                                                   AS ytd_start_close
    FROM base
)

SELECT
    trade_date,
    symbol,
    open,
    high,
    low,
    close,
    volume,
    prior_close,
    CASE
        WHEN prior_close IS NOT NULL AND prior_close <> 0
        THEN ROUND((close - prior_close) / prior_close * 100, 2)
    END                                                                     AS pct_change,
    CASE
        WHEN ytd_start_close IS NOT NULL AND ytd_start_close <> 0
        THEN ROUND((close - ytd_start_close) / ytd_start_close * 100, 2)
    END                                                                     AS ytd_return_pct
FROM enriched

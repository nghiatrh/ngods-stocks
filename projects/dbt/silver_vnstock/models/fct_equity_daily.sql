{{
    config(
        materialized = 'table',
        file_format  = 'iceberg'
    )
}}

/*
  Daily equity fact table.
  Grain: one row per (trade_date, symbol).

  Enrichments vs bronze:
    - prior_close       : previous session closing price (LAG)
    - pct_change        : (close - prior_close) / prior_close * 100
    - intraday_range_pct: (high - low) / prior_close * 100  — volatility proxy
    - volume_ratio_20d  : today's volume vs 20-session rolling average
*/

WITH base AS (
    SELECT
        trade_date,
        symbol,
        CAST(open   AS DOUBLE) AS open,
        CAST(high   AS DOUBLE) AS high,
        CAST(low    AS DOUBLE) AS low,
        CAST(close  AS DOUBLE) AS close,
        CAST(volume AS BIGINT) AS volume
    FROM warehouse.bronze.stg_equity_ohlcv
),

enriched AS (
    SELECT
        *,
        LAG(close) OVER (
            PARTITION BY symbol ORDER BY trade_date
        )                                                               AS prior_close,

        AVG(volume) OVER (
            PARTITION BY symbol
            ORDER BY trade_date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        )                                                               AS avg_volume_20d
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
    END                                                                 AS pct_change,
    CASE
        WHEN prior_close IS NOT NULL AND prior_close <> 0
        THEN ROUND((high - low) / prior_close * 100, 2)
    END                                                                 AS intraday_range_pct,
    CASE
        WHEN avg_volume_20d IS NOT NULL AND avg_volume_20d <> 0
        THEN ROUND(volume / avg_volume_20d, 2)
    END                                                                 AS volume_ratio_20d
FROM enriched

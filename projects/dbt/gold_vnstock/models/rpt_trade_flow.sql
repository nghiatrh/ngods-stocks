{{
    config(
        materialized = 'table',
        file_format  = 'iceberg'
    )
}}

/*
  Daily intraday trade flow aggregated per symbol.
  Grain: one row per (trade_date, symbol).

  Metrics:
    - vwap         : volume-weighted average price
    - buy/sell vol : matched volume split by trade direction
    - buy_ratio    : buy volume / total volume  (>0.5 = net buying pressure)
    - avg_trade_sz : mean shares per matched trade
*/

SELECT
    trade_date,
    symbol,
    COUNT(*)                                                            AS trade_count,
    SUM(volume)                                                        AS total_volume,
    ROUND(
        SUM(CAST(price AS DOUBLE) * volume) / NULLIF(SUM(volume), 0)
    , 0)                                                               AS vwap,
    ROUND(AVG(volume), 0)                                              AS avg_trade_size,
    SUM(CASE WHEN UPPER(match_type) = 'B' THEN volume ELSE 0 END)      AS buy_volume,
    SUM(CASE WHEN UPPER(match_type) = 'S' THEN volume ELSE 0 END)      AS sell_volume,
    ROUND(
        SUM(CASE WHEN UPPER(match_type) = 'B' THEN volume ELSE 0 END)
        * 1.0 / NULLIF(SUM(volume), 0)
    , 4)                                                               AS buy_ratio
FROM warehouse.bronze.stg_equity_trades
GROUP BY trade_date, symbol
ORDER BY trade_date DESC, total_volume DESC

{{
    config(
        materialized = 'table',
        file_format  = 'iceberg'
    )
}}

/*
  Daily market breadth summary across all VN30 equities.
  Grain: one row per trade_date.
*/

SELECT
    trade_date,
    COUNT(*)                                                            AS total_symbols,
    COUNT(CASE WHEN pct_change > 0  THEN 1 END)                        AS advancers,
    COUNT(CASE WHEN pct_change < 0  THEN 1 END)                        AS decliners,
    COUNT(CASE WHEN pct_change = 0  THEN 1 END)                        AS unchanged,
    COUNT(CASE WHEN pct_change IS NULL THEN 1 END)                     AS no_prior_data,
    ROUND(SUM(close * volume) / 1e9, 2)                                AS total_value_bn_vnd,
    SUM(volume)                                                        AS total_volume,
    ROUND(AVG(pct_change), 2)                                          AS avg_pct_change,
    MAX(pct_change)                                                    AS top_gain_pct,
    MIN(pct_change)                                                    AS top_loss_pct
FROM warehouse.silver.fct_equity_daily
GROUP BY trade_date
ORDER BY trade_date DESC

{{
    config(
        materialized = 'table',
        file_format  = 'iceberg'
    )
}}

/*
  Daily sector-level performance aggregated from VN30 equities.
  Grain: one row per (trade_date, sector).
*/

SELECT
    e.trade_date,
    d.icb_code3,
    d.industry_group                                                    AS sector_name,
    COUNT(DISTINCT e.symbol)                                            AS symbol_count,
    COUNT(CASE WHEN e.pct_change > 0 THEN 1 END)                       AS advancers,
    COUNT(CASE WHEN e.pct_change < 0 THEN 1 END)                       AS decliners,
    ROUND(AVG(e.pct_change), 2)                                        AS avg_pct_change,
    MAX(e.pct_change)                                                  AS top_gain_pct,
    MIN(e.pct_change)                                                  AS top_loss_pct,
    ROUND(SUM(e.close * e.volume) / 1e9, 2)                            AS total_value_bn_vnd,
    SUM(e.volume)                                                      AS total_volume
FROM warehouse.silver.fct_equity_daily  e
JOIN warehouse.silver.dim_equity        d ON e.symbol = d.symbol
WHERE e.pct_change IS NOT NULL
GROUP BY e.trade_date, d.icb_code3, d.industry_group
ORDER BY e.trade_date DESC, avg_pct_change DESC

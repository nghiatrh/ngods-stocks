{{
    config(
        materialized = 'table',
        file_format  = 'iceberg'
    )
}}

/*
  Daily per-stock performance enriched with company and sector info.
  Grain: one row per (trade_date, symbol).
  Primary table for stock-level drill-down in reports.
*/

SELECT
    e.trade_date,
    e.symbol,
    d.company_name,
    d.sector,
    d.industry_group,
    d.market_sector,
    e.open,
    e.high,
    e.low,
    e.close,
    e.volume,
    e.prior_close,
    e.pct_change,
    e.intraday_range_pct,
    e.volume_ratio_20d
FROM warehouse.silver.fct_equity_daily  e
LEFT JOIN warehouse.silver.dim_equity   d ON e.symbol = d.symbol
ORDER BY e.trade_date DESC, e.pct_change DESC

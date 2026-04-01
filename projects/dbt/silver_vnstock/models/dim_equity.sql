{{
    config(
        materialized = 'table',
        file_format  = 'iceberg'
    )
}}

/*
  Equity dimension — listing enriched with ICB industry classification.
  Grain: one row per listed symbol.
  Rebuilt in full on every run (reference data changes weekly).
*/

SELECT
    l.symbol,
    l.organ_name                        AS company_name,
    i.icb_code1,
    i.icb_code2,
    i.icb_code3,
    i.icb_code4,
    i.icb_name2                         AS market_sector,
    i.icb_name3                         AS industry_group,
    i.icb_name4                         AS sector
FROM warehouse.bronze.stg_equity_listing l
LEFT JOIN warehouse.bronze.stg_industry i
    ON  l.symbol  = i.symbol
    AND i.`table` = 'symbol_industry'

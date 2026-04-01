{{
    config(
        materialized = 'table',
        file_format  = 'iceberg'
    )
}}

/*
  Daily foreign investor buy/sell flow per symbol.
  Grain: one row per (trade_date, symbol).
  Source: order book snapshot which carries cumulative session totals.
*/

SELECT
    ob.trade_date,
    ob.listing_symbol                                                   AS symbol,
    d.company_name,
    d.sector,
    SUM(ob.match_foreign_buy_volume)                                   AS foreign_buy_volume,
    SUM(ob.match_foreign_sell_volume)                                  AS foreign_sell_volume,
    SUM(ob.match_foreign_buy_volume - ob.match_foreign_sell_volume)    AS net_foreign_volume,
    ROUND(SUM(ob.match_foreign_buy_value)    / 1e9, 4)                 AS foreign_buy_value_bn,
    ROUND(SUM(ob.match_foreign_sell_value)   / 1e9, 4)                 AS foreign_sell_value_bn,
    ROUND(SUM(ob.match_foreign_buy_value
              - ob.match_foreign_sell_value) / 1e9, 4)                 AS net_foreign_value_bn,
    MAX(ob.match_current_room)                                         AS remaining_foreign_room
FROM warehouse.bronze.stg_equity_order_book ob
LEFT JOIN warehouse.silver.dim_equity       d  ON ob.listing_symbol = d.symbol
GROUP BY ob.trade_date, ob.listing_symbol, d.company_name, d.sector
ORDER BY ob.trade_date DESC, net_foreign_value_bn DESC

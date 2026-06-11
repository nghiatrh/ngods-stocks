"""Unit tests for dbt-enrichment column resolution in app.ingest.

Run inside the chat image (deps + env available):
    docker compose run --rm --no-deps chat-api pytest tests/test_ingest.py
"""
import os

# app.config.Settings requires ANTHROPIC_API_KEY at import time; any value works.
os.environ.setdefault("ANTHROPIC_API_KEY", "test-key")

import pytest

from app.ingest import _underlying_column


@pytest.mark.parametrize(
    "leaf, expected",
    [
        # bare identifier -> that column (aggregated measure over a raw column)
        ({"name": "EquityPerformance.avg_close", "sql": "close"}, "close"),
        ({"name": "EquityPerformance.total_volume", "sql": "volume"}, "volume"),
        # single-column expression -> the one referenced column
        ({"name": "TradeFlow.avg_buy_ratio",
          "sql": "CAST({CUBE}.buy_ratio AS DOUBLE)"}, "buy_ratio"),
        ({"name": "EquityPerformance.trade_date",
          "sql": "CAST({CUBE}.trade_date AS TIMESTAMP)"}, "trade_date"),
        # multi-column expression -> fall back to the field name (no 1:1 column)
        ({"name": "MarketSummary.advance_decline_ratio",
          "sql": "CAST({CUBE}.advancers AS DOUBLE) / NULLIF({CUBE}.decliners, 0)"},
         "advance_decline_ratio"),
        ({"name": "EquityPerformance.symbol_date",
          "sql": "CONCAT({CUBE}.symbol, '_', CAST({CUBE}.trade_date AS VARCHAR))"},
         "symbol_date"),
        # no sql (e.g. a count measure) -> field name
        ({"name": "EquityPerformance.count"}, "count"),
        ({"name": "EquityPerformance.count", "sql": ""}, "count"),
        # same column referenced twice still resolves to that one column
        ({"name": "Foo.bar", "sql": "{CUBE}.x + {CUBE}.x"}, "x"),
    ],
)
def test_underlying_column(leaf, expected):
    assert _underlying_column(leaf) == expected

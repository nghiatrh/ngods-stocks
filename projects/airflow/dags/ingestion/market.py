"""
Market data fetching functions using the vnstock free API.

Covers:
  - VN30 symbol list
  - Equity OHLCV (daily candles)
  - Equity intraday trades
  - Equity order book snapshot (price board)
  - Index OHLCV (daily candles)
"""

import logging
from datetime import datetime
from typing import List

import pandas as pd

from ingestion.auth import setup_vnstock_auth
from ingestion.rate_limiter import throttle

log = logging.getLogger(__name__)

SOURCE = "VCI"
INDEX_SYMBOLS = ["VNINDEX", "VN30", "HNX30", "UPCOM"]


def get_vn30_symbols() -> List[str]:
    setup_vnstock_auth()
    throttle()
    from vnstock import Listing
    listing = Listing(source=SOURCE)
    symbols = listing.symbols_by_group(group_name="VN30")
    result = list(symbols) if hasattr(symbols, "__iter__") else []
    log.info("Fetched %d VN30 symbols", len(result))
    return result


def fetch_equity_ohlcv(symbols: List[str], date: str) -> pd.DataFrame:
    """
    Fetch daily OHLCV for each symbol on a given date.
    Returns a combined DataFrame with a 'symbol' column.
    """
    setup_vnstock_auth()
    from vnstock import Quote
    frames = []
    for symbol in symbols:
        throttle()
        try:
            quote = Quote(symbol=symbol, source=SOURCE)
            df = quote.history(start=date, end=date, interval="1D")
            if df is not None and not df.empty:
                df["symbol"] = symbol
                frames.append(df)
                log.info("OHLCV %s on %s: %d rows", symbol, date, len(df))
            else:
                log.warning("No OHLCV data for %s on %s", symbol, date)
        except Exception as e:
            log.error("Failed OHLCV for %s: %s", symbol, e)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result["date"] = date
    result["ingested_at"] = datetime.utcnow().isoformat()
    return result


def fetch_equity_trades(symbols: List[str], date: str) -> pd.DataFrame:
    """
    Fetch intraday trades for each symbol.
    Note: vnstock intraday() returns current-session data. This is best-effort;
    the task will succeed even if the market is closed.
    """
    setup_vnstock_auth()
    from vnstock import Quote
    frames = []
    for symbol in symbols:
        throttle()
        try:
            quote = Quote(symbol=symbol, source=SOURCE)
            df = quote.intraday(page_size=150000)
            if df is not None and not df.empty:
                df["symbol"] = symbol
                frames.append(df)
                log.info("Trades %s: %d rows", symbol, len(df))
            else:
                log.warning("No trades data for %s", symbol)
        except Exception as e:
            log.warning("Trades unavailable for %s (market may be closed): %s", symbol, e)

    if not frames:
        log.warning("No trades data retrieved for any symbol on %s", date)
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result["date"] = date
    result["ingested_at"] = datetime.utcnow().isoformat()
    return result


def fetch_equity_order_book(symbols: List[str], date: str) -> pd.DataFrame:
    """
    Fetch order book snapshot for all VN30 symbols at once using price_board.
    Returns a flat DataFrame with all bid/ask levels.
    """
    setup_vnstock_auth()
    throttle()
    from vnstock import Trading
    try:
        trading = Trading(source=SOURCE, symbol=symbols[0])
        df = trading.price_board(symbols_list=symbols)
        if df is None or df.empty:
            log.warning("Empty order book response")
            return pd.DataFrame()

        # Flatten MultiIndex columns if present (VCI source returns MultiIndex)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ["_".join(str(c) for c in col).strip("_") for col in df.columns]

        df["date"] = date
        df["ingested_at"] = datetime.utcnow().isoformat()
        log.info("Order book snapshot: %d symbols", len(df))
        return df
    except Exception as e:
        log.error("Failed to fetch order book: %s", e)
        return pd.DataFrame()


def fetch_index_ohlcv(date: str) -> pd.DataFrame:
    """
    Fetch daily OHLCV for major Vietnamese indices.
    """
    setup_vnstock_auth()
    from vnstock import Quote
    frames = []
    for symbol in INDEX_SYMBOLS:
        throttle()
        try:
            quote = Quote(symbol=symbol, source=SOURCE)
            df = quote.history(start=date, end=date, interval="1D")
            if df is not None and not df.empty:
                df["symbol"] = symbol
                frames.append(df)
                log.info("Index OHLCV %s on %s: %d rows", symbol, date, len(df))
            else:
                log.warning("No index OHLCV for %s on %s", symbol, date)
        except Exception as e:
            log.error("Failed index OHLCV for %s: %s", symbol, e)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result["date"] = date
    result["ingested_at"] = datetime.utcnow().isoformat()
    return result

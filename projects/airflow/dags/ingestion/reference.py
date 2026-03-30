"""
Reference data fetching functions using the vnstock free API.

Covers:
  - Equity listing (all symbols with exchange info)
  - Index listing (all indices)
  - Industry classification (ICB taxonomy + symbol mapping)
  - Corporate events (for VN30 symbols)
"""

import logging
from datetime import datetime
from typing import List

import pandas as pd

from ingestion.auth import setup_vnstock_auth
from ingestion.rate_limiter import throttle

log = logging.getLogger(__name__)

SOURCE = "VCI"


def fetch_equity_listing() -> pd.DataFrame:
    setup_vnstock_auth()
    throttle()
    from vnstock import Listing
    listing = Listing(source=SOURCE)
    df = listing.all_symbols()
    if df is None or df.empty:
        return pd.DataFrame()
    df["ingested_at"] = datetime.utcnow().isoformat()
    log.info("Equity listing: %d records", len(df))
    return df


def fetch_index_listing() -> pd.DataFrame:
    setup_vnstock_auth()
    throttle()
    from vnstock import Listing
    listing = Listing(source=SOURCE)
    df = listing.all_indices()
    if df is None or df.empty:
        return pd.DataFrame()
    df["ingested_at"] = datetime.utcnow().isoformat()
    log.info("Index listing: %d records", len(df))
    return df


def fetch_industry() -> pd.DataFrame:
    """
    Fetch two reference tables and combine:
    1. Symbol → industry mapping
    2. Full ICB taxonomy (4-level hierarchy)
    """
    setup_vnstock_auth()
    from vnstock import Listing
    listing = Listing(source=SOURCE)
    frames = []

    # Symbol-level industry mapping
    throttle()
    try:
        sym_industry = listing.symbols_by_industries()
        if sym_industry is not None and not sym_industry.empty:
            sym_industry["table"] = "symbol_industry"
            frames.append(sym_industry)
            log.info("Symbol industry mapping: %d records", len(sym_industry))
    except Exception as e:
        log.error("Failed symbol industry mapping: %s", e)

    # Full ICB taxonomy
    throttle()
    try:
        icb = listing.industries_icb()
        if icb is not None and not icb.empty:
            icb["table"] = "icb_taxonomy"
            frames.append(icb)
            log.info("ICB taxonomy: %d records", len(icb))
    except Exception as e:
        log.error("Failed ICB taxonomy: %s", e)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result["ingested_at"] = datetime.utcnow().isoformat()
    return result


def fetch_events(symbols: List[str]) -> pd.DataFrame:
    """
    Fetch corporate events for each symbol in the provided list.
    Uses VCI source which provides full event history.
    """
    setup_vnstock_auth()
    from vnstock import Company
    frames = []
    for symbol in symbols:
        throttle()
        try:
            company = Company(symbol=symbol, source="VCI")
            df = company.events()
            if df is not None and not df.empty:
                df["symbol"] = symbol
                frames.append(df)
                log.info("Events %s: %d records", symbol, len(df))
            else:
                log.warning("No events for %s", symbol)
        except Exception as e:
            log.error("Failed events for %s: %s", symbol, e)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result["ingested_at"] = datetime.utcnow().isoformat()
    return result

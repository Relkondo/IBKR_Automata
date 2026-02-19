"""Centralized configuration for IBKR Automata."""

import os

# --- TWS connection ---
TWS_HOST = "127.0.0.1"
TWS_PORT = 7497
TWS_CLIENT_ID = 1

# --- Paths ---
# Resolve relative to the project root (parent of src/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ASSETS_DIR = os.path.join(PROJECT_ROOT, "assets")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")

# --- Trading thresholds ---
MINIMUM_TRADING_AMOUNT = 100      # USD – net orders below this value are skipped
MAXIMUM_AMOUNT_AUTOMATIC_ORDER = 10_000  # USD – auto-confirmed orders above this require explicit approval

# --- Limit-price tuning ---
# Controls how aggressively limit orders cross the bid/ask spread (0-100).
#   0   = cross the spread fully (fills immediately)
#   50  = midpoint (balanced)
#   100 = sit on the passive side (cheapest, may not fill)
FILL_PATIENCE = 120

# --- Stale-order price tolerance ---
# When reconciling, an existing order is considered "stale" (and eligible
# for cancellation) if its price deviates from the new limit price by more
# than this fraction.  A wider tolerance is used for illiquid exchanges
# where spreads are naturally larger.
STALE_ORDER_TOL_PCT = 0.005           # 0.5 %
STALE_ORDER_TOL_PCT_ILLIQUID = 0.05   # 5 %

# --- Ticker redirections ---
# Merge a source ticker's Basket Allocation into a target ticker and
# drop the source row.  Useful when the source instrument is too
# expensive to trade (e.g. lot-size rules on index options) and a
# similar, cheaper instrument can absorb its allocation.
#
# Keys / values are the ticker PREFIX (the part before the first
# space in the input file).  For options the Ticker column is used;
# for stocks the Security Ticker column (falling back to Ticker).
#
# Example:  {"SPXW": "QQQ"}  redirects all SPXW option rows'
#           allocation to any QQQ option rows already in the file.
OPTION_TICKER_REDIRECTS: dict[str, str] = {
        "SPXW": "QQQ"
}
STOCK_TICKER_REDIRECTS: dict[str, str] = {
}

# --- Project Portfolio CSV column order ---
# Columns listed here appear first (in this order) when saving.
# Any extra columns present in the DataFrame are appended at the end.
PROJECT_PORTFOLIO_COLUMNS = [
    "Ticker",
    "Security Ticker",
    "Name",
    "IBKR Name",
    "IBKR Ticker",
    "Name Mismatch",
    "is_option",
    "clean_ticker",
    "MIC Primary Exchange",
    "conid",
    "currency",
    "fx_rate",
    "Basket Allocation",
    "Dollar Allocation",
    "bid",
    "ask",
    "last",
    "close",
    "day_high",
    "day_low",
    "market_rule_ids",
    "limit_price",
    "Qty",
    "Actual Dollar Allocation",
]

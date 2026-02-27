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
MAXIMUM_AMOUNT_AUTOMATIC_ORDER = 1500  # USD – auto-confirmed orders above this require explicit approval

# --- Relative-order tuning ---
# PRICE_OFFSET is the percentage offset passed directly to IBKR's
# ``percentOffset`` field on a Relative (REL) order.  IBKR pegs the
# order to the NBB (buy) or NBO (sell) and adds/subtracts this
# percentage, handling tick-size rounding automatically.
#   0   = no offset (passive, pegs exactly to bid/ask)
#   50  = moderate offset (half the spread-equivalent)
#   100 = very aggressive offset
PRICE_OFFSET = 10

# LIMIT_PRICE_OFFSET is a percentage of the reference price used to
# compute the cap (buy) or floor (sell) limit price on the Relative
# order.  This is NOT a spread percentage — it is applied to the full
# bid/ask/last/close price.
#   BUY  limit = bid  * (1 + LIMIT_PRICE_OFFSET / 100)
#   SELL limit = ask  * (1 - LIMIT_PRICE_OFFSET / 100)
#   Fallback chain: bid/ask → last → close
LIMIT_PRICE_OFFSET = 2

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

# --- Ticker ignore list ---
# Tickers listed here (upper-case) are completely invisible to the
# system: they are filtered out of the input spreadsheet (won't be
# bought) AND excluded from IBKR positions (won't be sold even if
# absent from the input).
IGNORE_TICKERS: list[str] = ["ENPLAS CORP", "ASUSTEK COMPUTER INC"]

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

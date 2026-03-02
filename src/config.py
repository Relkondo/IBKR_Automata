"""Centralized configuration for IBKR Automata."""

import os

from dotenv import load_dotenv

load_dotenv()

# --- IBKR connection ---
# Defaults target IB Gateway live.  Override via .env or shell env vars.
# Ports: 4001 = Gateway live, 4002 = Gateway paper,
#        7496 = TWS live,     7497 = TWS paper
TWS_HOST = os.getenv("IBKR_HOST", "127.0.0.1")
TWS_PORT = int(os.getenv("IBKR_PORT", "4001"))
TWS_CLIENT_ID = int(os.getenv("IBKR_CLIENT_ID", "1"))

# --- IB Gateway / IBC settings (used by src/gateway.py) ---
IBC_PATH = os.path.expanduser(os.getenv("IBC_PATH", "/opt/ibc"))
IBC_INI = os.path.expanduser(os.getenv("IBC_INI", "~/ibc/config.ini"))
GATEWAY_TWS_PATH = os.path.expanduser(os.getenv("TWS_PATH", "~/Applications"))
TWS_MAJOR_VRSN = os.getenv("TWS_MAJOR_VRSN", "10.37")
TRADING_MODE = os.getenv("TRADING_MODE", "paper")
GATEWAY_STARTUP_TIMEOUT = int(os.getenv("GATEWAY_STARTUP_TIMEOUT", "120"))

# --- Paths ---
# Resolve relative to the project root (parent of src/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIR = os.path.join(PROJECT_ROOT, "assets")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")

# --- Trading thresholds ---
MINIMUM_TRADING_AMOUNT = 100      # USD – net orders below this value are skipped
MAXIMUM_AMOUNT_AUTOMATIC_ORDER = 1500  # USD – auto-confirmed orders above this require explicit approval

# Subtracted from net liquidation before computing dollar allocations.
# Set to 0 to invest the full portfolio; set to e.g. 5000 to keep $5k in cash.
MINIMUM_CASH_RESERVE = 0

# When rebalancing an existing position, a high stock price can make
# the actual order (rounded to whole shares) much larger than the
# projected change from the portfolio model.  If the ratio
# |actual_vs_current| / |project_vs_current| exceeds this limit,
# the SELL order is skipped to avoid excessive trading and maintain
# exposure.  Only applies to SELL orders with existing positions.
SELL_REBALANCE_RATIO_LIMIT = 1.5

# --- Relative-order tuning ---
# PRICE_OFFSET is the percentage offset for Relative (REL) orders.
# IBKR pegs the order to the NBB (buy) or NBO (sell) and
# adds/subtracts this percentage, handling tick-size rounding
# automatically.  Stored as a human-readable percentage; divided
# by 100 before passing to IBKR's ``percentOffset`` field (which
# expects a fraction: 0.10 = 10%).
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

# --- Name ignore list ---
# Securities whose name (case-insensitive) matches any entry here are
# completely invisible to the system: they are filtered out of the
# input spreadsheet (won't be bought) AND excluded from extra IBKR
# positions (won't be sold even if absent from the input).
IGNORE_NAMES: list[str] = [
    "ENPLAS CORP",
    "ASUSTEK COMPUTER INC",
    "TOHO TITANIUM CO LTD",
    "SUMITOMO BAKELITE CO LTD",
    "APERAM - DIVIDEND RIGHTS",
    "TIC SOLUTIONS INC (OPTION)"
]

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
    "price_source",
    "Qty",
    "Actual Dollar Allocation",
]

# --- Telegram notifications ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

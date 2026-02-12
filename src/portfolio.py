"""Portfolio loading and filtering from Excel holdings files.

Reads the most recent .xlsx from the assets directory, selects the
required columns, filters out summary / empty rows, and determines
whether each row is a stock or an option.
"""

import os
import re

import pandas as pd

from src.config import ASSETS_DIR

# Columns we need from the Excel file (header row names).
_REQUIRED_COLUMNS = [
    "Ticker",
    "Security Ticker",
    "Name",
    "Dollar Allocation",
    "MIC Primary Exchange",
]


def _latest_xlsx(directory: str) -> str:
    """Return the path to the most recently modified .xlsx in *directory*."""
    xlsx_files = [
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if f.endswith(".xlsx") and not f.startswith("~$")
    ]
    if not xlsx_files:
        raise FileNotFoundError(f"No .xlsx files found in {directory}")
    return max(xlsx_files, key=os.path.getmtime)


# Regex that matches option-style tickers, e.g. "QQQ US 02/27/26 P600 Equity"
# Pattern: UNDERLYING <country> <MM/DD/YY> <C|P><strike> <suffix>
_OPT_TICKER_RE = re.compile(
    r"^(?P<underlying>[A-Z]+)"           # underlying symbol
    r"\s+[A-Z]{2}"                       # country code (ignored)
    r"\s+(?P<month>\d{2})/(?P<day>\d{2})/(?P<year>\d{2})"  # MM/DD/YY
    r"\s+(?P<right>[CP])"                # C = Call, P = Put
    r"(?P<strike>[\d.]+)"               # strike price
    r"(?:\s+\S+)?$"                      # optional trailing suffix (e.g. Equity)
)

# Month number -> IBKR short month string (used later for secdef queries).
# Accepts both zero-padded ("02") and unpadded ("2") keys.
MONTH_MAP = {
    "1": "JAN", "01": "JAN", "2": "FEB", "02": "FEB",
    "3": "MAR", "03": "MAR", "4": "APR", "04": "APR",
    "5": "MAY", "05": "MAY", "6": "JUN", "06": "JUN",
    "7": "JUL", "07": "JUL", "8": "AUG", "08": "AUG",
    "9": "SEP", "09": "SEP", "10": "OCT", "11": "NOV", "12": "DEC",
}


def _is_option(row: pd.Series) -> bool:
    """Heuristic: the row represents an option contract."""
    name = str(row.get("Name", ""))
    ticker = str(row.get("Ticker", "") or "")
    if "Calls on" in name or "Puts on" in name:
        return True
    if _OPT_TICKER_RE.match(ticker.strip()):
        return True
    return False


def _effective_ticker(row: pd.Series) -> str:
    """Return the security ticker if present, else the ticker."""
    sec = row.get("Security Ticker")
    if pd.notna(sec) and str(sec).strip():
        return str(sec).strip()
    return str(row.get("Ticker", "")).strip()


def _clean_ticker(ticker: str) -> str:
    """Strip the exchange suffix (e.g. 'NVDA US' -> 'NVDA')."""
    parts = ticker.rsplit(" ", 1)
    if len(parts) == 2 and parts[1].isalpha() and len(parts[1]) <= 4:
        return parts[0]
    return ticker


def load_portfolio(xlsx_path: str | None = None) -> pd.DataFrame:
    """Load, filter, and annotate the portfolio table.

    Parameters
    ----------
    xlsx_path : str | None
        Explicit path to the Excel file.  If *None*, the most recent
        .xlsx in the configured assets directory is used.

    Returns
    -------
    pd.DataFrame
        Filtered portfolio table with additional helper columns:
        ``is_option``, ``effective_ticker``, ``clean_ticker``.
    """
    path = xlsx_path or _latest_xlsx(ASSETS_DIR)
    print(f"Reading portfolio from {path} ...")

    df = pd.read_excel(path, engine="openpyxl")

    # Keep only the columns we care about (ignore extras gracefully).
    available = [c for c in _REQUIRED_COLUMNS if c in df.columns]
    df = df[available].copy()

    # Filter: drop rows with empty Name (summary / header rows).
    df = df[df["Name"].notna() & (df["Name"].astype(str).str.strip() != "")]
    # Also filter the literal dash used as a placeholder for Cash.
    df = df[df["Name"].astype(str).str.strip() != "-"]

    # Ensure Dollar Allocation is numeric.
    df["Dollar Allocation"] = pd.to_numeric(
        df["Dollar Allocation"], errors="coerce"
    )

    # Derived columns.
    df["is_option"] = df.apply(_is_option, axis=1)
    df["effective_ticker"] = df.apply(_effective_ticker, axis=1)
    df["clean_ticker"] = df["effective_ticker"].apply(_clean_ticker)

    df.reset_index(drop=True, inplace=True)
    print(f"Loaded {len(df)} positions ({df['is_option'].sum()} options).")
    return df

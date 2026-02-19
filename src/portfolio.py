"""Portfolio loading and filtering from Excel holdings files.

Reads the most recent .xlsx from the assets directory, selects the
required columns, filters out summary / empty rows, and determines
whether each row is a stock or an option.

The input file uses **Basket Allocation** (percentage of total) instead
of a dollar amount.  Dollar amounts are computed later by multiplying
the basket allocation by the account net liquidation value.
"""

import os
import re

import pandas as pd

from src.config import (
    ASSETS_DIR, OPTION_TICKER_REDIRECTS, STOCK_TICKER_REDIRECTS,
)

# Columns we need from the Excel file (header row names).
_REQUIRED_COLUMNS = [
    "Ticker",
    "Security Ticker",
    "Name",
    "Basket Allocation",
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
OPT_TICKER_RE = re.compile(
    r"^(?P<underlying>[A-Z]+)"           # underlying symbol
    r"\s+[A-Z]{2}"                       # country code (ignored)
    r"\s+(?P<month>\d{2})/(?P<day>\d{2})/(?P<year>\d{2})"  # MM/DD/YY
    r"\s+(?P<right>[CP])"                # C = Call, P = Put
    r"(?P<strike>[\d.]+)"               # strike price
    r"(?:\s+\S+)?$"                      # optional trailing suffix (e.g. Equity)
)


def _is_option(row: pd.Series) -> bool:
    """Heuristic: the row represents an option contract."""
    name = str(row.get("Name", ""))
    ticker = str(row.get("Ticker", "") or "")
    if "Calls on" in name or "Puts on" in name:
        return True
    if OPT_TICKER_RE.match(ticker.strip()):
        return True
    return False


def _clean_ticker(row: pd.Series) -> str:
    """Pick the best ticker for a row and strip Bloomberg-style suffixes.

    Prefers ``Security Ticker`` when present, falls back to ``Ticker``.
    Strips trailing country + asset-class tags
    (e.g. ``'NVDA US Equity'`` → ``'NVDA'``).
    """
    sec = row.get("Security Ticker")
    raw = str(sec).strip() if pd.notna(sec) and str(sec).strip() else \
        str(row.get("Ticker", "")).strip()
    return re.sub(
        r"\s+[A-Z]{2}\s+(?:Equity|Index)$", "", raw,
        flags=re.IGNORECASE,
    ).strip()


def _ticker_prefix(row: pd.Series) -> str:
    """Return the ticker prefix used for redirection matching.

    Options use the Ticker column; stocks use Security Ticker
    (falling back to Ticker).  Only the part before the first space
    is returned, upper-cased.
    """
    if row.get("is_option"):
        raw = str(row.get("Ticker", "")).strip()
    else:
        sec = row.get("Security Ticker")
        raw = (str(sec).strip()
               if pd.notna(sec) and str(sec).strip()
               else str(row.get("Ticker", "")).strip())
    parts = raw.split()
    return parts[0].upper() if parts else ""


def _apply_ticker_redirects(df: pd.DataFrame) -> pd.DataFrame:
    """Merge source rows' Basket Allocation into target rows and drop
    the sources.

    Processes ``OPTION_TICKER_REDIRECTS`` (option rows only) and
    ``STOCK_TICKER_REDIRECTS`` (stock rows only).  When multiple
    target rows match, the redirected allocation is distributed
    proportionally to their existing allocations.
    """
    if not OPTION_TICKER_REDIRECTS and not STOCK_TICKER_REDIRECTS:
        return df

    prefixes = df.apply(_ticker_prefix, axis=1)
    rows_to_drop: set[int] = set()

    for redirects, is_opt in [
        (OPTION_TICKER_REDIRECTS, True),
        (STOCK_TICKER_REDIRECTS, False),
    ]:
        for source, target in redirects.items():
            src_upper = source.strip().upper()
            tgt_upper = target.strip().upper()

            type_mask = df["is_option"] == is_opt
            source_mask = type_mask & (prefixes == src_upper)
            target_mask = type_mask & (prefixes == tgt_upper)

            source_idx = df.index[source_mask].tolist()
            target_idx = df.index[target_mask].tolist()

            if not source_idx:
                continue
            if not target_idx:
                kind = "option" if is_opt else "stock"
                print(f"  [!] Redirect {source} → {target} ({kind}): "
                      f"no target rows found, skipping")
                continue

            total_alloc = df.loc[source_idx, "Basket Allocation"].sum()

            target_allocs = df.loc[target_idx, "Basket Allocation"]
            target_total = target_allocs.sum()
            if target_total != 0:
                for idx in target_idx:
                    share = df.loc[idx, "Basket Allocation"] / target_total
                    df.loc[idx, "Basket Allocation"] += total_alloc * share
            else:
                per_target = total_alloc / len(target_idx)
                for idx in target_idx:
                    df.loc[idx, "Basket Allocation"] += per_target

            rows_to_drop.update(source_idx)
            kind = "option" if is_opt else "stock"
            print(f"  Redirect {source} → {target} ({kind}): "
                  f"{total_alloc:+.4f}% from {len(source_idx)} row(s) "
                  f"→ {len(target_idx)} row(s)")

    if rows_to_drop:
        df = df.drop(list(rows_to_drop)).reset_index(drop=True)

    return df


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
        ``is_option`` and ``clean_ticker``.
    """
    path = xlsx_path or _latest_xlsx(ASSETS_DIR)
    print(f"Reading portfolio from {path} ...")

    df = pd.read_excel(path, engine="openpyxl")

    # Keep only the columns we care about (ignore extras gracefully).
    available = [c for c in _REQUIRED_COLUMNS if c in df.columns]
    df = df[available].copy()

    # Filter: drop rows with empty Name (summary / header rows)
    # and the literal dash used as a placeholder for Cash.
    name_str = df["Name"].astype(str).str.strip()
    df = df[df["Name"].notna() & (name_str != "") & (name_str != "-")]

    # Ensure Basket Allocation is numeric (percentage of total).
    df["Basket Allocation"] = pd.to_numeric(
        df["Basket Allocation"], errors="coerce"
    )

    # Derived columns.
    df["is_option"] = df.apply(_is_option, axis=1)
    df["clean_ticker"] = df.apply(_clean_ticker, axis=1)

    # Apply ticker redirections (merge allocations, drop source rows).
    df = _apply_ticker_redirects(df)

    df.reset_index(drop=True, inplace=True)
    print(f"Loaded {len(df)} positions ({df['is_option'].sum()} options).")
    return df

"""ConID resolution for stocks and options.

For stocks we call POST /iserver/secdef/search.
For options we first resolve the underlying conid, then call
GET /iserver/secdef/info to find the specific option contract.

Fallback chain when a ticker is not found:
  1. Retry the IBKR search using the company *name* instead of the ticker.
  2. Ask an LLM (OpenAI) to infer the IBKR ticker from the company name,
     then retry the search with the suggested ticker.
"""

import os
import re
import time


import pandas as pd

from src.api_client import IBKRClient
from src.config import OPENAI_API_KEY_FILE
from src.portfolio import _OPT_TICKER_RE, MONTH_MAP


# ------------------------------------------------------------------
# Helpers – MIC sanitisation
# ------------------------------------------------------------------

def _safe_mic(mic) -> str | None:
    """Return *mic* as an uppercase string, or None if it's missing/NaN."""
    if mic is None:
        return None
    if isinstance(mic, float):
        # pandas NaN is a float
        return None
    s = str(mic).strip()
    return s if s else None


# ------------------------------------------------------------------
# Helpers – exchange matching
# ------------------------------------------------------------------

# Mapping from IBKR's common exchange abbreviations (as they appear in
# companyHeader, sections, or the exchange field) to ISO MIC codes.
# This lets us compare the API result against the MIC Primary Exchange
# column from the input spreadsheet.
_IBKR_EXCHANGE_TO_MIC: dict[str, str] = {
    # North America
    "NYSE": "XNYS",
    "NASDAQ": "XNAS",
    "ARCA": "ARCX",
    "AMEX": "XASE",
    "BATS": "BATS",
    "IEX": "IEXG",
    "TSE": "XTSE",
    "TSX": "XTSE",
    "VENTURE": "XTSX",
    "MEXI": "XMEX",
    "PSE": "XPHL",
    "PINK": "OTCM",
    # South America
    "B3": "BVMF",
    "BVMF": "BVMF",
    "BVL": "XLIM",
    "BCS": "XSGO",
    # Europe
    "LSE": "XLON",
    "LSEETF": "XLON",
    "FWB": "XFRA",
    "FWB2": "XFRA",
    "SWX": "XSWX",
    "IBIS": "XETR",
    "IBIS2": "XETR",
    "SBF": "XPAR",
    "ENXTPA": "XPAR",
    "AEB": "XAMS",
    "BRU": "XBRU",
    "LIS": "XLIS",
    "MIL": "XMIL",
    "BM": "XMAD",
    "VSE": "XWBO",
    "STO": "XSTO",
    "CPH": "XCSE",
    "HEL": "XHEL",
    "OSE": "XOSL",
    "WSE": "XWAR",
    "IST": "XIST",
    "ATH": "XATH",
    "BUD": "XBUD",
    "PRG": "XPRA",
    # Asia-Pacific
    "TSEJ": "XTKS",
    "SEHK": "XHKG",
    "HKSE": "XHKG",
    "SGX": "XSES",
    "ASX": "XASX",
    "KSE": "XKRX",
    "TWSE": "XTAI",
    "SSE": "XSHG",
    "SZSE": "XSHE",
    "NSE": "XNSE",
    "BSE": "XBOM",
    "NZE": "XNZE",
    # Middle East / Africa
    "TASE": "XTAE",
    "JSE": "XJSE",
}


def exchange_to_mic(exchange: str) -> str:
    """Convert an IBKR exchange abbreviation to a MIC code.

    Falls back to returning the exchange string itself (uppercased) if
    no mapping is found, so direct MIC-to-MIC comparisons still work.
    """
    return _IBKR_EXCHANGE_TO_MIC.get(exchange.upper(), exchange.upper())


def _extract_header_exchange(entry: dict) -> str | None:
    """Extract the exchange abbreviation from the companyHeader field.

    companyHeader typically looks like ``"MAGNA INTERNATIONAL INC (NYSE)"``
    — the exchange is the text inside the trailing parentheses.
    """
    header = entry.get("companyHeader") or ""
    if header.endswith(")"):
        start = header.rfind("(")
        if start != -1:
            return header[start + 1 : -1].strip()
    return None


# Exchange redirects: for certain MIC codes, prefer trading on an
# alternative exchange to sidestep lot-size restrictions (e.g. Japanese
# and Hong Kong stocks often require buying in lots of 100+).
# Each key is the original input MIC; the value is an ordered list of
# MIC codes to try *instead*.  If none of the redirects are found in the
# search results, the original MIC is still attempted as a last resort.
_MIC_EXCHANGE_REDIRECTS: dict[str, list[str]] = {
    "XTKS": ["XFRA", "OTCM"],   # FWB2, then PINK
    "XHKG": ["XFRA", "OTCM"],   # FWB2, then PINK
}


def _find_entry_by_mic(dict_results: list[dict],
                        target_mic: str) -> dict | None:
    """Return the first entry whose exchange resolves to *target_mic*."""
    for entry in dict_results:
        # Try sections.
        for section in entry.get("sections", []):
            if isinstance(section, dict):
                sec_exc = section.get("exchange", "")
                if exchange_to_mic(sec_exc) == target_mic:
                    return entry

        # Try top-level exchange field.
        top_exc = entry.get("exchange", "")
        if top_exc and exchange_to_mic(top_exc) == target_mic:
            return entry

        # Try companyHeader.
        header_exc = _extract_header_exchange(entry)
        if header_exc and exchange_to_mic(header_exc) == target_mic:
            return entry

    return None


def _match_exchange(results: list,
                    mic: str | None) -> tuple[dict | None, str | None]:
    """Pick the search result whose exchange best matches *mic*.

    Matching strategy:
      1. If *mic* has exchange redirects (e.g. XTKS -> FWB2/PINK), try
         each redirect MIC in order.
      2. Fall back to a direct match on the original *mic*.
      3. If nothing matches, return the first entry as default.

    Only dict entries are considered; non-dict items are skipped.

    Returns
    -------
    (entry, effective_mic) : tuple[dict | None, str | None]
        The matched entry and the MIC code that was actually matched.
        *effective_mic* differs from *mic* when a redirect was used.
        ``(None, None)`` when no results are available.
    """
    if not results:
        return None, None

    dict_results = [e for e in results if isinstance(e, dict)]
    if not dict_results:
        return None, None

    if mic:
        mic_upper = mic.upper()

        # Try redirect exchanges first (e.g. FWB2 / PINK for Asian markets).
        redirects = _MIC_EXCHANGE_REDIRECTS.get(mic_upper, [])
        for redirect_mic in redirects:
            match = _find_entry_by_mic(dict_results, redirect_mic)
            if match is not None:
                print(f"    [i] Redirected from {mic_upper} to "
                      f"{redirect_mic} (exchange redirect)")
                return match, redirect_mic

        # Direct match on the original MIC.
        match = _find_entry_by_mic(dict_results, mic_upper)
        if match is not None:
            return match, mic_upper

    return dict_results[0], mic


# ------------------------------------------------------------------
# LLM fallback
# ------------------------------------------------------------------

def _ask_llm_for_ticker(name: str, mic: str | None) -> str | None:
    """Ask an LLM to infer the IBKR ticker for a company name.

    Uses the OpenAI API if the ``OPENAI_API_KEY`` environment variable is
    set.  Returns the suggested ticker string, or None on failure.
    """
    try:
        with open(OPENAI_API_KEY_FILE) as f:
            api_key = f.read().strip()
    except FileNotFoundError:
        print(f"  [!] OpenAI key file not found at {OPENAI_API_KEY_FILE} – skipping LLM fallback.")
        return None
    if not api_key:
        print(f"  [!] OpenAI key file is empty – skipping LLM fallback.")
        return None

    try:
        from openai import OpenAI
    except ImportError:
        print("  [!] openai package not installed – skipping LLM fallback.")
        return None

    exchange_hint = f" (traded on exchange {mic})" if mic else ""
    prompt = (
        f"What is the Interactive Brokers (IBKR) ticker symbol for "
        f"the company \"{name}\"{exchange_hint}? "
        f"Reply with ONLY the ticker symbol, nothing else."
    )

    try:
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=20,
        )
        ticker = response.choices[0].message.content.strip().upper()
        # Basic sanity: should be short, alphanumeric-ish.
        if ticker and len(ticker) <= 12:
            return ticker
    except Exception as exc:
        print(f"  [!] LLM request failed: {exc}")

    return None


# ------------------------------------------------------------------
# Core resolution
# ------------------------------------------------------------------

def _search_conid(client: IBKRClient, query: str,
                  mic: str | None,
                  by_name: bool = False,
                  ) -> tuple[int, str | None, str | None, str | None] | None:
    """Run a single secdef/search for *query*.

    Parameters
    ----------
    by_name : bool
        If True, pass ``name=True`` to the API so the query is treated
        as a company name rather than a ticker symbol.

    Returns
    -------
    tuple[int, str | None, str | None, str | None] | None
        ``(conid, companyName, symbol, effective_mic)`` on success,
        ``None`` on failure.  *effective_mic* is the MIC code that was
        actually matched (may differ from *mic* when a redirect was used).
    """
    try:
        results = client.search_secdef(query, sec_type="STK", name=by_name)
    except Exception as exc:
        print(f"  [!] secdef/search failed for '{query}': {exc}")
        return None

    if not isinstance(results, list):
        return None

    # The API sometimes returns entries with a different secType than
    # requested (e.g. IND instead of STK).  Filter them out.
    results = [
        r for r in results
        if isinstance(r, dict) and r.get("secType", "").upper() == "STK"
    ]

    match, effective_mic = _match_exchange(results, mic)
    if match is None:
        return None

    raw_name = match.get("companyName") or match.get("companyHeader") or ""
    # Strip exchange suffixes: "APPLE INC - NASDAQ" or "APPLE INC (NASDAQ)"
    company_name = raw_name.split("(")[0].rsplit(" - ", 1)[0].strip() or None
    symbol = match.get("symbol")

    conid = match.get("conid")
    if conid is not None:
        return int(conid), company_name, symbol, effective_mic

    for section in match.get("sections", []):
        if isinstance(section, dict) and "conid" in section:
            return int(section["conid"]), company_name, symbol, effective_mic

    return None


def _resolve_stock_conid(client: IBKRClient, symbol: str,
                         mic: str | None,
                         name: str | None = None,
                         ) -> tuple[int, str | None, str | None, str | None] | None:
    """Search for a stock and return (conid, api_name, api_symbol, effective_mic).

    Fallback chain:
      1. Search by company *name* (if provided, with name=true flag).
      2. Search by *symbol* (ticker).
      3. Ask LLM for the IBKR ticker, then search again.

    *effective_mic* is the MIC code that was actually matched (may differ
    from the input *mic* when an exchange redirect was applied).
    """
    # --- Attempt 1: search by company name (preferred) ---
    if name:
        result = _search_conid(client, name, mic, by_name=True)
        if result is not None:
            return result
        print(f"  [~] Name '{name}' not found via name search.")

    # --- Attempt 2: search by ticker symbol ---
    # Strip trailing Bloomberg-style tags like "US Equity" or "JP Index".
    symbol = re.sub(r"\s+[A-Z]{2}\s+(?:Equity|Index)$", "", symbol, flags=re.IGNORECASE)
    result = _search_conid(client, symbol, mic)
    if result is not None:
        _, api_name, _, _ = result
        if name and api_name:
            print(f"  [!] Resolved via ticker fallback. "
                  f"Name mismatch: portfolio='{name}' vs API='{api_name}'")
        return result

    print(f"  [~] Ticker '{symbol}' also not found.")

    # --- Attempt 3: ask LLM for the ticker ---
    if name:
        print(f"  [~] Asking LLM for IBKR ticker for '{name}' ...")
        suggested = _ask_llm_for_ticker(name, mic)
        if suggested and suggested != symbol.upper():
            print(f"  [~] LLM suggested '{suggested}', searching ...")
            result = _search_conid(client, suggested, mic)
            if result is not None:
                _, api_name, _, _ = result
                if api_name:
                    print(f"  [!] Resolved via LLM. "
                          f"Name mismatch: portfolio='{name}' vs API='{api_name}'")
                return result
            print(f"  [!] LLM suggestion '{suggested}' also not found.")
        elif not suggested:
            print(f"  [!] LLM fallback unavailable (check {OPENAI_API_KEY_FILE}).")

    print(f"  [!] Could not resolve conid for '{symbol}'.")
    return None


def _extract_underlying_from_name(name: str | None) -> str | None:
    """Try to extract the underlying symbol from an option name.

    Examples:
      "February 26 Puts on SPX"   -> "SPX"
      "March 26 Calls on FXI US"  -> "FXI"
    """
    if not name:
        return None
    import re
    m = re.search(r"(?:Calls|Puts) on (\S+)", name)
    if m:
        symbol = m.group(1)
        # Strip trailing exchange suffix if present (e.g. "FXI US" -> just "FXI").
        parts = symbol.split()
        return parts[0] if parts else None
    return None


def _resolve_option_conid(client: IBKRClient, ticker: str,
                          mic: str | None,
                          name: str | None = None,
                          ) -> tuple[int, str | None, str | None, str | None] | None:
    """Resolve an option conid via the underlying + secdef/info.

    Returns ``(conid, api_name, api_symbol, effective_mic)`` or ``None``.
    Options don't use exchange redirects, so *effective_mic* is always
    the original *mic*.
    """
    # Strip trailing category tags that some input files append.
    clean = re.sub(r"\s+(?:Equity|Index)$", "", ticker.strip(), flags=re.IGNORECASE)
    m = _OPT_TICKER_RE.match(clean)
    if not m:
        print(f"  [!] Could not parse option ticker '{ticker}'")
        return None

    underlying = m.group("underlying")
    month_num = m.group("month")
    year_short = m.group("year")    # 2-digit year from the ticker (e.g. "26")
    right = m.group("right")        # "C" or "P"
    strike = float(m.group("strike"))

    # For options we use ticker-first logic (names are not useful here).
    # Try to extract a cleaner underlying symbol from the Name column
    # (e.g. "February 26 Puts on SPX" -> "SPX"), which may differ from
    # the ticker-derived underlying (e.g. "SPXW").
    name_underlying = _extract_underlying_from_name(name)

    # Step 1a – try the ticker-derived underlying first.
    underlying_result = _search_conid(client, underlying, None)

    # Step 1b – if the name gives a different underlying, try that next.
    if underlying_result is None and name_underlying and name_underlying != underlying:
        print(f"  [~] Trying underlying '{name_underlying}' extracted from name ...")
        underlying_result = _search_conid(client, name_underlying, None)

    # Step 1c – last resort: ask LLM for the underlying ticker.
    if underlying_result is None and name:
        print(f"  [~] Asking LLM for IBKR underlying ticker for '{name}' ...")
        suggested = _ask_llm_for_ticker(name, None)
        if suggested and suggested.upper() not in (underlying.upper(),
                                                    (name_underlying or "").upper()):
            print(f"  [~] LLM suggested '{suggested}', searching ...")
            underlying_result = _search_conid(client, suggested, None)

    if underlying_result is None:
        print(f"  [!] Could not resolve underlying for option '{ticker}'")
        return None

    underlying_conid, _, _, _ = underlying_result

    # Build the month string IBKR expects (e.g. "FEB26").
    month_str = MONTH_MAP.get(month_num, month_num)
    ibkr_month = f"{month_str}{year_short}"

    # Step 2 – query option info.
    try:
        results = client.get_secdef_info(
            conid=underlying_conid,
            sec_type="OPT",
            month=ibkr_month,
            right=right,
            strike=strike,
        )
    except Exception as exc:
        print(f"  [!] secdef/info failed for {ticker}: {exc}")
        return None

    if not results:
        print(
            f"  [!] No option contract found for {ticker} "
            f"(underlying conid={underlying_conid}, "
            f"month={ibkr_month}, right={right}, strike={strike})"
        )
        return None

    # Build the target maturity date string (YYYYMMDD) for exact-day
    # matching.  The ticker encodes MM/DD/YY so we can reconstruct it.
    target_maturity = f"20{year_short}{month_num}{m.group('day')}"

    # Filter results to the exact expiration day if possible.
    # The API may return multiple expiries within the same month
    # (e.g. weeklies).  Each result typically has a ``maturityDate``
    # field in YYYYMMDD format.
    day_matches = [
        r for r in results
        if isinstance(r, dict) and str(r.get("maturityDate", "")) == target_maturity
    ]
    if day_matches:
        first = day_matches[0]
    else:
        # No exact date match — pick the result with the closest
        # maturity date to our target.
        target_int = int(target_maturity)
        closest = None
        closest_diff = None
        for r in results:
            if not isinstance(r, dict):
                continue
            mat = r.get("maturityDate")
            if mat is None:
                continue
            try:
                diff = abs(int(str(mat)) - target_int)
            except (ValueError, TypeError):
                continue
            if closest_diff is None or diff < closest_diff:
                closest = r
                closest_diff = diff
        first = closest if closest is not None else (
            results[0] if isinstance(results[0], dict) else {}
        )
        chosen_mat = first.get("maturityDate", "?")
        print(f"  [~] No exact maturity match for {target_maturity}; "
              f"using closest: {chosen_mat}.")

    conid = first.get("conid")
    # Capture the option's description and symbol from the API response.
    api_name = first.get("desc2") or first.get("desc1")
    api_symbol = first.get("symbol") or first.get("tradingClass")
    if conid is not None:
        return int(conid), api_name, api_symbol, mic
    return None


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------

def resolve_conids(client: IBKRClient, df: pd.DataFrame) -> pd.DataFrame:
    """Add a ``conid`` column to *df* by querying the IBKR API.

    Parameters
    ----------
    client : IBKRClient
        Authenticated API client.
    df : pd.DataFrame
        Portfolio table with ``clean_ticker``, ``is_option``, ``Name``,
        and ``MIC Primary Exchange`` columns.

    Returns
    -------
    pd.DataFrame
        The same DataFrame with an added ``conid`` column.
    """
    conids: list[int | None] = []
    api_names: list[str | None] = []
    api_tickers: list[str | None] = []
    effective_mics: list[str | None] = []
    total = len(df)

    for idx, row in df.iterrows():
        ticker = row["clean_ticker"]
        mic = _safe_mic(row.get("MIC Primary Exchange"))
        name = row.get("Name")
        name = str(name).strip() if pd.notna(name) else None
        is_opt = row["is_option"]
        label = f"[{idx + 1}/{total}]"

        if is_opt:
            raw_ticker = str(row.get("Ticker", "")).strip()
            print(f"  {label} Resolving option '{raw_ticker}' ...")
            result = _resolve_option_conid(client, raw_ticker, mic, name)
        else:
            print(f"  {label} Resolving stock  '{ticker}' ...")
            result = _resolve_stock_conid(client, ticker, mic, name)

        if result is not None:
            cid, r_name, r_symbol, eff_mic = result
            conids.append(cid)
            api_names.append(r_name)
            api_tickers.append(r_symbol)
            effective_mics.append(eff_mic)
        else:
            conids.append(None)
            api_names.append(None)
            api_tickers.append(None)
            effective_mics.append(mic)

        # Small delay to respect rate limits.
        time.sleep(0.15)

    df["conid"] = conids
    df["IBKR Name"] = api_names
    df["IBKR Ticker"] = api_tickers

    # Update MIC Primary Exchange to the actual exchange we resolved to.
    # This ensures that downstream filtering (e.g. open-exchange checks)
    # uses the redirected exchange rather than the original input.
    df["MIC Primary Exchange"] = effective_mics

    # Flag rows where the portfolio name differs from the IBKR name.
    def _names_differ(row):
        orig = row.get("Name")
        ibkr = row.get("IBKR Name")
        if pd.isna(orig) or pd.isna(ibkr):
            return None
        return str(orig).strip().upper() != str(ibkr).strip().upper()

    df["Name Mismatch"] = df.apply(_names_differ, axis=1)

    resolved = df["conid"].notna().sum()
    failed = total - resolved
    print(f"\nResolved {resolved}/{total} conids.")
    if failed:
        unresolved = df[df["conid"].isna()][["clean_ticker", "Name"]].to_string(index=False)
        print(f"Unresolved ({failed}):\n{unresolved}")
    print()
    return df

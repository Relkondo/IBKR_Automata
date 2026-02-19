"""ConID resolution for stocks and options via ib_async.

Resolves IBKR contract IDs by querying TWS for each position in the
portfolio DataFrame.

Stocks from Japanese (XTKS) and Hong Kong (XHKG) exchanges are
redirected to German electronic exchanges or OTC to avoid lot-size
rules that prevent small purchases of Asian stocks.
"""

import re
import time

import pandas as pd
from ib_async import IB, Stock, Option

from src.portfolio import OPT_TICKER_RE


# ==================================================================
# Exchange helpers
# ==================================================================

_IBKR_TO_MIC: dict[str, list[str]] = {
    # North America
    "NYSE": ["XNYS"], "NASDAQ": ["XNAS", "XNGS", "XNCM", "XNMS"],
    "ARCA": ["ARCX"], "AMEX": ["XASE"],
    "BATS": ["BATS"], "IEX": ["IEXG"], "TSE": ["XTSE"], "TSX": ["XTSE"],
    "VENTURE": ["XTSX"], "MEXI": ["XMEX"], "PSE": ["XPHL"],
    "PINK": ["OTCM"],
    # South America
    "B3": ["BVMF"], "BVMF": ["BVMF"], "BVL": ["XLIM"], "BCS": ["XSGO"],
    # Europe
    "LSE": ["XLON"], "LSEETF": ["XLON"], "FWB": ["XFRA"], "FWB2": ["XFRA"],
    "EBS": ["XSWX"], "IBIS": ["XETR"], "IBIS2": ["XETR"],
    "SBF": ["XPAR"], "ENXTPA": ["XPAR"], "AEB": ["XAMS"], "ENEXT.BE": ["XBRU"],
    "LIS": ["XLIS"], "BVME": ["XMIL", "MTAA"], "BM": ["XMAD"], "VSE": ["XWBO"],
    "SFB": ["XSTO"], "CPH": ["XCSE"], "HEX": ["XHEL"], "OSE": ["XOSL"],
    "WSE": ["XWAR"], "IST": ["XIST"], "ATH": ["XATH"], "BUD": ["XBUD"],
    "PRG": ["XPRA"],
    # Asia-Pacific
    "TSEJ": ["XTKS"], "SEHK": ["XHKG"], "HKSE": ["XHKG"], "SGX": ["XSES"],
    "ASX": ["XASX"], "KSE": ["XKRX"], "TWSE": ["XTAI", "ROCO"], "TPEX": ["ROCO"], "SSE": ["XSHG"],
    "SZSE": ["XSHE"], "NSE": ["XNSE"], "BSE": ["XBOM"], "NZE": ["XNZE"],
    # Middle East / Africa
    "TASE": ["XTAE"], "JSE": ["XJSE"],
}

# MICs where lot-size rules make small purchases impractical.
# Preferred alternatives: German electronic exchanges, then OTC.
_REDIRECT_MICS: dict[str, list[str]] = {
    "XTKS": ["XFRA", "OTCM"],   # FWB2 first, PINK second
    "XHKG": ["XFRA", "OTCM"],
}

# Reverse mapping: MIC -> IBKR exchange abbreviations.
_MIC_TO_IBKR: dict[str, list[str]] = {}
for _abbr, _mics in _IBKR_TO_MIC.items():
    for _mic in _mics:
        _MIC_TO_IBKR.setdefault(_mic, [])
        if _abbr not in _MIC_TO_IBKR[_mic]:
            _MIC_TO_IBKR[_mic].append(_abbr)
# Prefer FWB2 / IBIS2 — they cover more international stocks.
for _mic, _preferred in [("XFRA", "FWB2"), ("XETR", "IBIS2")]:
    if _mic in _MIC_TO_IBKR and _preferred in _MIC_TO_IBKR[_mic]:
        _MIC_TO_IBKR[_mic].remove(_preferred)
        _MIC_TO_IBKR[_mic].insert(0, _preferred)


def exchange_to_mic(exchange: str) -> str:
    """Convert an IBKR exchange abbreviation to its primary MIC code.

    Returns the first (primary) MIC for the given exchange.
    """
    mics = _IBKR_TO_MIC.get(exchange.upper())
    return mics[0] if mics else exchange.upper()


def _mics_of(contract) -> list[str]:
    """Return all MICs for a contract's primary exchange."""
    exc = (contract.primaryExchange or "").upper()
    return list(_IBKR_TO_MIC.get(exc, [exc]))


def _safe_mic(value) -> str | None:
    """Return *value* as an uppercase MIC string, or None if missing."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    return s.upper() if s else None


# ==================================================================
# Listing helpers
# ==================================================================

def _get_listings(ib: IB, symbol: str, exchange: str = "SMART") -> list:
    """Return STK ContractDetails for *symbol* on *exchange*."""
    try:
        return ib.reqContractDetails(Stock(symbol, exchange, ""))
    except Exception as exc:
        print(f"    [!] reqContractDetails('{symbol}', '{exchange}'): {exc}")
        return []


def _search_by_name(ib: IB, name: str) -> list:
    """Search by company name via reqMatchingSymbols, return STK candidates."""
    try:
        descs = ib.reqMatchingSymbols(name)
        return [d for d in (descs or []) if d.contract.secType == "STK"]
    except Exception as exc:
        print(f"    [!] reqMatchingSymbols('{name}'): {exc}")
        return []


def _dedup_rule_ids(raw: str | None) -> str:
    """Deduplicate a comma-separated marketRuleIds string."""
    if not raw:
        return ""
    return ",".join(dict.fromkeys(r.strip() for r in raw.split(",") if r.strip()))


def _result_from(cd, eff_mic: str | None = None):
    """Build the resolution tuple from a ContractDetails."""
    c = cd.contract
    return c.conId, cd.longName, c.symbol, (
        eff_mic or exchange_to_mic(c.primaryExchange or "")
    ), (c.currency or "USD"), _dedup_rule_ids(cd.marketRuleIds)


def _query_on_exchanges(ib: IB, symbol: str, mic: str | None) -> list:
    """Query target exchange(s) for *symbol*, fall back to SMART.

    Tries the specific IBKR exchange(s) mapped to *mic* first.
    Only falls back to SMART if no specific exchange matches.
    """
    if mic:
        for exchange in list(_MIC_TO_IBKR.get(mic, [])):
            details = _get_listings(ib, symbol, exchange)
            if details:
                return details
    return _get_listings(ib, symbol)  # SMART fallback


def _query_all_redirects(
    ib: IB, symbol: str, redirects: list[str],
) -> list[tuple]:
    """Try **all** redirect exchanges for *symbol*.

    Continues through every redirect MIC so that all available
    listings are discovered.

    Returns a list of ``(ContractDetails, effective_mic)`` tuples.
    """
    hits: list[tuple] = []
    for redirect_mic in redirects:
        for exchange in _MIC_TO_IBKR.get(redirect_mic, []):
            details = _get_listings(ib, symbol, exchange)
            if details:
                hits.append((details[0], redirect_mic))
                break
    return hits


# ==================================================================
# Stock resolution
# ==================================================================

def _resolve_stock(
    ib: IB, symbol: str, mic: str | None, name: str | None,
    positions: dict[int, float] | None = None,
) -> tuple[int, str | None, str | None, str | None, str, str] | None:
    """Resolve a stock to ``(conid, long_name, api_symbol, effective_mic,
    currency, market_rule_ids)``.

    Non-redirected exchanges:
      1. Try ticker on target exchange(s), then SMART.
         Pick first result that matches the expected exchange.
      2. If none, search by name. Pick first result on expected exchange.

    Redirected exchanges (JP / HK → FWB2 / PINK):
      1. Search by name across all redirect exchanges.
      2. Among conids where the user already holds a position, prefer
         the one with the highest exposure (most shares held).
      3. Otherwise, prefer exact name match, then first hit in the
         default redirect order (XFRA > OTCM).
    """
    redirects = _REDIRECT_MICS.get(mic, []) if mic else []

    if redirects:
        return _resolve_redirected(
            ib, symbol, mic, name, redirects, positions or {})
    else:
        return _resolve_direct(ib, symbol, mic, name)


def _resolve_direct(
    ib: IB, symbol: str, mic: str | None, name: str | None,
) -> tuple[int, str | None, str | None, str | None, str, str] | None:
    """Non-redirected: try ticker, then name.  Pick first on expected exchange."""
    acceptable = [mic] if mic else []

    # --- 1. Try ticker ---
    details = _query_on_exchanges(ib, symbol, mic)
    if details:
        if acceptable:
            for cd in details:
                if any(m in acceptable for m in _mics_of(cd.contract)):
                    return _result_from(cd, mic)
        else:
            return _result_from(details[0])

    # --- 2. Try name ---
    if name:
        print(f"    [~] Ticker '{symbol}' not found; searching by name …")
        for desc in _search_by_name(ib, name):
            desc_details = _query_on_exchanges(ib, desc.contract.symbol, mic)
            if desc_details:
                if acceptable:
                    for cd in desc_details:
                        if any(m in acceptable for m in _mics_of(cd.contract)):
                            print(f"    [~] Name search → "
                                  f"symbol '{desc.contract.symbol}'")
                            return _result_from(cd, mic)
                else:
                    print(f"    [~] Name search → "
                          f"symbol '{desc.contract.symbol}'")
                    return _result_from(desc_details[0])

    return None


def _resolve_redirected(
    ib: IB, symbol: str, mic: str | None, name: str | None,
    redirects: list[str],
    positions: dict[int, float],
) -> tuple[int, str | None, str | None, str | None, str, str] | None:
    """Redirected: search all redirect exchanges, prefer the one the
    user already has the most exposure on.

    Priority: existing position (largest ``abs(qty)``) → exact name
    match → first hit (default ``_REDIRECT_MICS`` order) → original
    exchange fallback.
    """
    if not name:
        print(f"    [!] No name for redirected exchange — cannot resolve")
        return None

    candidates = _search_by_name(ib, name)
    if not candidates:
        print(f"    [!] No candidates found for name '{name}'")
        return None

    name_upper = name.strip().upper()

    # Collect ALL hits across all candidates and all redirect exchanges.
    # Each entry: (ContractDetails, effective_mic, is_name_match)
    all_hits: list[tuple] = []
    seen_conids: set[int] = set()

    for desc in candidates:
        desc_name = (desc.contract.description or "").strip().upper()
        is_name_match = desc_name == name_upper

        hits = _query_all_redirects(ib, desc.contract.symbol, redirects)
        for cd, eff_mic in hits:
            cid = cd.contract.conId
            if cid not in seen_conids:
                seen_conids.add(cid)
                all_hits.append((cd, eff_mic, is_name_match))

    if not all_hits:
        print(f"    [~] No redirect listing; falling back to original "
              f"exchange")
        return _resolve_direct(ib, symbol, mic, name)

    # --- Pick the best hit ---

    # Priority 1: the conid where the user holds the largest position.
    held = [
        h for h in all_hits
        if abs(positions.get(h[0].contract.conId, 0)) > 0
    ]
    if held:
        best = max(held,
                   key=lambda h: abs(positions.get(h[0].contract.conId, 0)))
        cd, eff_mic, _ = best
        qty = int(positions[cd.contract.conId])
        print(f"    [>] Preferring {eff_mic} (conid {cd.contract.conId})"
              f" — existing position of {qty} shares")
        return _result_from(cd, eff_mic)

    # Priority 2: exact name match (current default behaviour).
    for cd, eff_mic, is_match in all_hits:
        if is_match:
            print(f"    [>] Name match → "
                  f"'{cd.contract.symbol}' on {eff_mic}")
            return _result_from(cd, eff_mic)

    # Priority 3: first hit (respects _REDIRECT_MICS order).
    cd, eff_mic, _ = all_hits[0]
    print(f"    [>] Exchange match → '{cd.contract.symbol}' on {eff_mic}")
    return _result_from(cd, eff_mic)


# ==================================================================
# Option resolution
# ==================================================================

def _resolve_option(
    ib: IB, ticker: str, mic: str | None, name: str | None,
) -> tuple[int, str | None, str | None, str | None, str, str] | None:
    """Resolve an option to ``(conid, description, symbol, mic,
    currency, market_rule_ids)``.

    Parses tickers like ``"QQQ US 02/27/26 P600 Equity"``, qualifies
    the underlying stock, then looks up the exact option contract.
    """
    clean = re.sub(
        r"\s+(?:Equity|Index)$", "", ticker.strip(), flags=re.IGNORECASE,
    )
    m = OPT_TICKER_RE.match(clean)
    if not m:
        print(f"    [!] Cannot parse option ticker '{ticker}'")
        return None

    underlying = m.group("underlying")
    mm, dd, yy = m.group("month"), m.group("day"), m.group("year")
    right = m.group("right")        # "C" or "P"
    strike = float(m.group("strike"))
    expiry = f"20{yy}{mm}{dd}"

    # Qualify the underlying.
    und_details = _get_listings(ib, underlying)

    # Fallback: extract underlying from the Name ("March 26 Puts on SPX").
    if not und_details and name:
        m2 = re.search(r"(?:Calls|Puts) on (\S+)", name)
        if m2:
            alt = m2.group(1).split()[0]
            print(f"    [~] Trying underlying '{alt}' from name …")
            und_details = _get_listings(ib, alt)

    if not und_details:
        print(f"    [!] Cannot resolve underlying '{underlying}'")
        return None

    und_symbol = und_details[0].contract.symbol

    # Look up the option contract.
    opt = Option(und_symbol, expiry, strike, right, "SMART")
    try:
        opt_details = ib.reqContractDetails(opt)
    except Exception as exc:
        print(f"    [!] Option lookup failed: {exc}")
        return None

    if not opt_details:
        print(f"    [!] No option contract found for {ticker}")
        return None

    od = opt_details[0]
    c = od.contract
    desc = (
        od.longName
        or f"{c.symbol} {c.lastTradeDateOrContractMonth} {right}{strike}"
    )
    return c.conId, desc, c.symbol, mic, (c.currency or "USD"), _dedup_rule_ids(od.marketRuleIds)


# ==================================================================
# Public API
# ==================================================================

def resolve_conids(ib: IB, df: pd.DataFrame) -> pd.DataFrame:
    """Add ``conid``, ``IBKR Name``, ``IBKR Ticker``, and ``Name Mismatch``
    columns to *df* by querying TWS.

    Overwrites ``MIC Primary Exchange`` with the effective exchange
    (which may differ from the input when JP/HK redirects apply).

    For redirected exchanges (JP / HK), existing IBKR positions are
    checked so the resolver prefers the redirect exchange where the
    user already holds the most shares.
    """
    # Pre-fetch IBKR positions so redirected-exchange resolution can
    # prefer the exchange where the user already has the most exposure.
    positions: dict[int, float] = {}
    for pos in ib.positions():
        cid = pos.contract.conId
        if cid:
            positions[cid] = float(pos.position)

    conids: list[int | None] = []
    api_names: list[str | None] = []
    api_tickers: list[str | None] = []
    eff_mics: list[str | None] = []
    currencies: list[str | None] = []
    market_rule_ids: list[str | None] = []
    total = len(df)

    for idx, row in df.iterrows():
        symbol = row["clean_ticker"]
        mic = _safe_mic(row.get("MIC Primary Exchange"))
        name = row.get("Name")
        name = str(name).strip() if pd.notna(name) else None
        is_opt = row["is_option"]
        label = f"[{idx + 1}/{total}]"

        if is_opt:
            raw = str(row.get("Ticker", "")).strip()
            print(f"  {label} Option  '{raw}' …")
            result = _resolve_option(ib, raw, mic, name)
        else:
            print(f"  {label} Stock   '{symbol}' …")
            result = _resolve_stock(ib, symbol, mic, name, positions)

        if result:
            cid, r_name, r_sym, eff, ccy, mrids = result
        else:
            cid, r_name, r_sym, eff, ccy, mrids = (
                None, None, None, mic, None, None)
            print(f"    [!] FAILED to resolve '{symbol}'")

        conids.append(cid)
        api_names.append(r_name)
        api_tickers.append(r_sym)
        eff_mics.append(eff)
        currencies.append(ccy)
        market_rule_ids.append(mrids)

        time.sleep(0.05)

    df["conid"] = conids
    df["IBKR Name"] = api_names
    df["IBKR Ticker"] = api_tickers
    df["MIC Primary Exchange"] = eff_mics
    df["currency"] = currencies
    df["market_rule_ids"] = market_rule_ids

    # Flag rows where the portfolio name differs from what IBKR returned.
    def _names_differ(row):
        a, b = row.get("Name"), row.get("IBKR Name")
        if pd.isna(a) or pd.isna(b):
            return None
        return str(a).strip().upper() != str(b).strip().upper()

    df["Name Mismatch"] = df.apply(_names_differ, axis=1)

    resolved = df["conid"].notna().sum()
    print(f"\nResolved {resolved}/{total} conids.")
    if resolved < total:
        missed = df[df["conid"].isna()][["clean_ticker", "Name"]]
        print(
            f"Unresolved ({total - resolved}):\n"
            f"{missed.to_string(index=False)}"
        )
    print()
    return df

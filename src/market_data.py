"""Fetch market-data snapshots and compute limit prices via ib_async.

Uses ``ib.reqTickers()`` for efficient batch snapshot requests and
``Forex()`` contracts for exchange rates.

Fields used: bid, ask, last, close, high (day), low (day).

Limit-price formula uses ``LIMIT_PRICE_OFFSET`` to compute the cap
(buy) or floor (sell) for Relative (REL) orders:
  BUY  limit = bid  * (1 + LIMIT_PRICE_OFFSET / 100)
  SELL limit = ask  * (1 - LIMIT_PRICE_OFFSET / 100)
  Fallback chain: bid/ask → last → close
"""

import json
import math
import os
import urllib.request

import pandas as pd
from ib_async import IB, Contract, Forex

from src.config import (
    LIMIT_PRICE_OFFSET, MINIMUM_CASH_RESERVE, OUTPUT_DIR,
    PROJECT_PORTFOLIO_COLUMNS,
)
from src.connection import ensure_connected, suppress_errors


# ==================================================================
# Helpers
# ==================================================================

def _safe_float(val) -> float | None:
    """Return *val* as a positive float, or None.

    Filters out ``None``, NaN, Inf, and negative sentinels (ib_async
    uses ``-1`` to indicate unavailable data).
    """
    if val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f) or f < 0:
            return None
        return f
    except (TypeError, ValueError):
        return None


# ==================================================================
# Market-data snapshots
# ==================================================================

SNAPSHOT_BATCH_SIZE = 50


def snapshot_batch(
    ib: IB, contracts: list[Contract],
) -> dict[int, dict]:
    """Request snapshot tickers for a batch of contracts.

    Uses ``ib.reqTickers()`` which is blocking and returns when all
    snapshots are ready.  No manual polling needed.

    Returns ``{conid: {bid, ask, last, close, high, low}}``.
    """
    if not contracts:
        return {}

    result: dict[int, dict] = {}

    try:
        with suppress_errors(354):
            tickers = ib.reqTickers(*contracts)
    except Exception as exc:
        print(f"  [!] reqTickers failed: {exc}")
        return result

    for t in tickers:
        if not t.contract:
            continue
        cid = t.contract.conId
        bid = _safe_float(t.bid)
        ask = _safe_float(t.ask)
        last = _safe_float(t.last)
        close = _safe_float(t.close)
        high = _safe_float(t.high)
        low = _safe_float(t.low)

        result[cid] = {
            "bid": bid, "ask": ask,
            "last": last, "close": close,
            "high": high, "low": low,
        }

    n_with_ba = sum(
        1 for r in result.values()
        if r["bid"] is not None and r["ask"] is not None
    )
    print(f"    {n_with_ba}/{len(contracts)} with bid/ask, "
          f"{len(result)}/{len(contracts)} with any data")

    return result


# ==================================================================
# Tick-size snapping
# ==================================================================

# Cache: market_rule_id -> sorted list of (lowEdge, increment)
_market_rule_cache: dict[int, list[tuple[float, float]]] = {}


def _fetch_single_rule(ib: IB, rule_id: int) -> list[tuple[float, float]]:
    """Fetch and cache a single market rule by ID."""
    if rule_id in _market_rule_cache:
        return _market_rule_cache[rule_id]

    try:
        increments = ib.reqMarketRule(rule_id)
        if increments:
            rules = sorted(
                [(float(pi.lowEdge), float(pi.increment))
                 for pi in increments],
                key=lambda x: x[0],
            )
        else:
            rules = []
    except Exception as exc:
        print(f"  [!] reqMarketRule({rule_id}) failed: {exc}")
        rules = []

    _market_rule_cache[rule_id] = rules
    return rules


def _applicable_increment(
    rules: list[tuple[float, float]], price: float,
) -> float:
    """Return the tick increment applicable to *price* from *rules*."""
    if not rules:
        return 0.0
    increment = rules[0][1]
    for low_edge, inc in rules:
        if price >= low_edge:
            increment = inc
        else:
            break
    return increment


def snap_to_tick(
    price: float,
    ib: IB,
    rule_ids_str: str,
    is_buy: bool = True,
) -> float:
    """Snap *price* to the most restrictive valid tick increment.

    Checks **all** market rule IDs in *rule_ids_str* (which correspond
    to the different exchanges the contract can trade on).  For SMART
    routing the order must be valid on whichever exchange is selected,
    so we use the **largest** (most restrictive) tick at the given
    price level.

    For BUY orders, rounds **down** (conservative — we don't overpay).
    For SELL orders, rounds **up** (conservative — we don't undersell).
    """
    if not rule_ids_str or price <= 0:
        return price

    # Find the largest applicable tick across all rule sets.
    max_tick = 0.0
    for rid_str in rule_ids_str.split(","):
        rid_str = rid_str.strip()
        if not rid_str:
            continue
        rules = _fetch_single_rule(ib, int(rid_str))
        tick = _applicable_increment(rules, price)
        if tick > max_tick:
            max_tick = tick

    if max_tick <= 0:
        return price

    if is_buy:
        return math.floor(price / max_tick) * max_tick
    else:
        return math.ceil(price / max_tick) * max_tick


def _ensure_market_rules(
    ib: IB, df: pd.DataFrame, contracts: list[Contract],
) -> pd.DataFrame:
    """Populate the ``market_rule_ids`` column if absent or empty.

    Fetches contract details for each contract to obtain the
    market rule IDs needed for tick-size snapping.
    """
    has_rules = (
        "market_rule_ids" in df.columns
        and not df.loc[df["conid"].notna(), "market_rule_ids"]
                  .fillna("").astype(str).str.strip().eq("").all()
    )
    if has_rules:
        return df

    print("  Fetching market rules for tick-size snapping …")
    mrids_map: dict[int, str] = {}
    for c in contracts:
        try:
            cds = ib.reqContractDetails(c)
            if cds:
                raw = cds[0].marketRuleIds or ""
                mrids_map[c.conId] = ",".join(
                    dict.fromkeys(r.strip() for r in raw.split(",")
                                  if r.strip())
                )
        except Exception:
            pass
    df["market_rule_ids"] = df["conid"].apply(
        lambda cid: mrids_map.get(int(cid), "")
        if pd.notna(cid) else ""
    )
    print(f"  Market rules fetched for {len(mrids_map)} contracts.")
    return df


def _snap_limit_price(row, ib: IB) -> float | None:
    """Snap a row's limit price to a valid tick increment."""
    lp = row.get("limit_price")
    if pd.isna(lp):
        return None
    mrids = row.get("market_rule_ids")
    if pd.isna(mrids) or not str(mrids).strip():
        return lp
    da = row.get("Dollar Allocation")
    is_buy = pd.isna(da) or float(da) >= 0
    snapped = snap_to_tick(float(lp), ib, str(mrids), is_buy=is_buy)
    return round(snapped, 10)  # clean floating-point noise


# ==================================================================
# Limit-price calculation
# ==================================================================

def calc_limit_price(
    row, *, is_sell: bool | None = None,
) -> tuple[float | None, str | None]:
    """Compute the cap/floor limit price for a Relative (REL) order.

    Uses ``LIMIT_PRICE_OFFSET`` (a percentage of the reference price):

    BUY  → limit = ref_price * (1 + LIMIT_PRICE_OFFSET / 100)
    SELL → limit = ref_price * (1 - LIMIT_PRICE_OFFSET / 100)

    Reference price priority: bid (buy) / ask (sell), then last, then
    close.  The result is the maximum willing-to-pay (buy) or minimum
    willing-to-accept (sell).

    Parameters
    ----------
    is_sell : bool | None
        Override buy/sell determination.  When ``None`` (default), the
        direction is inferred from the row's ``Dollar Allocation``.

    Returns
    -------
    (limit_price, price_source)
        The computed limit price (or ``None``) and the name of the
        market-data field used as reference (``"bid"``, ``"ask"``,
        ``"last"``, ``"close"``, or ``None``).
    """
    bid = row.get("bid")
    ask = row.get("ask")
    last = row.get("last")
    close = row.get("close")

    if is_sell is None:
        dollar_alloc = row.get("Dollar Allocation")
        is_sell = pd.notna(dollar_alloc) and float(dollar_alloc) < 0

    multiplier = (1 - LIMIT_PRICE_OFFSET / 100) if is_sell \
        else (1 + LIMIT_PRICE_OFFSET / 100)

    # Primary: use bid (buy) or ask (sell) as reference.
    if is_sell and pd.notna(ask) and float(ask) > 0:
        return round(float(ask) * multiplier, 2), "ask"
    if not is_sell and pd.notna(bid) and float(bid) > 0:
        return round(float(bid) * multiplier, 2), "bid"

    # Fallback 1: last traded price.
    if pd.notna(last) and float(last) > 0:
        return round(float(last) * multiplier, 2), "last"

    # Fallback 2: close price.
    if pd.notna(close) and float(close) > 0:
        return round(float(close) * multiplier, 2), "close"

    # Fallback 3: any available price (no offset applied).
    if pd.notna(bid) and float(bid) > 0:
        return round(float(bid), 2), "bid"
    if pd.notna(ask) and float(ask) > 0:
        return round(float(ask), 2), "ask"

    return None, None


# ==================================================================
# Quantity & allocation helpers
# ==================================================================

def get_fx(row) -> float | None:
    """Return the FX rate for a row: 1.0 for USD, the rate for foreign, None if missing."""
    ccy = row.get("currency")
    fx = row.get("fx_rate")
    if pd.isna(ccy) or str(ccy).upper() == "USD":
        return 1.0
    if pd.notna(fx) and float(fx) > 0:
        return float(fx)
    return None


def _multiplier(row) -> int:
    """Return 100 for options, 1 for stocks."""
    return 100 if row.get("is_option") else 1


def _planned_qty(row) -> int | None:
    """Compute the planned share count from limit price and dollar allocation."""
    lp = row.get("limit_price")
    da = row.get("Dollar Allocation")
    if pd.isna(lp) or pd.isna(da) or float(lp) <= 0:
        return None
    fx = get_fx(row)
    if fx is None:
        return None
    local_alloc = abs(float(da)) * fx
    mult = _multiplier(row)
    shares = round(local_alloc / (float(lp) * mult))
    return shares if float(da) >= 0 else -shares


def _actual_dollar_alloc(row) -> float | None:
    """Compute the actual dollar allocation from limit price, qty, and FX."""
    lp = row.get("limit_price")
    qty = row.get("Qty")
    fx = get_fx(row)
    if pd.isna(lp) or pd.isna(qty) or fx is None:
        return None
    return round(
        float(lp) * float(qty) * _multiplier(row) / fx,
        2,
    )


# ==================================================================
# Currency resolution
# ==================================================================

def _try_forex_snapshot(ib: IB, pair: str) -> float | None:
    """Request a snapshot for Forex *pair* and return the rate, or None.

    Qualifies the contract first; skips silently if qualification fails.
    Does NOT call cancelMktData — snapshots auto-cancel on receipt.
    """
    fx = Forex(pair)
    ib.qualifyContracts(fx)
    if not fx.conId:
        return None  # pair doesn't exist on IDEALPRO
    t = ib.reqMktData(fx, snapshot=True)
    ib.sleep(2)
    rate = _safe_float(t.marketPrice())
    return rate if rate and rate > 0 else None


# Currencies where the convention is {ccy}USD (ccy is base, not USD).
_CCY_AS_BASE = {"EUR", "GBP", "AUD", "NZD"}

# ---------- Web-based FX fallback (ExchangeRate-API, free, no key) ----------

_WEB_FX_URL = "https://open.er-api.com/v6/latest/USD"
_web_fx_cache: dict[str, float] | None = None


def _fetch_web_fx_rate(ccy: str) -> float | None:
    """Look up USD -> *ccy* from the free ExchangeRate-API.

    Results are cached for the lifetime of the process (rates update daily).
    Returns the rate (units of *ccy* per 1 USD) or None.
    """
    global _web_fx_cache
    if _web_fx_cache is None:
        try:
            req = urllib.request.Request(
                _WEB_FX_URL,
                headers={"User-Agent": "IBKR_Automata/1.0"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
            if data.get("result") == "success":
                _web_fx_cache = data.get("rates", {})
            else:
                print("  [!] Web FX API returned unexpected payload.")
                _web_fx_cache = {}
        except Exception as exc:
            print(f"  [!] Web FX API request failed: {exc}")
            _web_fx_cache = {}  # don't retry on every call

    rate = _web_fx_cache.get(ccy.upper())
    if rate is not None and rate > 0:
        return float(rate)
    return None


def resolve_fx_rate(ib: IB, ccy: str,
                    auto_mode: bool = False) -> float | None:
    """Obtain the USD -> *ccy* exchange rate.

    Strategy (in order):
      1. IBKR Forex snapshot (standard pair convention, then reverse).
      2. Free web API (open.er-api.com — covers exotic pairs like TWD).
      3. Manual user input as a last resort (interactive only).

    In *auto_mode*, raises ``RuntimeError`` if the rate cannot be
    resolved (instead of prompting for manual input).

    Returns the rate (units of *ccy* per 1 USD) or None.
    """
    # --- Attempt 1: IBKR Forex snapshot ---
    with suppress_errors(200):
        if ccy in _CCY_AS_BASE:
            # Convention: {ccy}USD → price is "USD per 1 ccy", invert.
            rate = _try_forex_snapshot(ib, f"{ccy}USD")
            if rate is not None:
                inverted = round(1.0 / rate, 6)
                print(f"  USD -> {ccy} = {inverted}")
                return inverted
            # Fallback: try reverse.
            rate = _try_forex_snapshot(ib, f"USD{ccy}")
            if rate is not None:
                print(f"  USD -> {ccy} = {rate}")
                return rate
        else:
            # Convention: USD{ccy} → price is "ccy per 1 USD", direct.
            rate = _try_forex_snapshot(ib, f"USD{ccy}")
            if rate is not None:
                print(f"  USD -> {ccy} = {rate}")
                return rate
            # Fallback: try reverse.
            rate = _try_forex_snapshot(ib, f"{ccy}USD")
            if rate is not None:
                inverted = round(1.0 / rate, 6)
                print(f"  USD -> {ccy} = {inverted}")
                return inverted

    # --- Attempt 2: free web API ---
    web_rate = _fetch_web_fx_rate(ccy)
    if web_rate is not None:
        print(f"  USD -> {ccy} = {web_rate} (web)")
        return web_rate

    # --- Attempt 3: manual input (interactive) or abort (auto) ---
    if auto_mode:
        raise RuntimeError(
            f"Could not resolve FX rate for {ccy} "
            f"(IBKR and web API both failed)")

    print(f"  [!] Could not fetch Forex rate for {ccy}.")
    user_input = input(
        f"  Enter USD -> {ccy} rate (or press Enter to skip {ccy}): "
    ).strip()
    if user_input:
        try:
            manual_rate = float(user_input)
            if manual_rate > 0:
                print(f"  USD -> {ccy} = {manual_rate} (manual)")
                return manual_rate
            else:
                print(f"  [!] Invalid rate. Skipping {ccy}.")
        except ValueError:
            print(f"  [!] Not a number. Skipping {ccy}.")

    print(f"  [!] No exchange rate for {ccy}. "
          f"Orders in {ccy} will be skipped.")
    return None


def resolve_currencies(ib: IB, df: pd.DataFrame,
                       auto_mode: bool = False) -> pd.DataFrame:
    """Add ``fx_rate`` column to the portfolio table.

    Reads the ``currency`` column (populated by ``resolve_conids``)
    and fetches exchange rates for unique non-USD currencies via
    Forex snapshots.
    """
    if "currency" not in df.columns:
        df["currency"] = None
        df["fx_rate"] = None
        return df

    # Collect unique non-USD currencies from the DataFrame.
    valid_currencies = df.loc[df["currency"].notna(), "currency"].unique()
    unique_currencies = {
        str(c).upper() for c in valid_currencies
    } - {"USD"}

    if not unique_currencies:
        df["fx_rate"] = df["currency"].apply(
            lambda c: 1.0 if pd.notna(c) and str(c).upper() == "USD" else None
        )
        print("  No foreign currencies to resolve.\n")
        return df

    print(f"Resolving exchange rates for {len(unique_currencies)} "
          f"currencies: {', '.join(sorted(unique_currencies))} ...")

    fx_rates: dict[str, float] = {"USD": 1.0}
    for ccy in sorted(unique_currencies):
        resolved = resolve_fx_rate(ib, ccy, auto_mode=auto_mode)
        if resolved is not None:
            fx_rates[ccy] = resolved

    # Map rates back to each row.
    df["fx_rate"] = df["currency"].apply(
        lambda c: fx_rates.get(str(c).upper()) if pd.notna(c) else None
    )

    n_foreign = (
        df["currency"].notna()
        & (df["currency"].astype(str).str.upper() != "USD")
    ).sum()
    print(f"  {n_foreign} foreign-currency positions identified.\n")
    return df


# ==================================================================
# Public API
# ==================================================================

def get_investable_amount(ib: IB) -> float:
    """Fetch net liquidation from IBKR, subtract the cash reserve, and
    return the investable amount (USD).

    Prints the net liquidation, cash reserve (if non-zero), and
    investable amount.

    Raises
    ------
    RuntimeError
        If the net liquidation value cannot be found.
    """
    summary = ib.accountSummary()
    net_liq: float | None = None
    for item in summary:
        if item.tag == "NetLiquidation" and item.currency == "USD":
            val = float(item.value)
            if val > 0:
                net_liq = val
                break
    if net_liq is None:
        raise RuntimeError(
            "Could not retrieve NetLiquidation (USD) from account summary. "
            "Make sure TWS is connected and has account data loaded."
        )

    investable = net_liq - MINIMUM_CASH_RESERVE
    print(f"Net Liquidation (USD): ${net_liq:,.2f}")
    if MINIMUM_CASH_RESERVE:
        print(f"Cash Reserve          : ${MINIMUM_CASH_RESERVE:,.2f}")
    print(f"Investable Amount     : ${investable:,.2f}\n")
    return investable


def fetch_market_data(ib: IB, df: pd.DataFrame) -> pd.DataFrame:
    """Populate market-data columns and compute limit prices.

    Builds ``Contract`` objects from the ``conid`` column, qualifies
    them in bulk via ``ib.qualifyContracts()``, then fetches snapshots
    using ``ib.reqTickers()`` (batched for safety).

    Only rows with a valid (non-null) conid are queried.
    """
    ensure_connected(ib)

    all_conids = (
        df.loc[df["conid"].notna(), "conid"].astype(int).tolist()
    )

    if not all_conids:
        print("No valid conids to fetch market data for.")
        for col in ("bid", "ask", "last", "close",
                     "day_high", "day_low", "limit_price"):
            df[col] = None
        return df

    print(f"Fetching market data for {len(all_conids)} contracts ...")

    # 1. Qualify contracts.
    contracts = [Contract(conId=cid) for cid in all_conids]
    qualified = ib.qualifyContracts(*contracts)
    print(f"  Qualified {len(qualified)}/{len(all_conids)} contracts")

    cid_to_contract: dict[int, Contract] = {
        c.conId: c for c in qualified if c.conId
    }
    contracts_list = [cid_to_contract[cid] for cid in all_conids
                      if cid in cid_to_contract]

    # 2. Ensure market_rule_ids column exists (needed for tick-size snapping).
    df = _ensure_market_rules(ib, df, contracts_list)

    # 3. Fetch snapshots in batches.
    snapshot: dict[int, dict] = {}
    total_batches = math.ceil(len(contracts_list) / SNAPSHOT_BATCH_SIZE)
    for i in range(0, len(contracts_list), SNAPSHOT_BATCH_SIZE):
        batch = contracts_list[i : i + SNAPSHOT_BATCH_SIZE]
        batch_num = i // SNAPSHOT_BATCH_SIZE + 1
        print(f"  Batch {batch_num}/{total_batches} "
              f"({len(batch)} contracts) …")
        snapshot.update(snapshot_batch(ib, batch))

    # 4. Map snapshot data to DataFrame columns.
    bids, asks, lasts, closes, highs, lows = [], [], [], [], [], []
    for _, row in df.iterrows():
        cid = row.get("conid")
        if pd.notna(cid) and int(cid) in snapshot:
            entry = snapshot[int(cid)]
        else:
            entry = {"bid": None, "ask": None, "last": None,
                     "close": None, "high": None, "low": None}

        bids.append(entry["bid"])
        asks.append(entry["ask"])
        lasts.append(entry["last"])
        closes.append(entry["close"])
        highs.append(entry["high"])
        lows.append(entry["low"])

    df["bid"] = bids
    df["ask"] = asks
    df["last"] = lasts
    df["close"] = closes
    df["day_high"] = highs
    df["day_low"] = lows

    # 5. Compute limit prices and snap to valid tick increments.
    price_pairs = df.apply(calc_limit_price, axis=1, result_type="expand")
    df["limit_price"] = price_pairs[0]
    df["price_source"] = price_pairs[1]
    df["limit_price"] = df.apply(_snap_limit_price, axis=1, ib=ib)

    # 6. Compute planned quantities and actual dollar allocations.
    df["Qty"] = df.apply(_planned_qty, axis=1)
    df["Actual Dollar Allocation"] = df.apply(_actual_dollar_alloc, axis=1)

    got_bid = df["bid"].notna().sum()
    got_last = df["last"].notna().sum()
    got_limit = df["limit_price"].notna().sum()
    print(f"\nMarket data summary:")
    print(f"  Bid/Ask received : {got_bid}/{len(all_conids)}")
    print(f"  Last received    : {got_last}/{len(all_conids)}")
    print(f"  Limit price set  : {got_limit}/{len(all_conids)}\n")
    return df


def save_project_portfolio(df: pd.DataFrame) -> str:
    """Export the portfolio table to ``output/Project_Portfolio.csv``."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "Project_Portfolio.csv")
    # Order columns: listed config columns first, then any extras.
    ordered = [c for c in PROJECT_PORTFOLIO_COLUMNS if c in df.columns]
    extras = [c for c in df.columns if c not in ordered]
    out = df[ordered + extras]
    out.to_csv(out_path, index=False)
    print(f"Portfolio saved to {out_path}")
    return out_path

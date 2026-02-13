"""Fetch market-data snapshots and compute limit prices via ib_async.

Uses ``ib.reqTickers()`` for efficient batch snapshot requests and
``Forex()`` contracts for exchange rates.

Fields used: bid, ask, last, close, high (day), low (day).

Limit-price formula uses a FILL_PATIENCE parameter (0-100) that
controls how aggressively we cross the bid/ask spread:
  0   = cross the spread fully (fills immediately)
  50  = midpoint (balanced)
  100 = sit on the passive side (cheapest, may not fill)
"""

import math
import os

import pandas as pd
from ib_async import IB, Contract, Forex

from src.config import OUTPUT_DIR

# ------------------------------------------------------------------
# Snapshot batching
# ------------------------------------------------------------------
SNAPSHOT_BATCH_SIZE = 50

# ------------------------------------------------------------------
# Limit-price tuning
# ------------------------------------------------------------------
FILL_PATIENCE = 105  # 0 = cross spread immediately, 100 = sit on bid/ask


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

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


def fetch_net_liquidation(ib: IB) -> float:
    """Fetch the account net liquidation value in USD from IBKR.

    Uses ``ib.accountSummary()`` and looks for the ``NetLiquidation``
    tag with ``USD`` currency.

    Raises
    ------
    RuntimeError
        If the net liquidation value cannot be found.
    """
    summary = ib.accountSummary()
    for item in summary:
        if item.tag == "NetLiquidation" and item.currency == "USD":
            val = float(item.value)
            if val > 0:
                return val
    raise RuntimeError(
        "Could not retrieve NetLiquidation (USD) from account summary. "
        "Make sure TWS is connected and has account data loaded."
    )


def _snapshot_batch(
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


# ------------------------------------------------------------------
# Limit-price calculation
# ------------------------------------------------------------------

def _calc_limit_price(row) -> float | None:
    """Compute the limit price for a single row.

    Uses a spread-based formula controlled by ``FILL_PATIENCE`` (0-100):

    BUY  (Dollar Allocation >= 0):
      limit = ask - (ask - bid) * FILL_PATIENCE / 100
        0   → buy at ask  (aggressive, fills fast)
        50  → buy at midpoint
        100 → buy at bid  (patient, may not fill)

    SELL (Dollar Allocation < 0):
      limit = bid + (ask - bid) * FILL_PATIENCE / 100
        0   → sell at bid (aggressive, fills fast)
        50  → sell at midpoint
        100 → sell at ask (patient, may not fill)

    Fallbacks when bid/ask unavailable: ``last``, then ``close``.
    """
    bid = row.get("bid")
    ask = row.get("ask")
    last = row.get("last")
    close = row.get("close")

    dollar_alloc = row.get("Dollar Allocation")
    is_sell = pd.notna(dollar_alloc) and float(dollar_alloc) < 0

    # Primary: spread-based formula when both bid and ask exist.
    if pd.notna(bid) and pd.notna(ask):
        spread = float(ask) - float(bid)
        if spread >= 0:
            if is_sell:
                return round(float(bid) + spread * FILL_PATIENCE / 100, 2)
            else:
                return round(float(ask) - spread * FILL_PATIENCE / 100, 2)

    # Fallback 1: last traded price.
    if pd.notna(last) and float(last) > 0:
        return round(float(last), 2)

    # Fallback 2: close price.
    if pd.notna(close) and float(close) > 0:
        return round(float(close), 2)

    # Fallback 3: any available price.
    if pd.notna(bid) and float(bid) > 0:
        return round(float(bid), 2)
    if pd.notna(ask) and float(ask) > 0:
        return round(float(ask), 2)

    return None


# ------------------------------------------------------------------
# Currency resolution
# ------------------------------------------------------------------


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


def _resolve_fx_rate(ib: IB, ccy: str) -> float | None:
    """Obtain the USD -> *ccy* exchange rate.

    Tries the standard Forex pair convention first, then the reverse.
    Falls back to manual input.

    Returns the rate (units of *ccy* per 1 USD) or None.
    """
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

    # --- Attempt 3: manual input ---
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


def resolve_currencies(ib: IB, df: pd.DataFrame) -> pd.DataFrame:
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
        resolved = _resolve_fx_rate(ib, ccy)
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


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------

def fetch_market_data(ib: IB, df: pd.DataFrame) -> pd.DataFrame:
    """Populate market-data columns and compute limit prices.

    Builds ``Contract`` objects from the ``conid`` column, qualifies
    them in bulk via ``ib.qualifyContracts()``, then fetches snapshots
    using ``ib.reqTickers()`` (batched for safety).

    Only rows with a valid (non-null) conid are queried.
    """
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

    # Build Contract stubs from conid and qualify in bulk.
    contracts = [Contract(conId=cid) for cid in all_conids]
    qualified = ib.qualifyContracts(*contracts)
    print(f"  Qualified {len(qualified)}/{len(all_conids)} contracts")

    # Map conid -> qualified contract (fallback to stub if needed).
    cid_to_contract: dict[int, Contract] = {
        c.conId: c for c in qualified if c.conId
    }
    contracts_list = [cid_to_contract[cid] for cid in all_conids
                      if cid in cid_to_contract]

    # Fetch snapshots in batches via reqTickers.
    snapshot: dict[int, dict] = {}
    total_batches = math.ceil(len(contracts_list) / SNAPSHOT_BATCH_SIZE)
    for i in range(0, len(contracts_list), SNAPSHOT_BATCH_SIZE):
        batch = contracts_list[i : i + SNAPSHOT_BATCH_SIZE]
        batch_num = i // SNAPSHOT_BATCH_SIZE + 1
        print(f"  Batch {batch_num}/{total_batches} "
              f"({len(batch)} contracts) …")
        batch_result = _snapshot_batch(ib, batch)
        snapshot.update(batch_result)

    # Map results back to the DataFrame.
    bids, asks, lasts, closes, highs, lows = [], [], [], [], [], []
    for _, row in df.iterrows():
        cid = row.get("conid")
        if pd.notna(cid) and int(cid) in snapshot:
            entry = snapshot[int(cid)]
            bids.append(entry["bid"])
            asks.append(entry["ask"])
            lasts.append(entry["last"])
            closes.append(entry["close"])
            highs.append(entry["high"])
            lows.append(entry["low"])
        else:
            bids.append(None)
            asks.append(None)
            lasts.append(None)
            closes.append(None)
            highs.append(None)
            lows.append(None)

    df["bid"] = bids
    df["ask"] = asks
    df["last"] = lasts
    df["close"] = closes
    df["day_high"] = highs
    df["day_low"] = lows

    df["limit_price"] = df.apply(_calc_limit_price, axis=1)

    def _get_fx(r) -> float | None:
        ccy = r.get("currency")
        fx = r.get("fx_rate")
        if pd.isna(ccy) or str(ccy).upper() == "USD":
            return 1.0
        if pd.notna(fx) and float(fx) > 0:
            return float(fx)
        return None

    def _planned_qty(r):
        lp = r.get("limit_price")
        da = r.get("Dollar Allocation")
        if pd.isna(lp) or pd.isna(da) or float(lp) <= 0:
            return None
        fx = _get_fx(r)
        if fx is None:
            return None
        local_alloc = abs(float(da)) * fx
        shares = round(local_alloc / float(lp))
        return shares if float(da) >= 0 else -shares

    df["Qty"] = df.apply(_planned_qty, axis=1)
    df["Actual Dollar Allocation"] = df.apply(
        lambda r: round(
            float(r["limit_price"]) * float(r["Qty"]) / (_get_fx(r) or 1.0),
            2,
        )
        if pd.notna(r.get("limit_price")) and pd.notna(r.get("Qty"))
        and _get_fx(r) is not None
        else None,
        axis=1,
    )

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
    drop_cols = [c for c in ("effective_ticker",) if c in df.columns]
    df.drop(columns=drop_cols).to_csv(out_path, index=False)
    print(f"Portfolio saved to {out_path}")
    return out_path

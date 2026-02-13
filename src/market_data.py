"""Fetch market-data snapshots and compute limit prices via ib_async.

Uses ``ib.reqMktData(snapshot=True)`` for price data and ``Forex()``
contracts for exchange rates.

Fields used: bid, ask, marketPrice (mark), high (day high), low (day low).

Limit-price formula uses a SPEED_VS_GREED parameter that controls how
aggressively we undercut/overcut the reference price.
"""

import math
import os

import pandas as pd
from ib_async import IB, Contract, Forex, Ticker as IbTicker

from src.config import OUTPUT_DIR

# ------------------------------------------------------------------
# Polling parameters
# ------------------------------------------------------------------
BATCH_SIZE = 50
POLL_DELAY_SECONDS = 2
MAX_POLLS = 5

# ------------------------------------------------------------------
# Limit-price tuning
# ------------------------------------------------------------------
SPEED_VS_GREED = 20


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _safe_float(val) -> float | None:
    """Return *val* as float if it's a real number, else None."""
    if val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def _poll_snapshot(ib: IB, contracts: list[Contract],
                   ) -> dict[int, dict]:
    """Request snapshot market data for a batch of contracts.

    TWS returns live data when a subscription exists, or delayed data
    otherwise (market data type 3 is set at connection time).  We poll
    with adaptive early-exit logic (up to MAX_POLLS rounds).

    Returns a dict ``{conid: {bid, ask, mark, high, low}}``.
    """
    if not contracts:
        return {}

    result: dict[int, dict] = {}
    tickers: list[IbTicker] = []

    # Request snapshots for all contracts.
    for c in contracts:
        try:
            t = ib.reqMktData(c, genericTickList="",
                              snapshot=True, regulatorySnapshot=False)
            tickers.append(t)
        except Exception as exc:
            print(f"  [!] reqMktData failed for conid {c.conId}: {exc}")

    # Allow initial data to arrive.
    ib.sleep(POLL_DELAY_SECONDS)

    conid_map = {t.contract.conId: t for t in tickers if t.contract}

    for attempt in range(1, MAX_POLLS + 1):
        for cid, t in conid_map.items():
            bid = _safe_float(t.bid)
            ask = _safe_float(t.ask)
            mark = _safe_float(t.marketPrice())
            high = _safe_float(t.high)
            low = _safe_float(t.low)

            if attempt == 1:
                print(f"    conid={cid}: bid={bid!r}, ask={ask!r}, "
                      f"mark={mark!r}, high={high!r}, low={low!r}")

            if cid not in result:
                result[cid] = {
                    "bid": None, "ask": None,
                    "mark": None, "high": None, "low": None,
                }
            rec = result[cid]
            if bid is not None:
                rec["bid"] = bid
            if ask is not None:
                rec["ask"] = ask
            if mark is not None:
                rec["mark"] = mark
            if high is not None:
                rec["high"] = high
            if low is not None:
                rec["low"] = low

        n_with_data = sum(
            1 for r in result.values()
            if any(v is not None for v in r.values())
        )
        print(f"  Poll {attempt}/{MAX_POLLS}: "
              f"{n_with_data}/{len(contracts)} conids with some data")

        # Adaptive early-exit.
        if attempt >= 2:
            all_have_range = all(
                (r["bid"] is not None and r["ask"] is not None)
                or (r["high"] is not None and r["low"] is not None)
                for r in result.values()
            ) if result else False
            if all_have_range and len(result) == len(contracts):
                print("  -> All conids have bid/ask or high/low. Stopping.")
                break

        if attempt >= 3:
            all_have_mark = all(
                r["mark"] is not None for r in result.values()
            ) if result else False
            if all_have_mark and len(result) == len(contracts):
                print("  -> All conids have mark price. Stopping.")
                break

        if attempt < MAX_POLLS:
            ib.sleep(POLL_DELAY_SECONDS)

    no_data = len(contracts) - len(result)
    if no_data > 0:
        print(f"  [!] {no_data} conids got no data after "
              f"{MAX_POLLS} polls.")

    return result


# ------------------------------------------------------------------
# Limit-price calculation
# ------------------------------------------------------------------

def _calc_limit_price(row) -> float | None:
    """Compute the limit price for a single row.

    BUY formula (Dollar Allocation >= 0):
      1. If bid available:  bid - (mark - bid) / SPEED_VS_GREED
      2. Elif day_low available: mark - (mark - day_low) / SPEED_VS_GREED
      3. Else: mark

    SELL formula (Dollar Allocation < 0):
      1. If ask available:  ask - (mark - ask) / SPEED_VS_GREED
      2. Elif day_high available: mark - (mark - day_high) / SPEED_VS_GREED
      3. Else: mark
    """
    mark = row.get("mark")
    bid = row.get("bid")
    ask = row.get("ask")
    high = row.get("day_high")
    low = row.get("day_low")

    if pd.isna(mark) and pd.isna(bid) and pd.isna(ask):
        return None

    dollar_alloc = row.get("Dollar Allocation")
    is_sell = pd.notna(dollar_alloc) and float(dollar_alloc) < 0

    if is_sell:
        if pd.notna(ask) and pd.notna(mark):
            return round(ask - (mark - ask) / SPEED_VS_GREED, 2)
        if pd.notna(high) and pd.notna(mark):
            return round(mark - (mark - high) / SPEED_VS_GREED, 2)
        if pd.notna(mark):
            return round(mark, 2)
        if pd.notna(ask):
            return round(ask, 2)
        return None
    else:
        if pd.notna(bid) and pd.notna(mark):
            return round(bid - (mark - bid) / SPEED_VS_GREED, 2)
        if pd.notna(low) and pd.notna(mark):
            return round(mark - (mark - low) / SPEED_VS_GREED, 2)
        if pd.notna(mark):
            return round(mark, 2)
        if pd.notna(bid):
            return round(bid, 2)
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
    """Add ``currency`` and ``fx_rate`` columns to the portfolio table.

    Uses ``ib.reqContractDetails()`` to fetch the trading currency for
    each contract, and ``Forex()`` snapshots for exchange rates.
    """
    valid = df[df["conid"].notna()].copy()
    all_conids = valid["conid"].astype(int).tolist()

    if not all_conids:
        df["currency"] = None
        df["fx_rate"] = None
        return df

    print(f"Resolving currencies for {len(all_conids)} contracts ...")

    # Build Contract objects from conids and ask for details.
    cid_to_currency: dict[int, str] = {}
    for cid in all_conids:
        c = Contract(conId=cid)
        try:
            details = ib.reqContractDetails(c)
            if details:
                ccy = details[0].contract.currency
                if ccy:
                    cid_to_currency[cid] = ccy.upper()
        except Exception as exc:
            print(f"  [!] Currency fetch failed for conid {cid}: {exc}")
        ib.sleep(0.05)

    # Fetch exchange rates for unique non-USD currencies.
    unique_currencies = set(cid_to_currency.values()) - {"USD"}
    fx_rates: dict[str, float] = {"USD": 1.0}
    for ccy in sorted(unique_currencies):
        resolved = _resolve_fx_rate(ib, ccy)
        if resolved is not None:
            fx_rates[ccy] = resolved

    # Map back to the DataFrame.
    currencies: list[str | None] = []
    rates: list[float | None] = []
    for _, row in df.iterrows():
        cid = row.get("conid")
        if pd.notna(cid) and int(cid) in cid_to_currency:
            ccy = cid_to_currency[int(cid)]
            currencies.append(ccy)
            rates.append(fx_rates.get(ccy))
        else:
            currencies.append(None)
            rates.append(None)

    df["currency"] = currencies
    df["fx_rate"] = rates

    n_foreign = sum(1 for c in currencies if c is not None and c != "USD")
    print(f"  {n_foreign} foreign-currency positions identified.\n")
    return df


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------

def fetch_market_data(ib: IB, df: pd.DataFrame) -> pd.DataFrame:
    """Populate market-data columns and compute limit prices.

    Only rows with a valid (non-null) conid are queried.
    """
    valid = df[df["conid"].notna()].copy()
    all_conids = valid["conid"].astype(int).tolist()

    if not all_conids:
        print("No valid conids to fetch market data for.")
        for col in ("bid", "ask", "mark", "day_high", "day_low", "limit_price"):
            df[col] = None
        return df

    print(f"Fetching market data for {len(all_conids)} contracts ...")

    # Build a Contract for each conid.
    conid_to_contract: dict[int, Contract] = {}
    for cid in all_conids:
        c = Contract(conId=cid)
        try:
            details = ib.reqContractDetails(c)
            if details:
                conid_to_contract[cid] = details[0].contract
            else:
                conid_to_contract[cid] = c
        except Exception:
            conid_to_contract[cid] = c
        ib.sleep(0.02)

    snapshot: dict[int, dict] = {}

    # Process in batches.
    contracts_list = [conid_to_contract[cid] for cid in all_conids
                      if cid in conid_to_contract]
    for i in range(0, len(contracts_list), BATCH_SIZE):
        batch = contracts_list[i : i + BATCH_SIZE]
        print(f"\n  Batch {i // BATCH_SIZE + 1} "
              f"({len(batch)} conids) ...")
        batch_result = _poll_snapshot(ib, batch)
        snapshot.update(batch_result)

    # Map back to the DataFrame.
    bids, asks, marks, highs, lows = [], [], [], [], []
    for _, row in df.iterrows():
        cid = row.get("conid")
        if pd.notna(cid) and int(cid) in snapshot:
            entry = snapshot[int(cid)]
            bids.append(entry["bid"])
            asks.append(entry["ask"])
            marks.append(entry["mark"])
            highs.append(entry["high"])
            lows.append(entry["low"])
        else:
            bids.append(None)
            asks.append(None)
            marks.append(None)
            highs.append(None)
            lows.append(None)

    df["bid"] = bids
    df["ask"] = asks
    df["mark"] = marks
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
        shares = math.floor(local_alloc / float(lp))
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
    got_mark = df["mark"].notna().sum()
    got_limit = df["limit_price"].notna().sum()
    print(f"\nMarket data summary:")
    print(f"  Bid/Ask received : {got_bid}/{len(all_conids)}")
    print(f"  Mark received    : {got_mark}/{len(all_conids)}")
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

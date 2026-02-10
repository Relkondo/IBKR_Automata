"""Fetch market-data snapshots and compute limit prices.

The IBKR snapshot endpoint is subscription-based: the first call only
*initiates* subscriptions.  We therefore issue an explicit priming call,
then poll with an adaptive strategy that stops early once enough data
has been collected.

Fields queried: bid (84), ask (86), mark price (7635),
day high (70), day low (71).

Limit-price formula uses a SPEED_VS_GREED parameter that controls how
aggressively we undercut/overcut the reference price.
"""

import math
import os
import time

import pandas as pd

from src.api_client import IBKRClient
from src.config import OUTPUT_DIR

# ------------------------------------------------------------------
# IBKR field codes
# ------------------------------------------------------------------
FIELD_BID = "84"
FIELD_ASK = "86"
FIELD_MARK = "7635"
FIELD_DAY_HIGH = "70"
FIELD_DAY_LOW = "71"

ALL_FIELDS = [FIELD_BID, FIELD_ASK, FIELD_MARK, FIELD_DAY_HIGH, FIELD_DAY_LOW]

# ------------------------------------------------------------------
# Polling parameters
# ------------------------------------------------------------------
PRIME_DELAY_SECONDS = 3
POLL_DELAY_SECONDS = 2
BATCH_SIZE = 50

# ------------------------------------------------------------------
# Limit-price tuning
# ------------------------------------------------------------------
SPEED_VS_GREED = 10


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _try_float(value) -> float | None:
    """Convert *value* to float, returning None on failure."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _poll_snapshot(client: IBKRClient, conids: list[int]) -> dict[int, dict]:
    """Poll the snapshot endpoint with adaptive early-exit logic.

    Polling strategy (after the priming call):
      Poll 1-2 : always execute.
      After 2  : stop if every conid has bid/ask OR day-high/low.
      Poll 3   : executed only if the above condition is not met.
      After 3  : stop if every conid has at least mark price.
      Poll 4-5 : executed only if some conids still lack mark price.

    Returns a dict mapping ``conid -> {"bid", "ask", "mark", "high", "low"}``
    where each value is ``float | None``.
    """
    # Accumulator – we merge values across polls so later polls can
    # fill in fields that were missing earlier.
    result: dict[int, dict] = {}

    # --- Priming call: initiate subscriptions (data not expected) ---
    try:
        client.get_market_snapshot(conids, fields=ALL_FIELDS)
    except Exception as exc:
        print(f"  [!] Priming snapshot request failed: {exc}")

    time.sleep(PRIME_DELAY_SECONDS)

    if not conids:
        return result

    max_polls = 5

    for attempt in range(1, max_polls + 1):
        try:
            data = client.get_market_snapshot(conids, fields=ALL_FIELDS)
        except Exception as exc:
            print(f"  [!] Snapshot request failed (poll {attempt}): {exc}")
            time.sleep(POLL_DELAY_SECONDS)
            continue

        for entry in data:
            cid = int(entry.get("conid", 0))
            if cid == 0:
                continue

            bid = _try_float(entry.get(FIELD_BID))
            ask = _try_float(entry.get(FIELD_ASK))
            mark = _try_float(entry.get(FIELD_MARK))
            high = _try_float(entry.get(FIELD_DAY_HIGH))
            low = _try_float(entry.get(FIELD_DAY_LOW))

            # Log raw values on the first real poll for diagnostics.
            if attempt == 1:
                print(
                    f"    conid={cid}: bid={bid!r}, ask={ask!r}, "
                    f"mark={mark!r}, high={high!r}, low={low!r}"
                )

            # Merge into accumulator, keeping the best known value.
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

        # --- Reporting ---
        n_with_data = sum(
            1 for r in result.values()
            if any(v is not None for v in r.values())
        )
        print(
            f"  Poll {attempt}/{max_polls}: "
            f"{n_with_data}/{len(conids)} conids with some data"
        )

        # --- Adaptive early-exit checks ---
        if attempt >= 2:
            # After poll 2+: stop if every conid has bid/ask OR high/low.
            all_have_price_range = all(
                (r["bid"] is not None and r["ask"] is not None)
                or (r["high"] is not None and r["low"] is not None)
                for r in result.values()
            ) if result else False

            if all_have_price_range and len(result) == len(conids):
                print("  -> All conids have bid/ask or high/low. Stopping.")
                break

        if attempt >= 3:
            # After poll 3+: stop if every conid has at least mark price.
            all_have_mark = all(
                r["mark"] is not None for r in result.values()
            ) if result else False

            if all_have_mark and len(result) == len(conids):
                print("  -> All conids have mark price. Stopping.")
                break

        if attempt < max_polls:
            time.sleep(POLL_DELAY_SECONDS)

    no_data = len(conids) - len(result)
    if no_data > 0:
        print(
            f"  [!] {no_data} conids got no data after "
            f"{max_polls} polls. Data may be unavailable."
        )

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

    # We need at least mark price to compute anything useful.
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
# Public API
# ------------------------------------------------------------------

def fetch_market_data(client: IBKRClient, df: pd.DataFrame) -> pd.DataFrame:
    """Populate market-data columns and compute limit prices.

    Only rows with a valid (non-null) conid are queried.

    New columns added: ``bid``, ``ask``, ``mark``, ``day_high``,
    ``day_low``, ``limit_price``.
    """
    valid = df[df["conid"].notna()].copy()
    all_conids = valid["conid"].astype(int).tolist()

    if not all_conids:
        print("No valid conids to fetch market data for.")
        for col in ("bid", "ask", "mark", "day_high", "day_low", "limit_price"):
            df[col] = None
        return df

    print(f"Fetching market data for {len(all_conids)} contracts ...")

    snapshot: dict[int, dict] = {}

    # Process in batches.
    for i in range(0, len(all_conids), BATCH_SIZE):
        batch = all_conids[i : i + BATCH_SIZE]
        print(f"\n  Batch {i // BATCH_SIZE + 1} "
              f"({len(batch)} conids) ...")
        batch_result = _poll_snapshot(client, batch)
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

    # Qty = floor(|Dollar Allocation| / limit_price), signed.
    # Actual Dollar Allocation = limit_price * |Qty|.
    def _planned_qty(r):
        lp = r.get("limit_price")
        da = r.get("Dollar Allocation")
        if pd.isna(lp) or pd.isna(da) or float(lp) <= 0:
            return None
        shares = math.floor(abs(float(da)) / float(lp))
        return shares if float(da) >= 0 else -shares

    df["Qty"] = df.apply(_planned_qty, axis=1)
    df["Actual Dollar Allocation"] = df.apply(
        lambda r: round(float(r["limit_price"]) * abs(float(r["Qty"])), 2)
        if pd.notna(r.get("limit_price")) and pd.notna(r.get("Qty"))
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
    """Export the portfolio table to ``output/Project_Portfolio.csv``.

    Returns the path to the written file.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "Project_Portfolio.csv")
    # Drop internal helper columns that aren't useful in the output.
    drop_cols = [c for c in ("effective_ticker",) if c in df.columns]
    df.drop(columns=drop_cols).to_csv(out_path, index=False)
    print(f"Portfolio saved to {out_path}")
    return out_path

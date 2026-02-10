"""Reconcile the target portfolio against IBKR's current state.

Compares the Project_Portfolio (desired state) to existing positions and
pending orders on IBKR, computing the *net* quantity that still needs to
be ordered for each conid.  Stale pending orders whose limit price no
longer matches are cancelled and their quantity is reclaimed.

The result is a DataFrame identical to the input but with an extra
``net_quantity`` column that the order loop should use instead of
computing quantity from raw dollar allocation.
"""

from __future__ import annotations

import math
import time

import pandas as pd

from src.api_client import IBKRClient
from src.orders import get_account_id


# ------------------------------------------------------------------
# Data fetchers
# ------------------------------------------------------------------

def _fetch_positions(client: IBKRClient,
                     account_id: str) -> dict[int, float]:
    """Return a mapping ``{conid: signed_position_qty}``."""
    raw = client.get_positions(account_id)
    positions: dict[int, float] = {}
    for entry in raw:
        cid = entry.get("conid")
        qty = entry.get("position", 0)
        if cid is not None:
            positions[int(cid)] = positions.get(int(cid), 0) + float(qty)
    return positions


def _fetch_open_orders(client: IBKRClient) -> list[dict]:
    """Return the list of open orders with normalised fields.

    Each dict has: ``conid``, ``orderId``, ``side``, ``price``,
    ``remainingQuantity``, ``status``.
    """
    raw = client.get_live_orders()
    orders: list[dict] = []
    for entry in raw:
        cid = entry.get("conid") or entry.get("conidex")
        oid = entry.get("orderId") or entry.get("order_id")
        status = str(entry.get("status", "")).lower()

        # Only consider orders that are still active.
        if status in ("cancelled", "filled", "inactive"):
            continue

        side = str(entry.get("side", "")).upper()
        # Normalise IBKR's various side representations.
        if side in ("B", "BOT", "BUY"):
            side = "BUY"
        elif side in ("S", "SLD", "SELL"):
            side = "SELL"

        price = entry.get("price")
        remaining = entry.get("remainingQuantity") or entry.get("remaining_quantity") or entry.get("totalSize") or 0

        if cid is not None and oid is not None:
            orders.append({
                "conid": int(str(cid).split(".")[0]),  # conidex can be "123.0"
                "orderId": str(oid),
                "side": side,
                "price": float(price) if price is not None else None,
                "remainingQuantity": float(remaining),
                "status": status,
            })
    return orders


# ------------------------------------------------------------------
# Core reconciliation
# ------------------------------------------------------------------

def _signed_order_qty(order: dict) -> float:
    """Return the pending order quantity as a signed number.

    BUY orders are positive, SELL orders are negative.
    """
    qty = order["remainingQuantity"]
    return qty if order["side"] == "BUY" else -qty


def reconcile(client: IBKRClient,
              df: pd.DataFrame) -> pd.DataFrame:
    """Compute net quantities and cancel stale orders.

    For each row in *df* that has a valid ``conid`` and ``limit_price``:

    1. Look up ``existing_qty`` from the user's IBKR positions.
    2. Look up ``pending_qty`` from open orders for the same conid.
    3. If a pending order exists at a **different** limit price, cancel it
       and do not count its quantity towards ``pending_qty``.
    4. ``target_qty = floor(|dollar_alloc| / limit_price)`` with the
       correct sign.
    5. ``net_qty = target_qty - existing_qty - pending_qty``.

    The result DataFrame has new columns:
    ``existing_qty``, ``pending_qty``, ``target_qty``, ``net_quantity``,
    and ``cancelled_orders`` (number of stale orders cancelled for that row).

    Parameters
    ----------
    client : IBKRClient
        Authenticated API client.
    df : pd.DataFrame
        The Project_Portfolio DataFrame (must have ``conid``,
        ``limit_price``, ``Dollar Allocation`` columns).

    Returns
    -------
    pd.DataFrame
        A copy of *df* with the additional columns described above.
    """
    account_id = get_account_id(client)

    print("Fetching current IBKR positions ...")
    positions = _fetch_positions(client, account_id)
    print(f"  Found {len(positions)} positions.\n")

    print("Fetching open orders ...")
    open_orders = _fetch_open_orders(client)
    print(f"  Found {len(open_orders)} active open orders.\n")

    # Group open orders by conid for efficient lookup.
    orders_by_conid: dict[int, list[dict]] = {}
    for o in open_orders:
        orders_by_conid.setdefault(o["conid"], []).append(o)

    # Price tolerance for comparing limit prices (absolute).
    PRICE_TOL = 0.02

    existing_qtys: list[float | None] = []
    pending_qtys: list[float | None] = []
    target_qtys: list[int | None] = []
    net_qtys: list[int | None] = []
    cancelled_counts: list[int] = []

    total = len(df)
    for idx, row in df.iterrows():
        conid_raw = row.get("conid")
        limit_price = row.get("limit_price")
        dollar_alloc = row.get("Dollar Allocation")

        # If essential data is missing, propagate None.
        if pd.isna(conid_raw) or pd.isna(limit_price) or pd.isna(dollar_alloc):
            existing_qtys.append(None)
            pending_qtys.append(None)
            target_qtys.append(None)
            net_qtys.append(None)
            cancelled_counts.append(0)
            continue

        conid = int(conid_raw)
        limit_price = float(limit_price)
        dollar_alloc = float(dollar_alloc)

        # FX rate: local currency units per 1 USD (1.0 for USD positions).
        # Non-USD positions without a valid rate are skipped.
        ccy = row.get("currency")
        fx_raw = row.get("fx_rate")
        is_usd = pd.isna(ccy) or str(ccy).upper() == "USD"
        if is_usd:
            fx = 1.0
        elif pd.notna(fx_raw) and float(fx_raw) > 0:
            fx = float(fx_raw)
        else:
            # Cannot reconcile without a valid exchange rate.
            existing_qtys.append(None)
            pending_qtys.append(None)
            target_qtys.append(None)
            net_qtys.append(None)
            cancelled_counts.append(0)
            continue

        # --- Target quantity (signed: positive=BUY, negative=SELL) ---
        # Dollar Allocation is in USD; limit_price is in local currency.
        if limit_price > 0:
            local_alloc = abs(dollar_alloc) * fx
            target = math.floor(local_alloc / limit_price)
            target = target if dollar_alloc >= 0 else -target
        else:
            target = 0

        # --- Existing position ---
        existing = positions.get(conid, 0)

        # --- Pending orders (cancel stale ones) ---
        pending = 0
        cancelled = 0
        conid_orders = orders_by_conid.get(conid, [])
        for order in conid_orders:
            order_price = order.get("price")
            if order_price is not None and abs(order_price - limit_price) > PRICE_TOL:
                # Stale order – different price. Cancel it.
                try:
                    client.cancel_order(account_id, order["orderId"])
                    name = row.get("Name", "")
                    print(f"  [{idx + 1}/{total}] Cancelled stale order "
                          f"{order['orderId']} for '{name}' "
                          f"(old price={order_price}, new price={limit_price})")
                    cancelled += 1
                    time.sleep(0.2)
                except Exception as exc:
                    print(f"  [!] Failed to cancel order {order['orderId']}: {exc}")
                    # Count it as pending since we couldn't cancel.
                    pending += _signed_order_qty(order)
            else:
                # Order at the correct price – count towards pending.
                pending += _signed_order_qty(order)

        net = target - int(existing) - int(pending)

        existing_qtys.append(existing)
        pending_qtys.append(pending)
        target_qtys.append(target)
        net_qtys.append(net)
        cancelled_counts.append(cancelled)

    df = df.copy()
    df["existing_qty"] = existing_qtys
    df["pending_qty"] = pending_qtys
    df["target_qty"] = target_qtys
    df["net_quantity"] = net_qtys
    df["cancelled_orders"] = cancelled_counts

    # Summary – count distinct stocks, not total shares.
    stocks_to_buy = sum(1 for q in net_qtys if q is not None and q > 0)
    stocks_to_sell = sum(1 for q in net_qtys if q is not None and q < 0)
    stocks_on_target = sum(1 for q in net_qtys if q is not None and q == 0)
    total_cancelled = sum(cancelled_counts)
    print(f"\nReconciliation complete:")
    print(f"  Stocks to BUY     : {stocks_to_buy}")
    print(f"  Stocks to SELL    : {stocks_to_sell}")
    print(f"  Already on target : {stocks_on_target}")
    print(f"  Stale orders cancelled : {total_cancelled}\n")

    return df

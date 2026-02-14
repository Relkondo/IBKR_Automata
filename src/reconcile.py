"""Reconcile the target portfolio against IBKR's current state via ib_async.

Compares the Project_Portfolio (desired state) to existing positions and
pending orders on IBKR, computing the *net* quantity that still needs to
be ordered for each conid.  Stale pending orders whose limit price no
longer matches are cancelled and their quantity is reclaimed.

The result is a DataFrame identical to the input but with extra columns:
``existing_qty``, ``pending_qty``, ``target_qty``, ``net_quantity``,
and ``cancelled_orders``.
"""

from __future__ import annotations

import pandas as pd
from ib_async import IB

from src.cancel import (
    CancelState, signed_order_qty,
    resolve_cancel_decision, execute_cancel,
)
from src.config import STALE_ORDER_TOL_PCT, STALE_ORDER_TOL_PCT_ILLIQUID
from src.exchange_hours import is_exchange_open
from src.extra_positions import compute_net_quantity, reconcile_extra_positions
from src.market_data import get_fx


# ==================================================================
# Data fetchers
# ==================================================================

def _fetch_positions(ib: IB,
                     ) -> tuple[dict[int, float], dict[int, dict]]:
    """Return positions and their raw metadata.

    Returns
    -------
    positions : dict[int, float]
        ``{conid: signed_position_qty}``.
    meta : dict[int, dict]
        ``{conid: {ticker, name, currency, exchange}}``.
    """
    raw = ib.positions()
    positions: dict[int, float] = {}
    meta: dict[int, dict] = {}
    for pos in raw:
        c = pos.contract
        cid = c.conId
        if cid:
            positions[cid] = float(pos.position)
            meta[cid] = {
                "ticker": c.symbol or str(cid),
                "name": c.symbol or str(cid),
                "currency": c.currency or "USD",
                "exchange": c.primaryExchange or c.exchange or "",
            }
    return positions, meta


def _fetch_open_orders(ib: IB) -> list[dict]:
    """Return open orders as normalised dicts.

    Each dict has: ``conid``, ``orderId``, ``side``, ``price``,
    ``remainingQuantity``, ``status``, ``trade`` (the Trade object).
    """
    trades = ib.openTrades()
    orders: list[dict] = []
    for trade in trades:
        c = trade.contract
        o = trade.order
        status = trade.orderStatus.status

        if status in ("Cancelled", "Filled", "Inactive"):
            continue

        side = o.action.upper() if o.action else ""
        if side in ("B", "BOT"):
            side = "BUY"
        elif side in ("S", "SLD"):
            side = "SELL"

        cid = c.conId
        if cid:
            orders.append({
                "conid": cid,
                "orderId": o.orderId,
                "side": side,
                "price": o.lmtPrice if hasattr(o, "lmtPrice") else None,
                "remainingQuantity": float(
                    trade.orderStatus.remaining
                    if trade.orderStatus.remaining
                    else o.totalQuantity
                ),
                "status": status,
                "trade": trade,
            })
    return orders


# ==================================================================
# Net-quantity computation
# ==================================================================

def compute_net_quantities(
    df: pd.DataFrame,
    positions: dict[int, float],
    orders_by_conid: dict[int, list[dict]],
) -> pd.DataFrame:
    """Add ``existing_qty``, ``pending_qty``, ``target_qty``, and
    ``net_quantity`` columns to *df*.

    For each row with a valid ``conid`` and ``Qty``, delegates to
    ``compute_net_quantity`` which also applies the minimum-trade
    filter.

    Returns a copy of the DataFrame with the new columns.
    """
    existing_qtys: list[float | None] = []
    pending_qtys: list[float | None] = []
    target_qtys: list[int | None] = []
    net_qtys: list[int | None] = []

    for _, row in df.iterrows():
        conid_raw = row.get("conid")
        qty_raw = row.get("Qty")

        if pd.isna(conid_raw):
            existing_qtys.append(None)
            pending_qtys.append(None)
            target_qtys.append(None)
            net_qtys.append(None)
            continue

        conid = int(conid_raw)
        existing = positions.get(conid, 0)

        pending = 0.0
        for order in orders_by_conid.get(conid, []):
            pending += signed_order_qty(order)

        existing_qtys.append(existing)
        pending_qtys.append(pending)

        if pd.isna(qty_raw):
            # No projected Qty (e.g. missing market data), but we
            # can still show current positions and pending orders.
            target_qtys.append(None)
            net_qtys.append(None)
            continue

        target = round(float(qty_raw))

        lp_raw = row.get("limit_price")
        lp = float(lp_raw) if pd.notna(lp_raw) else None
        fx_val = get_fx(row)

        net = compute_net_quantity(target, existing, pending, lp, fx_val)

        target_qtys.append(target)
        net_qtys.append(net)

    out = df.copy()
    out["existing_qty"] = existing_qtys
    out["pending_qty"] = pending_qtys
    out["target_qty"] = target_qtys
    out["net_quantity"] = net_qtys
    return out


# ==================================================================
# Stale-order cancellation
# ==================================================================

_ILLIQUID_MICS = {"XFRA", "OTCM"}


def _is_order_stale(
    order: dict, limit_price: float, tol_pct: float,
) -> bool:
    """Return True if the order's price deviates from *limit_price*
    by more than *tol_pct* (fractional).
    """
    order_price = order.get("price")
    if order_price is None or not limit_price:
        return False
    return abs(order_price - limit_price) / limit_price > tol_pct


def _cancel_stale_orders(
    ib: IB,
    df: pd.DataFrame,
    orders_by_conid: dict[int, list[dict]],
    all_exchanges: bool,
    state: CancelState,
) -> tuple[dict[int, list[dict]], list[int]]:
    """Cancel stale orders and return the remaining (non-cancelled) ones.

    An order is *stale* when its limit price deviates from the
    freshly computed limit price by more than the configured tolerance.

    Returns
    -------
    remaining_orders : dict[int, list[dict]]
        Orders grouped by conid, with cancelled ones removed.
    cancelled_counts : list[int]
        Per-row count of cancelled orders (aligned with *df*).
    """
    remaining: dict[int, list[dict]] = {
        cid: list(ords) for cid, ords in orders_by_conid.items()
    }
    cancelled_counts: list[int] = []
    total = len(df)

    for idx, row in df.iterrows():
        conid_raw = row.get("conid")
        limit_price_raw = row.get("limit_price")

        if pd.isna(conid_raw) or pd.isna(limit_price_raw):
            cancelled_counts.append(0)
            continue

        conid = int(conid_raw)
        limit_price = float(limit_price_raw)
        conid_orders = orders_by_conid.get(conid, [])

        if not conid_orders:
            cancelled_counts.append(0)
            continue

        mic = row.get("MIC Primary Exchange")
        mic_str = str(mic).strip().upper() if pd.notna(mic) else ""
        can_cancel = all_exchanges or (
            bool(mic_str) and is_exchange_open(mic_str)
        )
        tol_pct = (STALE_ORDER_TOL_PCT_ILLIQUID
                   if mic_str in _ILLIQUID_MICS
                   else STALE_ORDER_TOL_PCT)

        kept: list[dict] = []
        cancelled = 0
        name = row.get("Name", "")
        label = f"[{idx + 1}/{total}]"

        for order in conid_orders:
            if not _is_order_stale(order, limit_price, tol_pct):
                kept.append(order)
                continue

            # Order is stale — decide whether to cancel.
            order_price = order.get("price")
            header = (
                f"\n  {label} Stale order {order['orderId']} "
                f"for '{name}' (old price={order_price}, "
                f"new price={limit_price})\n"
                f"  Exchange: {mic_str or '?'}"
            )

            decision, is_auto = resolve_cancel_decision(
                mic_str, can_cancel, state, prompt_header=header)

            if decision == "skip":
                reason = ("exchange closed" if not can_cancel
                          else "auto-skip" if is_auto else "skipped")
                print(f"  {label} Stale order {order['orderId']} "
                      f"for '{name}' — {reason}")
                kept.append(order)
                continue

            # Cancel the stale order.
            trade_obj = order.get("trade")
            if trade_obj and execute_cancel(ib, trade_obj.order):
                auto_tag = " (auto)" if is_auto else ""
                print(f"  {label} Cancelled stale order "
                      f"{order['orderId']} for '{name}' "
                      f"(old={order_price}, new={limit_price})"
                      f"{auto_tag}")
                cancelled += 1
            else:
                print(f"  [!] Failed to cancel order "
                      f"{order['orderId']}")
                kept.append(order)

        remaining[conid] = kept
        cancelled_counts.append(cancelled)

    return remaining, cancelled_counts


# ==================================================================
# Public API
# ==================================================================

def reconcile(ib: IB,
              df: pd.DataFrame,
              all_exchanges: bool = True,
              dry_run: bool = False) -> pd.DataFrame:
    """Compute net quantities and optionally cancel stale orders.

    When *dry_run* is ``True``, no orders are cancelled — all pending
    orders are counted as-is and extra positions produce synthetic rows
    for read-only display.  Useful for comparisons.

    1. Cancel stale orders (skipped in dry-run).
    2. Compute net quantities using ``compute_net_quantities``.
    3. Handle extra IBKR positions not in the input file.
    """
    print("Fetching current IBKR positions ...")
    positions, position_meta = _fetch_positions(ib)
    print(f"  Found {len(positions)} positions.\n")

    print("Fetching open orders ...")
    open_orders = _fetch_open_orders(ib)
    print(f"  Found {len(open_orders)} active open orders.\n")

    orders_by_conid: dict[int, list[dict]] = {}
    for o in open_orders:
        orders_by_conid.setdefault(o["conid"], []).append(o)

    # Cancel consent state is shared between stale-order and
    # extra-position cancellation so user choices carry over.
    state = CancelState()

    if dry_run:
        # Read-only: skip cancellation, count all orders as pending.
        df = compute_net_quantities(df, positions, orders_by_conid)
        df["cancelled_orders"] = 0
        cancelled_counts: list[int] = [0] * len(df)
    else:
        # Phase 1: Cancel stale orders.
        remaining_orders, cancelled_counts = _cancel_stale_orders(
            ib, df, orders_by_conid, all_exchanges, state)

        # Phase 2: Compute net quantities with remaining orders.
        df = compute_net_quantities(df, positions, remaining_orders)
        df["cancelled_orders"] = cancelled_counts

    # ------------------------------------------------------------------
    # Extra positions not in the input file.
    # ------------------------------------------------------------------
    df_conids = set(int(c) for c in df["conid"].dropna().unique())
    extra_conids = [
        cid for cid, qty in positions.items()
        if cid not in df_conids and qty != 0
    ]

    extra_cancelled = 0
    if extra_conids:
        extra_rows, extra_cancelled = reconcile_extra_positions(
            ib=ib,
            extra_conids=extra_conids,
            positions=positions,
            position_meta=position_meta,
            orders_by_conid=orders_by_conid,
            all_exchanges=all_exchanges,
            cancel_state=state,
            dry_run=dry_run,
        )

        if extra_rows:
            extra_df = pd.DataFrame(extra_rows)
            for col in df.columns:
                if col not in extra_df.columns:
                    extra_df[col] = None
            df = pd.concat([df, extra_df[df.columns]], ignore_index=True)

    # Summary.
    all_net = df["net_quantity"].tolist()
    stocks_to_buy = sum(1 for q in all_net if pd.notna(q) and q > 0)
    stocks_to_sell = sum(1 for q in all_net if pd.notna(q) and q < 0)
    stocks_on_target = sum(1 for q in all_net if pd.notna(q) and q == 0)
    total_cancelled = sum(cancelled_counts) + extra_cancelled
    print(f"\nReconciliation complete:")
    print(f"  Stocks to BUY     : {stocks_to_buy}")
    print(f"  Stocks to SELL    : {stocks_to_sell}")
    print(f"  Already on target : {stocks_on_target}")
    print(f"  Stale orders cancelled : {total_cancelled}\n")

    return df

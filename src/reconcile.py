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
from src.config import (
    SELL_REBALANCE_RATIO_LIMIT,
    STALE_ORDER_TOL_PCT,
    STALE_ORDER_TOL_PCT_ILLIQUID,
)
from src.connection import ensure_connected
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
        if not cid:
            continue
        symbol = c.symbol or str(cid)
        positions[cid] = float(pos.position)
        meta[cid] = {
            "ticker": symbol,
            "name": symbol,
            "currency": c.currency or "USD",
            "exchange": c.primaryExchange or c.exchange or "",
        }
    return positions, meta


def _fetch_open_orders(ib: IB) -> list[dict]:
    """Return open orders as normalised dicts.

    Each dict has: ``conid``, ``orderId``, ``side``, ``price``,
    ``remainingQuantity``, ``status``, ``trade`` (the Trade object),
    and ``cancellable`` (bool).

    Orders with ``orderId == 0`` originate from another client (e.g.
    TWS GUI) and were fetched by ``reqAllOpenOrders``.  They are
    included so their quantity counts as pending, but marked
    ``cancellable=False`` because IBKR cannot cancel them via this
    API connection.
    """
    trades = ib.openTrades()
    orders: list[dict] = []
    unmanageable = 0
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
        if not cid:
            continue

        cancellable = bool(o.orderId)
        if not cancellable:
            unmanageable += 1

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
            "cancellable": cancellable,
        })

    if unmanageable:
        print(f"  ({unmanageable} order(s) from another client — "
              f"visible but not cancellable from this connection)")

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
            target_qtys.append(None)
            net_qtys.append(None)
            continue

        lp_raw = row.get("limit_price")
        lp = float(lp_raw) if pd.notna(lp_raw) else None
        fx_val = get_fx(row)

        target = round(float(qty_raw))
        mult = 100 if row.get("is_option") else 1
        net = compute_net_quantity(target, existing, pending, lp, fx_val,
                                   multiplier=mult)

        # Sell-rebalance ratio guard: when reducing an existing position,
        # verify the rounded order doesn't overshoot the projected change.
        if net < 0 and existing != 0 and lp and fx_val:
            actual_da_raw = row.get("Actual Dollar Allocation")
            dollar_alloc_raw = row.get("Dollar Allocation")
            if pd.notna(actual_da_raw) and pd.notna(dollar_alloc_raw):
                current_value = abs(existing) * lp * mult / fx_val
                project_vs_current = float(dollar_alloc_raw) - current_value
                actual_vs_current = float(actual_da_raw) - current_value
                if project_vs_current != 0:
                    ratio = actual_vs_current / project_vs_current
                    if ratio > SELL_REBALANCE_RATIO_LIMIT:
                        name = row.get("Name", "")
                        ticker = row.get("clean_ticker", "")
                        print(
                            f"  Zeroing SELL '{name}' ({ticker}) -- "
                            f"rebalance ratio {ratio:.2f} exceeds "
                            f"{SELL_REBALANCE_RATIO_LIMIT} "
                            f"(actual ${actual_vs_current:+,.0f} vs "
                            f"projected ${project_vs_current:+,.0f})."
                        )
                        net = 0

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


def _cancel_superfluous_orders(
    ib: IB,
    df: pd.DataFrame,
    orders_by_conid: dict[int, list[dict]],
    positions: dict[int, float],
    all_exchanges: bool,
    state: CancelState,
) -> tuple[dict[int, list[dict]], list[int]]:
    """Cancel orders whose direction or size conflicts with the target.

    An order is *superfluous* when keeping it would force
    ``compute_net_quantities`` to emit a counter-order.  Cases:

    * Position already on target, yet a BUY or SELL is pending.
    * A BUY is pending but the position needs to *decrease*
      (or vice versa).
    * Pending orders in the correct direction exceed the quantity
      needed, causing a counter-order for the excess.

    Returns
    -------
    remaining_orders : dict[int, list[dict]]
        Orders grouped by conid, with superfluous ones removed.
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
        qty_raw = row.get("Qty")

        if pd.isna(conid_raw) or pd.isna(qty_raw):
            cancelled_counts.append(0)
            continue

        conid = int(conid_raw)
        target = round(float(qty_raw))
        existing = round(positions.get(conid, 0))
        conid_orders = remaining.get(conid, [])

        if not conid_orders:
            cancelled_counts.append(0)
            continue

        raw_need = target - existing

        to_cancel: list[dict] = []
        to_keep: list[dict] = []

        if raw_need == 0:
            to_cancel = list(conid_orders)
        else:
            # Separate wrong-direction orders from right-direction ones.
            right_dir: list[dict] = []
            for order in conid_orders:
                sq = signed_order_qty(order)
                if (raw_need > 0 and sq < 0) or (raw_need < 0 and sq > 0):
                    to_cancel.append(order)
                else:
                    right_dir.append(order)

            # If right-direction orders overshoot the need, trim.
            right_total = sum(abs(signed_order_qty(o)) for o in right_dir)
            if right_total > abs(raw_need):
                accumulated = 0.0
                for order in right_dir:
                    sq_abs = abs(signed_order_qty(order))
                    if accumulated + sq_abs <= abs(raw_need):
                        accumulated += sq_abs
                        to_keep.append(order)
                    else:
                        to_cancel.append(order)
            else:
                to_keep.extend(right_dir)

        if not to_cancel:
            cancelled_counts.append(0)
            continue

        # Determine exchange and cancellability.
        mic = row.get("MIC Primary Exchange")
        mic_str = str(mic).strip().upper() if pd.notna(mic) else ""
        can_cancel = all_exchanges or (
            bool(mic_str) and is_exchange_open(mic_str)
        )

        name = row.get("Name", "")
        label = f"[{idx + 1}/{total}]"
        cancelled = 0

        for order in to_cancel:
            sq = signed_order_qty(order)
            direction = "BUY" if sq > 0 else "SELL"

            if not order.get("cancellable", True):
                print(f"  {label} Superfluous {direction} order "
                      f"{order['orderId']} for '{name}' "
                      f"— not cancellable (belongs to another client)")
                to_keep.append(order)
                continue

            if raw_need == 0:
                reason_detail = "position already on target"
            elif (raw_need > 0 and sq < 0) or (raw_need < 0 and sq > 0):
                reason_detail = "wrong direction"
            else:
                reason_detail = "exceeds quantity needed"

            header = (
                f"\n  {label} Superfluous {direction} order "
                f"{order['orderId']} for '{name}' "
                f"(qty={int(abs(sq))}, need={raw_need:+d}, "
                f"{reason_detail})\n"
                f"  Exchange: {mic_str or '?'}"
            )

            decision, is_auto = resolve_cancel_decision(
                mic_str, can_cancel, state, prompt_header=header)

            if decision == "skip":
                reason = ("exchange closed" if not can_cancel
                          else "auto-skip" if is_auto else "skipped")
                print(f"  {label} Superfluous order {order['orderId']} "
                      f"for '{name}' — {reason}")
                to_keep.append(order)
                continue

            trade_obj = order.get("trade")
            if trade_obj and execute_cancel(ib, trade_obj.order):
                auto_tag = " (auto)" if is_auto else ""
                print(f"  {label} Cancelled superfluous {direction} order "
                      f"{order['orderId']} for '{name}' "
                      f"(qty={int(abs(sq))}, need={raw_need:+d})"
                      f"{auto_tag}")
                cancelled += 1
            else:
                print(f"  [!] Failed to cancel order "
                      f"{order['orderId']}")
                to_keep.append(order)

        remaining[conid] = to_keep
        cancelled_counts.append(cancelled)

    return remaining, cancelled_counts


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

            if not order.get("cancellable", True):
                print(f"  {label} Stale order {order['orderId']} "
                      f"for '{name}' — not cancellable "
                      f"(belongs to another client)")
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
              dry_run: bool = False,
              auto_mode: bool = False) -> pd.DataFrame:
    """Compute net quantities and optionally cancel stale orders.

    When *dry_run* is ``True``, no orders are cancelled — all pending
    orders are counted as-is and extra positions produce synthetic rows
    for read-only display.  Useful for comparisons.

    When *auto_mode* is ``True``, all cancellation prompts are
    auto-confirmed (equivalent to user pressing Cancel All).

    1. Cancel stale orders (skipped in dry-run).
    2. Compute net quantities using ``compute_net_quantities``.
    3. Handle extra IBKR positions not in the input file.
    """
    ensure_connected(ib)

    print("Fetching current IBKR positions ...")
    positions, position_meta = _fetch_positions(ib)
    print(f"  Found {len(positions)} positions.\n")

    if not positions and auto_mode:
        raise RuntimeError(
            "IBKR returned 0 positions in auto mode.  This almost "
            "certainly means the Gateway data sync has not completed.  "
            "Aborting to prevent duplicate orders."
        )

    print("Fetching open orders ...")
    open_orders = _fetch_open_orders(ib)
    print(f"  Found {len(open_orders)} active open orders.\n")

    orders_by_conid: dict[int, list[dict]] = {}
    for o in open_orders:
        orders_by_conid.setdefault(o["conid"], []).append(o)

    # Cancel consent state is shared between stale-order and
    # extra-position cancellation so user choices carry over.
    state = CancelState(confirm_all=auto_mode)

    if dry_run:
        # Read-only: skip cancellation, count all orders as pending.
        df = compute_net_quantities(df, positions, orders_by_conid)
        df["cancelled_orders"] = 0
        cancelled_counts: list[int] = [0] * len(df)
    else:
        # Phase 1: Cancel stale orders (price drifted beyond tolerance).
        remaining_orders, stale_counts = _cancel_stale_orders(
            ib, df, orders_by_conid, all_exchanges, state)

        # Phase 2: Cancel superfluous orders (wrong direction or
        # overshooting the target) to avoid counter-orders.
        remaining_orders, superfluous_counts = _cancel_superfluous_orders(
            ib, df, remaining_orders, positions, all_exchanges, state)

        # Phase 3: Compute net quantities with remaining orders.
        df = compute_net_quantities(df, positions, remaining_orders)
        cancelled_counts = [
            s + p for s, p in zip(stale_counts, superfluous_counts)
        ]
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
    print(f"  Stale orders cancelled : {total_cancelled}")
    print(f"Note: all exchanges are included in the above reconciliation process.\n")

    return df

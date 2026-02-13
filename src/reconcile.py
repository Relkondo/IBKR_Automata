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
from ib_async import IB, Trade

from src.exchange_hours import is_exchange_open
from src.extra_positions import compute_net_quantity, reconcile_extra_positions
from src.orders import get_account_id
from src.contracts import exchange_to_mic


# ------------------------------------------------------------------
# Data fetchers
# ------------------------------------------------------------------

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


# ------------------------------------------------------------------
# Core reconciliation
# ------------------------------------------------------------------

def _signed_order_qty(order: dict) -> float:
    """BUY orders positive, SELL orders negative."""
    qty = order["remainingQuantity"]
    return qty if order["side"] == "BUY" else -qty


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

        if pd.isna(conid_raw) or pd.isna(qty_raw):
            existing_qtys.append(None)
            pending_qtys.append(None)
            target_qtys.append(None)
            net_qtys.append(None)
            continue

        conid = int(conid_raw)
        target = round(float(qty_raw))
        existing = positions.get(conid, 0)

        pending = 0.0
        for order in orders_by_conid.get(conid, []):
            pending += _signed_order_qty(order)

        # Resolve FX rate for the min-trade filter.
        lp_raw = row.get("limit_price")
        fx_raw = row.get("fx_rate")
        ccy = row.get("currency")
        is_usd = pd.isna(ccy) or str(ccy).upper() == "USD"
        lp = float(lp_raw) if pd.notna(lp_raw) else None
        fx_val = 1.0 if is_usd else (
            float(fx_raw) if pd.notna(fx_raw) and float(fx_raw) > 0
            else None
        )

        net = compute_net_quantity(target, existing, pending, lp, fx_val)

        existing_qtys.append(existing)
        pending_qtys.append(pending)
        target_qtys.append(target)
        net_qtys.append(net)

    out = df.copy()
    out["existing_qty"] = existing_qtys
    out["pending_qty"] = pending_qtys
    out["target_qty"] = target_qtys
    out["net_quantity"] = net_qtys
    return out


def _cancel_stale_orders(
    ib: IB,
    df: pd.DataFrame,
    orders_by_conid: dict[int, list[dict]],
    all_exchanges: bool,
) -> tuple[dict[int, list[dict]], list[int],
           bool, bool, set[str], set[str]]:
    """Cancel stale orders and return the remaining (non-cancelled) ones.

    Returns
    -------
    remaining_orders : dict[int, list[dict]]
        Orders grouped by conid, with cancelled ones removed.
    cancelled_counts : list[int]
        Per-row count of cancelled orders (aligned with *df*).
    cancel_confirm_all, cancel_skip_all : bool
    cancel_confirm_exchanges, cancel_skip_exchanges : set[str]
    """
    PRICE_TOL_PCT = 0.005
    PRICE_TOL_PCT_ILLIQUID = 0.05
    _ILLIQUID_MICS = {"XFRA", "OTCM"}

    # Start with a copy — orders not touched by any row stay as-is.
    remaining: dict[int, list[dict]] = {
        cid: list(ords) for cid, ords in orders_by_conid.items()
    }
    cancelled_counts: list[int] = []

    cancel_confirm_all: bool = False
    cancel_skip_all: bool = False
    cancel_confirm_exchanges: set[str] = set()
    cancel_skip_exchanges: set[str] = set()

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
        can_cancel = all_exchanges
        if not can_cancel and pd.notna(mic) and str(mic).strip():
            can_cancel = is_exchange_open(str(mic).strip())

        mic_str = str(mic).strip().upper() if pd.notna(mic) else ""
        tol_pct = (PRICE_TOL_PCT_ILLIQUID
                   if mic_str in _ILLIQUID_MICS
                   else PRICE_TOL_PCT)

        kept: list[dict] = []
        cancelled = 0

        for order in conid_orders:
            order_price = order.get("price")
            price_diff_pct = (abs(order_price - limit_price) / limit_price
                              if order_price is not None and limit_price
                              else None)

            if price_diff_pct is not None and price_diff_pct > tol_pct:
                if not can_cancel:
                    name = row.get("Name", "")
                    print(f"  [{idx + 1}/{total}] Stale order "
                          f"{order['orderId']} for '{name}' kept "
                          f"(exchange {mic_str} closed)")
                    kept.append(order)
                    continue

                name = row.get("Name", "")

                if cancel_skip_all or mic_str in cancel_skip_exchanges:
                    print(f"  [{idx + 1}/{total}] Stale order "
                          f"{order['orderId']} for '{name}' skipped "
                          f"(auto-skip)")
                    kept.append(order)
                    continue

                auto = (cancel_confirm_all
                        or mic_str in cancel_confirm_exchanges)

                if not auto:
                    print(f"\n  [{idx + 1}/{total}] Stale order "
                          f"{order['orderId']} for '{name}' "
                          f"(old price={order_price}, "
                          f"new price={limit_price})")
                    print(f"  Exchange: {mic_str or '?'}")
                    mic_label = mic_str or "?"
                    choice = input(
                        f"  [Y] Cancel  [A] Cancel All  "
                        f"[E] Cancel All {mic_label}  "
                        f"[S] Skip  [X] Skip All {mic_label}  "
                        f"[N] Skip All  > "
                    ).strip().upper()

                    if choice == "A":
                        cancel_confirm_all = True
                    elif choice == "E":
                        cancel_confirm_exchanges.add(mic_str)
                    elif choice == "X":
                        cancel_skip_exchanges.add(mic_str)
                        kept.append(order)
                        continue
                    elif choice == "N":
                        cancel_skip_all = True
                        kept.append(order)
                        continue
                    elif choice == "S":
                        kept.append(order)
                        continue
                    elif choice != "Y":
                        kept.append(order)
                        continue

                # Cancel the stale order via ib_async.
                try:
                    trade_obj = order.get("trade")
                    if trade_obj:
                        ib.cancelOrder(trade_obj.order)
                    ib.sleep(0.3)
                    auto_tag = " (auto)" if auto else ""
                    print(f"  [{idx + 1}/{total}] Cancelled stale order "
                          f"{order['orderId']} for '{name}' "
                          f"(old price={order_price}, "
                          f"new price={limit_price}){auto_tag}")
                    cancelled += 1
                except Exception as exc:
                    print(f"  [!] Failed to cancel order "
                          f"{order['orderId']}: {exc}")
                    kept.append(order)
            else:
                kept.append(order)

        remaining[conid] = kept
        cancelled_counts.append(cancelled)

    return (remaining, cancelled_counts,
            cancel_confirm_all, cancel_skip_all,
            cancel_confirm_exchanges, cancel_skip_exchanges)


def reconcile(ib: IB,
              df: pd.DataFrame,
              all_exchanges: bool = True,
              dry_run: bool = False) -> pd.DataFrame:
    """Compute net quantities and optionally cancel stale orders.

    When *dry_run* is ``True``, no orders are cancelled and extra
    positions are not processed — useful for read-only comparisons.

    1. Cancel stale orders (skipped in dry-run).
    2. Compute net quantities using ``compute_net_quantities``.
    3. Handle extra positions not in the input file (skipped in dry-run).
    """
    account_id = get_account_id(ib)

    print("Fetching current IBKR positions ...")
    positions, position_meta = _fetch_positions(ib)
    print(f"  Found {len(positions)} positions.\n")

    print("Fetching open orders ...")
    open_orders = _fetch_open_orders(ib)
    print(f"  Found {len(open_orders)} active open orders.\n")

    orders_by_conid: dict[int, list[dict]] = {}
    for o in open_orders:
        orders_by_conid.setdefault(o["conid"], []).append(o)

    if dry_run:
        # Read-only: skip cancellation, count all orders as pending.
        df = compute_net_quantities(df, positions, orders_by_conid)
        df["cancelled_orders"] = 0
        cancelled_counts: list[int] = [0] * len(df)
        cancel_confirm_all = False
        cancel_skip_all = False
        cancel_confirm_exchanges: set[str] = set()
        cancel_skip_exchanges: set[str] = set()
    else:
        # Phase 1: Cancel stale orders.
        (remaining_orders, cancelled_counts,
         cancel_confirm_all, cancel_skip_all,
         cancel_confirm_exchanges, cancel_skip_exchanges,
         ) = _cancel_stale_orders(ib, df, orders_by_conid, all_exchanges)

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
        (extra_rows, extra_cancelled,
         cancel_confirm_all, cancel_skip_all,
         cancel_confirm_exchanges, cancel_skip_exchanges,
         ) = reconcile_extra_positions(
            ib=ib,
            extra_conids=extra_conids,
            positions=positions,
            position_meta=position_meta,
            orders_by_conid=orders_by_conid,
            all_exchanges=all_exchanges,
            cancel_confirm_all=cancel_confirm_all,
            cancel_skip_all=cancel_skip_all,
            cancel_confirm_exchanges=cancel_confirm_exchanges,
            cancel_skip_exchanges=cancel_skip_exchanges,
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

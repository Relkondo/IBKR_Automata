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

import math
import time

import pandas as pd
from ib_async import IB, Trade

from src.exchange_hours import is_exchange_open
from src.extra_positions import reconcile_extra_positions
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


def reconcile(ib: IB,
              df: pd.DataFrame,
              all_exchanges: bool = True) -> pd.DataFrame:
    """Compute net quantities and cancel stale orders.

    For each row with a valid ``conid`` and ``limit_price``:
      1. existing_qty from IBKR positions.
      2. pending_qty from open orders for the same conid.
      3. Stale orders (different price) are cancelled.
      4. target_qty = floor(|dollar_alloc * fx| / limit_price) with sign.
      5. net_qty = target - existing - pending.
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

    # Price tolerances.
    PRICE_TOL_PCT = 0.005
    PRICE_TOL_PCT_ILLIQUID = 0.05
    _ILLIQUID_MICS = {"XFRA", "OTCM"}

    existing_qtys: list[float | None] = []
    pending_qtys: list[float | None] = []
    target_qtys: list[int | None] = []
    net_qtys: list[int | None] = []
    cancelled_counts: list[int] = []

    # Consent tracking.
    cancel_confirm_all: bool = False
    cancel_skip_all: bool = False
    cancel_confirm_exchanges: set[str] = set()
    cancel_skip_exchanges: set[str] = set()

    total = len(df)
    for idx, row in df.iterrows():
        conid_raw = row.get("conid")
        limit_price = row.get("limit_price")
        dollar_alloc = row.get("Dollar Allocation")

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

        ccy = row.get("currency")
        fx_raw = row.get("fx_rate")
        is_usd = pd.isna(ccy) or str(ccy).upper() == "USD"
        if is_usd:
            fx = 1.0
        elif pd.notna(fx_raw) and float(fx_raw) > 0:
            fx = float(fx_raw)
        else:
            existing_qtys.append(None)
            pending_qtys.append(None)
            target_qtys.append(None)
            net_qtys.append(None)
            cancelled_counts.append(0)
            continue

        # Target quantity.
        if limit_price > 0:
            local_alloc = abs(dollar_alloc) * fx
            target = math.floor(local_alloc / limit_price)
            target = target if dollar_alloc >= 0 else -target
        else:
            target = 0

        existing = positions.get(conid, 0)

        # Pending orders (cancel stale ones).
        pending = 0
        cancelled = 0
        conid_orders = orders_by_conid.get(conid, [])

        mic = row.get("MIC Primary Exchange")
        can_cancel = all_exchanges
        if not can_cancel and pd.notna(mic) and str(mic).strip():
            can_cancel = is_exchange_open(str(mic).strip())

        mic_str = str(mic).strip().upper() if pd.notna(mic) else ""
        tol_pct = (PRICE_TOL_PCT_ILLIQUID
                   if mic_str in _ILLIQUID_MICS
                   else PRICE_TOL_PCT)

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
                    pending += _signed_order_qty(order)
                    continue

                name = row.get("Name", "")

                if cancel_skip_all or mic_str in cancel_skip_exchanges:
                    print(f"  [{idx + 1}/{total}] Stale order "
                          f"{order['orderId']} for '{name}' skipped "
                          f"(auto-skip)")
                    pending += _signed_order_qty(order)
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
                        pending += _signed_order_qty(order)
                        continue
                    elif choice == "N":
                        cancel_skip_all = True
                        pending += _signed_order_qty(order)
                        continue
                    elif choice == "S":
                        pending += _signed_order_qty(order)
                        continue
                    elif choice != "Y":
                        pending += _signed_order_qty(order)
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
                    pending += _signed_order_qty(order)
            else:
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

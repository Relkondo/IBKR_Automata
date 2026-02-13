"""Handle IBKR positions that are not present in the input file.

When reconciling, any position held on IBKR whose conid does not appear
in the Project Portfolio is treated as if the target quantity is 0.
This module fetches the necessary market data, cancels stale orders, and
builds synthetic DataFrame rows so the order loop can liquidate (or
cover) those positions.
"""

from __future__ import annotations

import math

import pandas as pd
from ib_async import IB, Contract

from src.contracts import exchange_to_mic
from src.exchange_hours import is_exchange_open
from src.market_data import (
    _snapshot_batch, _resolve_fx_rate, SNAPSHOT_BATCH_SIZE, FILL_PATIENCE,
)


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------

def reconcile_extra_positions(
    ib: IB,
    extra_conids: list[int],
    positions: dict[int, float],
    position_meta: dict[int, dict],
    orders_by_conid: dict[int, list[dict]],
    all_exchanges: bool,
    cancel_confirm_all: bool,
    cancel_skip_all: bool,
    cancel_confirm_exchanges: set[str],
    cancel_skip_exchanges: set[str],
) -> tuple[list[dict], int, bool, bool, set[str], set[str]]:
    """Process IBKR positions not in the input file.

    Returns
    -------
    extra_rows : list[dict]
        Synthetic row dicts ready to be appended to the DataFrame.
    extra_cancelled : int
        Number of stale orders cancelled for extra positions.
    cancel_confirm_all, cancel_skip_all : bool
    cancel_confirm_exchanges, cancel_skip_exchanges : set[str]
    """
    print(f"\nFound {len(extra_conids)} IBKR position(s) not in the "
          f"input file. Fetching market data to prepare sell orders ...")

    # --- Qualify contracts and extract currencies -----------------------
    extra_contracts = [Contract(conId=cid) for cid in extra_conids]
    qualified = ib.qualifyContracts(*extra_contracts)
    cid_to_contract = {c.conId: c for c in qualified if c.conId}

    extra_currencies: dict[int, str] = {}
    for cid in extra_conids:
        qc = cid_to_contract.get(cid)
        if qc and qc.currency:
            extra_currencies[cid] = qc.currency.upper()
        else:
            extra_currencies[cid] = "USD"

    # Fetch exchange rates for unique non-USD currencies.
    unique_ccys = {c for c in extra_currencies.values() if c != "USD"}
    fx_rates: dict[str, float] = {"USD": 1.0}
    for ccy in sorted(unique_ccys):
        resolved = _resolve_fx_rate(ib, ccy)
        if resolved is not None:
            fx_rates[ccy] = resolved

    extra_fx: dict[int, float] = {}
    for cid in extra_conids:
        ccy = extra_currencies.get(cid, "USD")
        if ccy in fx_rates:
            extra_fx[cid] = fx_rates[ccy]

    # --- Fetch market data for extra conids ----------------------------
    contracts_list = [cid_to_contract[cid] for cid in extra_conids
                      if cid in cid_to_contract]

    snapshot: dict[int, dict] = {}
    total_batches = math.ceil(len(contracts_list) / SNAPSHOT_BATCH_SIZE) if contracts_list else 0
    for i in range(0, len(contracts_list), SNAPSHOT_BATCH_SIZE):
        batch = contracts_list[i : i + SNAPSHOT_BATCH_SIZE]
        batch_num = i // SNAPSHOT_BATCH_SIZE + 1
        print(f"  Extra batch {batch_num}/{total_batches} "
              f"({len(batch)} contracts) …")
        batch_result = _snapshot_batch(ib, batch)
        snapshot.update(batch_result)

    # --- Cancel open orders on extra conids -----------------------------
    # For extra positions the target is 0, so every order is stale.
    # Track which orders are *kept* (not cancelled) so their signed
    # quantity can be subtracted from net_quantity later.
    extra_cancelled = 0
    pending_by_conid: dict[int, float] = {}

    def _signed_qty(order: dict) -> float:
        qty = order["remainingQuantity"]
        return qty if order["side"] == "BUY" else -qty

    for cid in extra_conids:
        conid_orders = orders_by_conid.get(cid, [])
        pm = position_meta.get(cid, {})
        pos_name = pm.get("name", str(cid))
        raw_exchange = pm.get("exchange", "")
        mic_code = exchange_to_mic(raw_exchange) if raw_exchange else ""

        can_cancel = all_exchanges
        if not can_cancel and mic_code:
            can_cancel = is_exchange_open(mic_code)

        for order in conid_orders:
            if not can_cancel:
                print(f"  Extra-position stale order "
                      f"{order['orderId']} for '{pos_name}' kept "
                      f"(exchange {mic_code} closed)")
                pending_by_conid[cid] = (
                    pending_by_conid.get(cid, 0) + _signed_qty(order))
                continue

            if cancel_skip_all or mic_code in cancel_skip_exchanges:
                print(f"  Extra-position stale order "
                      f"{order['orderId']} for '{pos_name}' skipped "
                      f"(auto-skip)")
                pending_by_conid[cid] = (
                    pending_by_conid.get(cid, 0) + _signed_qty(order))
                continue

            auto = (cancel_confirm_all
                    or mic_code in cancel_confirm_exchanges)
            if not auto:
                mic_label = mic_code or "?"
                print(f"\n  Extra-position stale order "
                      f"{order['orderId']} for '{pos_name}' "
                      f"(price={order.get('price')})")
                print(f"  Exchange: {mic_label}")
                choice = input(
                    f"  [Y] Cancel  [A] Cancel All  "
                    f"[E] Cancel All {mic_label}  "
                    f"[S] Skip  [X] Skip All {mic_label}  "
                    f"[N] Skip All  > "
                ).strip().upper()

                if choice == "A":
                    cancel_confirm_all = True
                elif choice == "E":
                    cancel_confirm_exchanges.add(mic_code)
                elif choice == "X":
                    cancel_skip_exchanges.add(mic_code)
                    pending_by_conid[cid] = (
                        pending_by_conid.get(cid, 0) + _signed_qty(order))
                    continue
                elif choice == "N":
                    cancel_skip_all = True
                    pending_by_conid[cid] = (
                        pending_by_conid.get(cid, 0) + _signed_qty(order))
                    continue
                elif choice == "S":
                    pending_by_conid[cid] = (
                        pending_by_conid.get(cid, 0) + _signed_qty(order))
                    continue
                elif choice != "Y":
                    print(f"    Invalid choice, skipping.")
                    pending_by_conid[cid] = (
                        pending_by_conid.get(cid, 0) + _signed_qty(order))
                    continue

            try:
                trade_obj = order.get("trade")
                if trade_obj:
                    ib.cancelOrder(trade_obj.order)
                ib.sleep(0.3)
                auto_tag = " (auto)" if auto else ""
                print(f"  Cancelled extra-position order "
                      f"{order['orderId']} for '{pos_name}'{auto_tag}")
                extra_cancelled += 1
            except Exception as exc:
                print(f"  [!] Failed to cancel order "
                      f"{order['orderId']}: {exc}")
                # Cancellation failed — order stays, count as pending.
                pending_by_conid[cid] = (
                    pending_by_conid.get(cid, 0) + _signed_qty(order))

    # --- Build synthetic rows ------------------------------------------
    extra_rows: list[dict] = []
    for cid in extra_conids:
        existing = positions.get(cid, 0)
        pending = pending_by_conid.get(cid, 0)
        if existing == 0:
            continue

        pm = position_meta.get(cid, {})
        raw_exchange = pm.get("exchange", "")
        mic_code = exchange_to_mic(raw_exchange) if raw_exchange else ""
        ccy = extra_currencies.get(cid, "USD")
        fx = extra_fx.get(cid)

        snap = snapshot.get(cid, {})
        bid = snap.get("bid")
        ask_val = snap.get("ask")
        last_val = snap.get("last")
        close_val = snap.get("close")
        high_val = snap.get("high")
        low_val = snap.get("low")

        row_dict: dict = {
            "conid": float(cid),
            "Name": pm.get("name", str(cid)),
            "clean_ticker": pm.get("ticker", str(cid)),
            "IBKR Name": pm.get("name", str(cid)),
            "IBKR Ticker": pm.get("ticker", str(cid)),
            "MIC Primary Exchange": mic_code,
            "currency": ccy,
            "fx_rate": fx,
            "Dollar Allocation": 0.0,
            "bid": bid,
            "ask": ask_val,
            "last": last_val,
            "close": close_val,
            "day_high": high_val,
            "day_low": low_val,
            "is_option": False,
            "existing_qty": existing,
            "pending_qty": pending,
            "target_qty": 0,
            "cancelled_orders": 0,
        }

        # Compute limit price using FILL_PATIENCE spread formula.
        limit_price = None
        is_sell = existing > 0

        if bid is not None and ask_val is not None:
            spread = ask_val - bid
            if spread >= 0:
                if is_sell:
                    limit_price = round(
                        bid + spread * FILL_PATIENCE / 100, 2)
                else:
                    limit_price = round(
                        ask_val - spread * FILL_PATIENCE / 100, 2)
        elif last_val is not None and last_val > 0:
            limit_price = round(last_val, 2)
        elif close_val is not None and close_val > 0:
            limit_price = round(close_val, 2)
        elif bid is not None and bid > 0:
            limit_price = round(bid, 2)
        elif ask_val is not None and ask_val > 0:
            limit_price = round(ask_val, 2)

        row_dict["limit_price"] = limit_price
        row_dict["net_quantity"] = 0 - round(existing) - round(pending)

        extra_rows.append(row_dict)

    if extra_rows:
        print(f"  Prepared {len(extra_rows)} extra-position row(s) "
              f"to sell/cover.")
    if extra_cancelled:
        print(f"  Extra-position orders cancelled: {extra_cancelled}")

    return (extra_rows, extra_cancelled,
            cancel_confirm_all, cancel_skip_all,
            cancel_confirm_exchanges, cancel_skip_exchanges)

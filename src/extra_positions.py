"""Handle IBKR positions that are not present in the input file.

When reconciling, any position held on IBKR whose conid does not appear
in the Project Portfolio is treated as if the target quantity is 0.
This module fetches the necessary market data, cancels stale orders, and
builds synthetic DataFrame rows so the order loop can liquidate (or
cover) those positions.
"""

from __future__ import annotations

import pandas as pd
from ib_async import IB, Contract, Forex

from src.contracts import exchange_to_mic
from src.exchange_hours import is_exchange_open
from src.market_data import _poll_snapshot, _safe_float, BATCH_SIZE, SPEED_VS_GREED


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

    # --- Fetch currency info for extra conids --------------------------
    extra_currencies: dict[int, str] = {}
    extra_fx: dict[int, float] = {}
    for cid in extra_conids:
        c = Contract(conId=cid)
        try:
            details = ib.reqContractDetails(c)
            if details:
                ccy = details[0].contract.currency or "USD"
                extra_currencies[cid] = ccy.upper()
        except Exception as exc:
            print(f"  [!] Could not fetch details for conid {cid}: {exc}")
        ib.sleep(0.02)

    # Fetch exchange rates for unique non-USD currencies.
    unique_ccys = set(c for c in extra_currencies.values() if c != "USD")
    fx_rates: dict[str, float] = {}
    for ccy in unique_ccys:
        try:
            pair = f"{ccy}USD"
            fx_contract = Forex(pair)
            ib.qualifyContracts(fx_contract)
            t = ib.reqMktData(fx_contract, snapshot=True)
            ib.sleep(2)
            rate = _safe_float(t.marketPrice())
            ib.cancelMktData(fx_contract)
            if rate and rate > 0:
                fx_rates[ccy] = round(1.0 / rate, 6)
            else:
                print(f"  [!] Could not get FX rate for USD->{ccy}")
        except Exception as exc:
            print(f"  [!] FX rate fetch failed for {ccy}: {exc}")

    for cid in extra_conids:
        ccy = extra_currencies.get(cid, "USD")
        if ccy == "USD":
            extra_fx[cid] = 1.0
        elif ccy in fx_rates:
            extra_fx[cid] = fx_rates[ccy]

    # --- Fetch market data for extra conids ----------------------------
    # Build Contract objects.
    extra_contracts: list[Contract] = []
    for cid in extra_conids:
        c = Contract(conId=cid)
        try:
            details = ib.reqContractDetails(c)
            if details:
                extra_contracts.append(details[0].contract)
            else:
                extra_contracts.append(c)
        except Exception:
            extra_contracts.append(c)
        ib.sleep(0.02)

    snapshot: dict[int, dict] = {}
    for i in range(0, len(extra_contracts), BATCH_SIZE):
        batch = extra_contracts[i : i + BATCH_SIZE]
        batch_result = _poll_snapshot(ib, batch)
        snapshot.update(batch_result)

    # --- Cancel stale open orders on extra conids ----------------------
    extra_cancelled = 0
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
                continue

            if cancel_skip_all or mic_code in cancel_skip_exchanges:
                print(f"  Extra-position stale order "
                      f"{order['orderId']} for '{pos_name}' skipped "
                      f"(auto-skip)")
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
                    continue
                elif choice == "N":
                    cancel_skip_all = True
                    continue
                elif choice == "S":
                    continue
                elif choice != "Y":
                    print(f"    Invalid choice, skipping.")
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

    # --- Build synthetic rows ------------------------------------------
    extra_rows: list[dict] = []
    for cid in extra_conids:
        existing = positions.get(cid, 0)
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
        mark_val = snap.get("mark")
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
            "mark": mark_val,
            "day_high": high_val,
            "day_low": low_val,
            "is_option": False,
            "existing_qty": existing,
            "pending_qty": 0,
            "target_qty": 0,
            "cancelled_orders": 0,
        }

        # Compute limit price.
        limit_price = None
        if existing > 0:
            # SELL formula.
            if mark_val is not None and ask_val is not None:
                limit_price = round(
                    ask_val - (mark_val - ask_val) / SPEED_VS_GREED, 2)
            elif mark_val is not None and high_val is not None:
                limit_price = round(
                    mark_val - (mark_val - high_val) / SPEED_VS_GREED, 2)
            elif mark_val is not None:
                limit_price = round(mark_val, 2)
            elif ask_val is not None:
                limit_price = round(ask_val, 2)
        else:
            # BUY (cover) formula.
            if mark_val is not None and bid is not None:
                limit_price = round(
                    bid - (mark_val - bid) / SPEED_VS_GREED, 2)
            elif mark_val is not None and low_val is not None:
                limit_price = round(
                    mark_val - (mark_val - low_val) / SPEED_VS_GREED, 2)
            elif mark_val is not None:
                limit_price = round(mark_val, 2)
            elif bid is not None:
                limit_price = round(bid, 2)

        row_dict["limit_price"] = limit_price
        row_dict["net_quantity"] = 0 - int(existing)

        extra_rows.append(row_dict)

    if extra_rows:
        print(f"  Prepared {len(extra_rows)} extra-position row(s) "
              f"to sell/cover.")
    if extra_cancelled:
        print(f"  Extra-position orders cancelled: {extra_cancelled}")

    return (extra_rows, extra_cancelled,
            cancel_confirm_all, cancel_skip_all,
            cancel_confirm_exchanges, cancel_skip_exchanges)

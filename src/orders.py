"""Interactive order placement with user confirmation via ib_async.

For each row in the portfolio table the user is prompted to confirm,
modify, skip, or quit.  Placed orders are tracked and a summary is
printed at the end.
"""

import math

import pandas as pd
from ib_async import IB, Contract, LimitOrder, Trade

from src.contracts import exchange_to_mic
from src.exchange_hours import is_exchange_open
from src.market_data import snap_to_tick


# ------------------------------------------------------------------
# Account
# ------------------------------------------------------------------

def get_account_id(ib: IB) -> str:
    """Retrieve the first managed account ID."""
    accounts = ib.managedAccounts()
    if not accounts:
        raise RuntimeError("No managed accounts returned by TWS.")
    account_id = accounts[0]
    print(f"Using account: {account_id}\n")
    return account_id


# ------------------------------------------------------------------
# Cancel all orders
# ------------------------------------------------------------------

def cancel_all_orders(ib: IB,
                      all_exchanges: bool = True) -> None:
    """Fetch every open order and attempt to cancel each one.

    When *all_exchanges* is ``False``, only cancel orders whose
    exchange is currently open.
    """
    account_id = get_account_id(ib)

    print("Fetching open orders ...")
    open_trades = ib.openTrades()

    if not open_trades:
        print("No active orders to cancel.\n")
        return

    print(f"Found {len(open_trades)} active order(s). Cancelling ...\n")

    cancelled = 0
    failed = 0
    skipped = 0

    # Per-exchange consent tracking.
    cancel_confirm_all: bool = False
    cancel_skip_all: bool = False
    cancel_confirm_exchanges: set[str] = set()
    cancel_skip_exchanges: set[str] = set()

    for trade in open_trades:
        c = trade.contract
        o = trade.order
        status = trade.orderStatus.status

        # Skip already done orders.
        if status in ("Cancelled", "Filled", "Inactive"):
            continue

        oid = o.orderId
        ticker = c.symbol or ""
        side = o.action or ""
        remaining = o.totalQuantity
        price = o.lmtPrice if hasattr(o, "lmtPrice") else ""

        # Determine MIC.
        raw_exchange = c.primaryExchange or c.exchange or ""
        mic = exchange_to_mic(raw_exchange) if raw_exchange else ""

        # Exchange filtering.
        if not all_exchanges and mic:
            if not is_exchange_open(mic):
                print(f"  Skipped order {oid}  {side} {remaining} {ticker} "
                      f"@ {price}  (exchange {mic} closed)")
                skipped += 1
                continue

        # Auto-skip check.
        if cancel_skip_all or mic in cancel_skip_exchanges:
            print(f"  Skipped order {oid}  {side} {remaining} {ticker} "
                  f"@ {price}  (auto-skip)")
            skipped += 1
            continue

        # Auto-confirm check.
        auto = cancel_confirm_all or mic in cancel_confirm_exchanges

        if not auto:
            mic_label = mic or "?"
            print(f"\n  Order {oid}  {side} {remaining} {ticker} @ {price}"
                  f"  (exchange: {mic_label})")
            choice = input(
                f"  [Y] Cancel  [A] Cancel All  "
                f"[E] Cancel All {mic_label}  "
                f"[S] Skip  [X] Skip All {mic_label}  "
                f"[N] Skip All  > "
            ).strip().upper()

            if choice == "A":
                cancel_confirm_all = True
            elif choice == "E":
                cancel_confirm_exchanges.add(mic)
            elif choice == "X":
                cancel_skip_exchanges.add(mic)
                skipped += 1
                continue
            elif choice == "N":
                cancel_skip_all = True
                skipped += 1
                continue
            elif choice == "S":
                skipped += 1
                continue
            elif choice != "Y":
                skipped += 1
                continue

        # Proceed with cancellation.
        try:
            ib.cancelOrder(o)
            ib.sleep(0.3)
            auto_tag = " (auto)" if auto else ""
            print(f"  Cancelled order {oid}  {side} {remaining} "
                  f"{ticker} @ {price}{auto_tag}")
            cancelled += 1
        except Exception as exc:
            print(f"  [!] Failed to cancel order {oid} ({ticker}): {exc}")
            failed += 1

    parts = [f"{cancelled} cancelled", f"{failed} failed"]
    if skipped:
        parts.append(f"{skipped} skipped")
    print(f"\nDone: {', '.join(parts)}.\n")


# ------------------------------------------------------------------
# Order helpers
# ------------------------------------------------------------------

def _format_currency(value: float) -> str:
    return f"${value:,.2f}"


def _place_order(ib: IB, contract: Contract, order: LimitOrder,
                 ) -> Trade:
    """Place an order and wait briefly for acknowledgement."""
    trade = ib.placeOrder(contract, order)
    # Wait for TWS to acknowledge (status transitions).
    ib.sleep(1)
    return trade


# ------------------------------------------------------------------
# Interactive loop
# ------------------------------------------------------------------

def run_order_loop(ib: IB, df: pd.DataFrame) -> list[dict]:
    """Iterate over the portfolio and interactively place orders.

    Returns a list of summary records for successfully placed orders.
    """
    account_id = get_account_id(ib)
    placed_orders: list[dict] = []
    confirm_all = False
    auto_confirm_exchanges: set[str] = set()
    auto_skip_exchanges: set[str] = set()
    total = len(df)

    for idx, row in df.iterrows():
        conid = row.get("conid")
        name = row.get("Name", "")
        ticker = row.get("clean_ticker", "")
        dollar_alloc = row.get("Dollar Allocation")
        limit_price = row.get("limit_price")

        # FX rate.
        ccy = row.get("currency")
        fx_raw = row.get("fx_rate")
        is_usd = pd.isna(ccy) or str(ccy).upper() == "USD"
        if is_usd:
            fx = 1.0
        elif pd.notna(fx_raw) and float(fx_raw) > 0:
            fx = float(fx_raw)
        else:
            fx = None

        # Skip rows that cannot be ordered.
        skip_reasons: list[str] = []
        if pd.isna(conid):
            skip_reasons.append("no conid")
        if pd.isna(limit_price):
            skip_reasons.append("no limit price")
        if pd.isna(dollar_alloc):
            skip_reasons.append("no dollar allocation")
        if fx is None:
            skip_reasons.append(f"no exchange rate for {ccy}")

        if skip_reasons:
            print(f"[{idx + 1}/{total}] Skipping '{name}' ({ticker}) -- "
                  f"{', '.join(skip_reasons)}.")
            continue

        conid = int(conid)
        limit_price = float(limit_price)
        dollar_alloc = float(dollar_alloc)

        # Determine side and quantity.
        net_qty_raw = row.get("net_quantity")
        if pd.notna(net_qty_raw):
            net_qty = int(net_qty_raw)
            if net_qty == 0:
                print(f"[{idx + 1}/{total}] '{name}' ({ticker}) -- "
                      "already on target, nothing to order.")
                continue
            side = "SELL" if net_qty < 0 else "BUY"
            quantity_initial = abs(net_qty)
        else:
            side = "SELL" if dollar_alloc < 0 else "BUY"
            local_alloc = abs(dollar_alloc) * fx
            quantity_initial = (math.floor(local_alloc / limit_price)
                                if limit_price > 0 else 0)

        quantity = quantity_initial

        ccy_label = str(ccy) if pd.notna(ccy) else "USD"
        is_foreign = ccy_label != "USD"
        mic_raw = row.get("MIC Primary Exchange")
        mic_str = str(mic_raw).strip().upper() if pd.notna(mic_raw) else ""

        # Auto-skip by exchange.
        if mic_str in auto_skip_exchanges:
            print(f"\n[{idx + 1}/{total}] {name} ({ticker}) -- "
                  f"auto-skipped ({mic_str})")
            continue

        # Build the ib_async Contract for this order.
        order_contract = Contract(conId=conid)
        try:
            details = ib.reqContractDetails(order_contract)
            if details:
                order_contract = details[0].contract
        except Exception:
            pass

        # --- Prompt loop ---
        while True:
            local_amount = round(limit_price * quantity, 2)
            details_str = (
                f"\n[{idx + 1}/{total}] {name} ({ticker})\n"
                f"  Side              : {side}\n"
                f"  Exchange          : {mic_str or '?'}\n"
                f"  Currency          : {ccy_label}\n"
                f"  Limit Price       : {limit_price:,.2f} {ccy_label}\n"
                f"  Quantity          : {quantity}\n"
                f"  Amount            : {local_amount:,.2f} {ccy_label}\n"
            )
            if is_foreign:
                usd_amount = (round(local_amount / fx, 2)
                              if fx > 0 else None)
                if usd_amount is not None:
                    details_str += (f"  Amount (USD)      : "
                                    f"{_format_currency(usd_amount)}\n")
            if pd.notna(net_qty_raw):
                existing = row.get("existing_qty", 0)
                pending = row.get("pending_qty", 0)
                target = row.get("target_qty", 0)
                details_str += (
                    f"  --- reconciliation ---\n"
                    f"  Target qty        : {target}\n"
                    f"  Existing position : {int(existing)}\n"
                    f"  Pending orders    : {int(pending)}\n"
                    f"  Net to order      : {int(net_qty_raw)}\n"
                )
            else:
                details_str += (
                    f"  Dollar Allocation : "
                    f"{_format_currency(dollar_alloc)}\n"
                )
            print(details_str)

            if confirm_all or mic_str in auto_confirm_exchanges:
                choice = "Y"
                print("  (auto-confirmed)")
            else:
                mic_label = mic_str or "?"
                choice = input(
                    f"  [Y] Confirm  [A] Confirm All  "
                    f"[E] Confirm All {mic_label}  [M] Modify\n"
                    f"  [S] Skip  [X] Skip All {mic_label}  "
                    f"[Q] Quit  > "
                ).strip().upper()

            if choice in ("Y", "A", "E"):
                if choice == "A":
                    confirm_all = True
                elif choice == "E":
                    auto_confirm_exchanges.add(mic_str)

                order = LimitOrder(side, quantity, limit_price)
                order.tif = "DAY"

                try:
                    trade = _place_order(ib, order_contract, order)
                    status = trade.orderStatus.status

                    # Check for Error 110 (tick-size rejection).
                    has_tick_error = any(
                        getattr(e, "errorCode", 0) == 110
                        for e in trade.log
                    )
                    if has_tick_error:
                        # Fetch market rules and snap the price.
                        mrids = row.get("market_rule_ids")
                        if pd.isna(mrids) or not str(mrids).strip():
                            # Fetch from contract details on the fly.
                            try:
                                cds = ib.reqContractDetails(
                                    order_contract)
                                if cds:
                                    mrids = cds[0].marketRuleIds or ""
                            except Exception:
                                mrids = ""
                        mrids = str(mrids).strip() if mrids else ""
                        if mrids:
                            is_buy = side == "BUY"
                            adjusted = round(
                                snap_to_tick(limit_price, ib, mrids,
                                             is_buy=is_buy),
                                10,
                            )
                            if adjusted != limit_price:
                                print(
                                    f"    Price "
                                    f"{_format_currency(limit_price)} "
                                    f"rejected (tick-size). Retrying at "
                                    f"{_format_currency(adjusted)} …")
                                limit_price = adjusted
                                continue
                        print(f"    [!] Tick-size error but could not "
                              f"determine valid tick — skipping.")
                        break

                    order_id = trade.order.orderId
                    print(f"    Order placed -- order_id: {order_id} "
                          f"(status: {status})")
                    if status == "Cancelled":
                        print(f"    [!] Order {order_id} was immediately "
                              f"cancelled — not counting as placed.")
                    else:
                        placed_orders.append({
                            "ticker": ticker,
                            "name": name,
                            "conid": conid,
                            "side": side,
                            "quantity": quantity,
                            "limit_price": limit_price,
                            "order_id": order_id,
                        })
                except Exception as exc:
                    print(f"    [!] Order failed: {exc}")
                    if confirm_all:
                        print("    Skipping (auto-confirm mode).")
                    else:
                        retry = input(
                            "    [R] Retry  [S] Skip  > "
                        ).strip().upper()
                        if retry == "R":
                            continue
                break

            elif choice == "M":
                new_qty = input(
                    f"  New quantity [{quantity}]: ").strip()
                if new_qty:
                    try:
                        quantity = int(new_qty.replace(",", ""))
                    except ValueError:
                        print("    Invalid number, keeping original.")

                new_price = input(
                    f"  New limit price "
                    f"[{_format_currency(limit_price)}]: "
                ).strip()
                if new_price:
                    try:
                        limit_price = float(
                            new_price.replace(",", "").replace("$", ""))
                    except ValueError:
                        print("    Invalid number, keeping original.")

                new_side = input(
                    f"  New side [{side}]: ").strip().upper()
                if new_side in ("BUY", "SELL"):
                    side = new_side
                elif new_side:
                    print("    Invalid side, keeping original.")
                continue

            elif choice == "X":
                auto_skip_exchanges.add(mic_str)
                print(f"    Skipped (+ auto-skip all {mic_str}).")
                break

            elif choice == "S":
                print("    Skipped.")
                break

            elif choice == "Q":
                print("    Quitting order loop.")
                return placed_orders

            else:
                print("    Invalid choice. "
                      "Please enter Y, A, E, M, S, X, or Q.")

    return placed_orders


def print_order_summary(orders: list[dict]) -> None:
    """Pretty-print a summary table of placed orders."""
    if not orders:
        print("\nNo orders were placed.")
        return

    print("\n" + "=" * 78)
    print("  ORDER SUMMARY")
    print("=" * 78)
    header = (
        f"{'Ticker':<12} {'Name':<28} {'Side':<6} {'Qty':>8} "
        f"{'Limit':>10} {'Order ID':>12}"
    )
    print(header)
    print("-" * 78)
    for o in orders:
        print(
            f"{o['ticker']:<12} {o['name'][:26]:<28} {o['side']:<6} "
            f"{o['quantity']:>8} "
            f"{_format_currency(o['limit_price']):>10} "
            f"{str(o['order_id']):>12}"
        )
    print("=" * 78)
    print(f"  Total orders placed: {len(orders)}\n")

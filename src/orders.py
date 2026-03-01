"""Order placement, cancellation, and interactive confirmation via ib_async.

Provides the ``cancel_all_orders`` bulk-cancellation command and the
interactive ``run_order_loop`` for placing orders.  For each row in the
portfolio table the user is prompted to confirm, modify, skip, or quit.
Placed orders are tracked and a summary is printed at the end.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd
from ib_async import IB, Contract, Order, Trade

from src.cancel import (
    CancelState, resolve_cancel_decision, execute_cancel,
)
from src.config import MAXIMUM_AMOUNT_AUTOMATIC_ORDER, PRICE_OFFSET
from src.connection import ensure_connected
from src.contracts import exchange_to_mic
from src.exchange_hours import is_exchange_open
from src.market_data import get_fx, snap_to_tick


@dataclass
class _AutoState:
    """Mutable state for auto-confirm / auto-skip across the order loop."""

    confirm_all: bool = False
    confirm_exchanges: set[str] = field(default_factory=set)
    skip_exchanges: set[str] = field(default_factory=set)


@dataclass
class _OrderParams:
    """All data needed to display, confirm, and submit one order.

    ``limit_price``, ``quantity``, and ``side`` are mutable — they can
    be changed by the Modify prompt or by tick-size snapping.
    """

    row: pd.Series
    order_contract: Contract
    idx_label: str
    name: str
    ticker: str
    conid: int
    limit_price: float
    quantity: int
    side: str
    multiplier: int
    ccy_label: str
    is_foreign: bool
    fx: float
    dollar_alloc: float
    net_qty_raw: float | None  # pandas scalar: set by reconciliation, NaN/None otherwise
    mic_str: str
    is_option: bool


# ==================================================================
# Cancel all orders
# ==================================================================

def cancel_all_orders(ib: IB,
                      all_exchanges: bool = False,
                      auto_mode: bool = False) -> None:
    """Fetch every open order and attempt to cancel each one.

    When *all_exchanges* is ``False`` (the default), only cancel
    orders whose exchange is currently open.
    When *auto_mode* is ``True``, all cancellations are automatic.
    """
    print("Fetching open orders ...")
    open_trades = ib.openTrades()

    if not open_trades:
        print("No active orders to cancel.\n")
        return

    print(f"Found {len(open_trades)} active order(s). Cancelling ...\n")

    cancelled = 0
    failed = 0
    skipped = 0
    state = CancelState(confirm_all=auto_mode)

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

        # Determine MIC and exchange-open status.
        raw_exchange = c.primaryExchange or c.exchange or ""
        mic = exchange_to_mic(raw_exchange) if raw_exchange else ""
        can_cancel = all_exchanges or not mic or is_exchange_open(mic)

        order_desc = f"{side} {remaining} {ticker} @ {price}"
        header = (
            f"\n  Order {oid}  {order_desc}\n"
            f"  Exchange: {mic or '?'}"
        )

        decision, is_auto = resolve_cancel_decision(
            mic, can_cancel, state, prompt_header=header)

        if decision == "skip":
            reason = ("exchange closed" if not can_cancel
                      else "auto-skip" if is_auto else "skipped")
            print(f"  Skipped order {oid}  {order_desc}  ({reason})")
            skipped += 1
            continue

        # Proceed with cancellation.
        if execute_cancel(ib, o):
            auto_tag = " (auto)" if is_auto else ""
            print(f"  Cancelled order {oid}  {order_desc}{auto_tag}")
            cancelled += 1
        else:
            print(f"  [!] Failed to cancel order {oid} ({ticker})")
            failed += 1

    parts = [f"{cancelled} cancelled", f"{failed} failed"]
    if skipped:
        parts.append(f"{skipped} skipped")
    print(f"\nDone: {', '.join(parts)}.\n")


# ==================================================================
# Order helpers
# ==================================================================

def _format_currency(value: float, ccy: str = "USD") -> str:
    """Format a monetary value.  Uses ``$`` for USD, appends the
    currency code for everything else."""
    if ccy == "USD":
        return f"${value:,.2f}"
    return f"{value:,.2f} {ccy}"


def _place_order(ib: IB, contract: Contract, order: Order,
                 ) -> tuple[Trade, str]:
    """Place an order and wait briefly for acknowledgement.

    Returns ``(trade, error_reason)`` where *error_reason* is the
    error message captured from ib_async's errorEvent (empty string
    if no error).  This is needed because ib_async may skip adding
    the error to ``trade.log`` when the order is already "done"
    (e.g. status Inactive arrives before the error callback).
    """
    captured: list[str] = []

    def _on_error(reqId, errorCode, errorString, contract):
        captured.append(f"Error {errorCode}: {errorString}")

    ib.errorEvent += _on_error
    trade = ib.placeOrder(contract, order)
    ib.sleep(1)
    ib.errorEvent -= _on_error

    return trade, captured[-1] if captured else ""


# ==================================================================
# Interactive loop
# ==================================================================

def _compute_usd_amount(limit_price: float, quantity: int,
                        multiplier: int, fx: float) -> float:
    """Return the USD notional value of an order."""
    local_amount = limit_price * quantity * multiplier
    return round(local_amount / fx, 2) if fx > 0 else local_amount


def _format_order_details(p: _OrderParams) -> str:
    """Build the human-readable order summary shown before each prompt."""
    local_amount = round(p.limit_price * p.quantity * p.multiplier, 2)
    lines = [
        f"\n{p.idx_label} {p.name} ({p.ticker})"
        + (" (OPTION)" if p.is_option else ""),
        f"  Order Type        : REL (Relative)",
        f"  Side              : {p.side}",
        f"  Exchange          : {p.mic_str or '?'}",
        f"  Currency          : {p.ccy_label}",
        f"  Limit Price (cap) : {p.limit_price:,.2f} {p.ccy_label}",
        f"  Price Offset      : {PRICE_OFFSET}%",
        f"  Quantity          : {p.quantity}",
        f"  Amount (at limit) : {local_amount:,.2f} {p.ccy_label}",
    ]

    if p.is_foreign and p.fx > 0:
        usd_amount = round(local_amount / p.fx, 2)
        lines.append(f"  Amount (USD)      : {_format_currency(usd_amount)}")

    if pd.notna(p.net_qty_raw):
        existing = p.row.get("existing_qty", 0)
        pending = p.row.get("pending_qty", 0)
        target = p.row.get("target_qty", 0)
        lines += [
            f"  --- reconciliation ---",
            f"  Target qty        : {target}",
            f"  Existing position : {int(existing)}",
            f"  Pending orders    : {int(pending)}",
            f"  Net to order      : {int(p.net_qty_raw)}",
        ]
    else:
        lines.append(
            f"  Dollar Allocation : {_format_currency(p.dollar_alloc)}")

    return "\n".join(lines) + "\n"


def _prompt_modify(p: _OrderParams) -> None:
    """Prompt the user to modify quantity, limit price, and/or side.

    Mutates *p* in-place.
    """
    new_qty = input(f"  New quantity [{p.quantity}]: ").strip()
    if new_qty:
        try:
            p.quantity = int(new_qty.replace(",", ""))
        except ValueError:
            print("    Invalid number, keeping original.")

    new_price = input(
        f"  New limit price [{_format_currency(p.limit_price, p.ccy_label)}]: "
    ).strip()
    if new_price:
        try:
            p.limit_price = float(
                new_price.replace(",", "").replace("$", ""))
        except ValueError:
            print("    Invalid number, keeping original.")

    new_side = input(f"  New side [{p.side}]: ").strip().upper()
    if new_side in ("BUY", "SELL"):
        p.side = new_side
    elif new_side:
        print("    Invalid side, keeping original.")


def _handle_tick_error(
    ib: IB, p: _OrderParams,
) -> float | None:
    """Try to snap the limit price to a valid tick after Error 110.

    Returns the adjusted price if a valid snap is found (and differs
    from the current price), or ``None`` if the error cannot be fixed.
    """
    mrids = p.row.get("market_rule_ids")
    if pd.isna(mrids) or not str(mrids).strip():
        try:
            cds = ib.reqContractDetails(p.order_contract)
            if cds:
                mrids = cds[0].marketRuleIds or ""
        except Exception:
            mrids = ""
    mrids = str(mrids).strip() if mrids else ""

    if not mrids:
        return None

    is_buy = p.side == "BUY"
    adjusted = round(
        snap_to_tick(p.limit_price, ib, mrids, is_buy=is_buy), 10,
    )
    if adjusted != p.limit_price:
        return adjusted
    return None


def _order_summary(p: _OrderParams, **extra) -> dict:
    """Build a compact summary dict for an order."""
    d = {
        "ticker": p.ticker,
        "name": p.name,
        "conid": p.conid,
        "side": p.side,
        "quantity": p.quantity,
        "limit_price": p.limit_price,
        "exchange": p.mic_str or "?",
        "usd_amount": _compute_usd_amount(
            p.limit_price, p.quantity, p.multiplier, p.fx),
    }
    d.update(extra)
    return d


def _place_single_order(
    ib: IB,
    p: _OrderParams,
    placed_orders: list[dict],
    state: _AutoState,
    *,
    allow_auto: bool = True,
    deferred_orders: list[_OrderParams] | None = None,
    rejected_orders: list[dict] | None = None,
    large_orders: list[dict] | None = None,
    auto_mode: bool = False,
) -> str:
    """Run the prompt-loop for one order.

    Returns a control signal:
      ``"next"``  -- move to the next row
      ``"quit"``  -- abort the whole loop

    Side-effects: mutates *placed_orders* and *state*.
    """
    while True:
        print(_format_order_details(p))

        # --- Resolve the user's choice (auto-confirm or prompt) -------
        is_auto = allow_auto and (
            state.confirm_all or p.mic_str in state.confirm_exchanges
        )
        if is_auto:
            usd_val = _compute_usd_amount(
                p.limit_price, p.quantity, p.multiplier, p.fx)
            if usd_val > MAXIMUM_AMOUNT_AUTOMATIC_ORDER:
                if auto_mode:
                    # -auto: reject large orders outright.
                    print(
                        f"  (REJECTED — USD amount "
                        f"{_format_currency(usd_val)} exceeds "
                        f"{_format_currency(MAXIMUM_AMOUNT_AUTOMATIC_ORDER)}"
                        f" auto limit)")
                    if rejected_orders is not None:
                        rejected_orders.append(_order_summary(
                            p, reason="Amount exceeds auto limit"))
                    if large_orders is not None:
                        large_orders.append(
                            _order_summary(p, status="rejected"))
                    return "next"
                elif deferred_orders is not None:
                    # Interactive: defer for manual approval later.
                    print(
                        f"  (deferred — USD amount "
                        f"{_format_currency(usd_val)} exceeds "
                        f"{_format_currency(MAXIMUM_AMOUNT_AUTOMATIC_ORDER)}"
                        f" auto-confirm threshold)")
                    deferred_orders.append(p)
                    if large_orders is not None:
                        large_orders.append(
                            _order_summary(p, status="deferred"))
                    return "next"
            choice = "Y"
            print("  (auto-confirmed)")
        else:
            mic_label = p.mic_str or "?"
            choice = input(
                f"  [Y] Confirm  [A] Confirm All  "
                f"[E] Confirm All {mic_label}  [M] Modify\n"
                f"  [S] Skip  [X] Skip All {mic_label}  "
                f"[Q] Quit  > "
            ).strip().upper()

        # --- Act on the choice ----------------------------------------
        if choice in ("Y", "A", "E"):
            if choice == "A":
                state.confirm_all = True
            elif choice == "E":
                state.confirm_exchanges.add(p.mic_str)

            order = Order(
                orderType='REL',
                action=p.side,
                totalQuantity=p.quantity,
                lmtPrice=p.limit_price,
                percentOffset=PRICE_OFFSET / 100,
                tif='DAY',
            )

            try:
                trade, error_reason = _place_order(
                    ib, p.order_contract, order)
                status = trade.orderStatus.status

                # Check for Error 110 (tick-size rejection).
                has_tick_error = any(
                    getattr(e, "errorCode", 0) == 110
                    for e in trade.log
                )
                if has_tick_error:
                    adjusted = _handle_tick_error(ib, p)
                    if adjusted is not None:
                        print(
                            f"    Price "
                            f"{_format_currency(p.limit_price, p.ccy_label)} "
                            f"rejected (tick-size). Retrying at "
                            f"{_format_currency(adjusted, p.ccy_label)} …")
                        p.limit_price = adjusted
                        continue
                    print(f"    [!] Tick-size error but could not "
                          f"determine valid tick — skipping.")
                    break

                order_id = trade.order.orderId
                print(f"    Order placed -- order_id: {order_id} "
                      f"(status: {status})")
                usd_amt = _compute_usd_amount(
                    p.limit_price, p.quantity, p.multiplier, p.fx)
                _FAILED_STATUSES = {
                    "Cancelled", "Inactive", "ApiCancelled",
                }
                if status in _FAILED_STATUSES:
                    print(f"    [!] Order {order_id} was {status.lower()}"
                          f" — not counting as placed.")
                    if rejected_orders is not None:
                        reason = error_reason
                        if not reason:
                            for entry in trade.log:
                                msg = getattr(entry, "message", "")
                                if msg:
                                    reason = msg
                        rejected_orders.append(
                            _order_summary(p, reason=reason))
                else:
                    placed_orders.append({
                        "ticker": p.ticker,
                        "name": p.name,
                        "conid": p.conid,
                        "side": p.side,
                        "quantity": p.quantity,
                        "limit_price": p.limit_price,
                        "order_id": order_id,
                        "usd_amount": usd_amt,
                    })
                    if (large_orders is not None
                            and usd_amt > MAXIMUM_AMOUNT_AUTOMATIC_ORDER):
                        large_orders.append(
                            _order_summary(p, status="placed",
                                           order_id=order_id))
            except Exception as exc:
                print(f"    [!] Order failed: {exc}")
                if rejected_orders is not None:
                    rejected_orders.append(
                        _order_summary(p, reason=str(exc)))
                if is_auto:
                    print("    Skipping (auto-confirm mode).")
                else:
                    retry = input(
                        "    [R] Retry  [S] Skip  > "
                    ).strip().upper()
                    if retry == "R":
                        continue
            break

        elif choice == "M":
            _prompt_modify(p)
            continue

        elif choice == "X":
            state.skip_exchanges.add(p.mic_str)
            print(f"    Skipped (+ auto-skip all {p.mic_str}).")
            break

        elif choice == "S":
            print("    Skipped.")
            break

        elif choice == "Q":
            print("    Quitting order loop.")
            return "quit"

        else:
            print("    Invalid choice. "
                  "Please enter Y, A, E, M, S, X, or Q.")

    return "next"


# ==================================================================
# Order-params preparation
# ==================================================================

def _prepare_order_params(
    ib: IB,
    row: pd.Series,
    idx: int,
    total: int,
    state: _AutoState,
) -> _OrderParams | None:
    """Build an ``_OrderParams`` from a DataFrame row.

    Returns ``None`` (and prints the reason) when the row should be
    skipped — missing data, zero quantity, auto-skip exchange, etc.
    """
    label = f"[{idx + 1}/{total}]"
    name = row.get("Name", "")
    ticker = row.get("clean_ticker", "")
    conid_raw = row.get("conid")
    dollar_alloc_raw = row.get("Dollar Allocation")
    limit_price_raw = row.get("limit_price")
    ccy = row.get("currency")
    fx = get_fx(row)

    # --- Collect skip reasons -----------------------------------------
    skip_reasons: list[str] = []
    if pd.isna(conid_raw):
        skip_reasons.append("no conid")
    if pd.isna(limit_price_raw):
        skip_reasons.append("no limit price")
    if pd.isna(dollar_alloc_raw):
        skip_reasons.append("no dollar allocation")
    if fx is None:
        skip_reasons.append(f"no exchange rate for {ccy}")

    if skip_reasons:
        print(f"{label} Skipping '{name}' ({ticker}) -- "
              f"{', '.join(skip_reasons)}.")
        return None

    conid = int(conid_raw)
    limit_price = float(limit_price_raw)
    dollar_alloc = float(dollar_alloc_raw)

    if limit_price <= 0:
        print(f"{label} Skipping '{name}' ({ticker}) -- "
              f"invalid limit price {limit_price}.")
        return None

    # --- Determine side and quantity ----------------------------------
    is_option = bool(row.get("is_option"))
    multiplier = 100 if is_option else 1
    net_qty_raw = row.get("net_quantity")

    if pd.notna(net_qty_raw):
        net_qty = int(net_qty_raw)
        if net_qty == 0:
            print(f"{label} '{name}' ({ticker}) -- "
                  "already on target, nothing to order.")
            return None
        side = "SELL" if net_qty < 0 else "BUY"
        quantity = abs(net_qty)
    else:
        side = "SELL" if dollar_alloc < 0 else "BUY"
        local_alloc = abs(dollar_alloc) * fx
        quantity = round(local_alloc / (limit_price * multiplier))
        if quantity <= 0:
            print(f"{label} Skipping '{name}' ({ticker}) -- "
                  "computed quantity is 0.")
            return None

    # --- Exchange / currency labels -----------------------------------
    ccy_label = str(ccy) if pd.notna(ccy) else "USD"
    is_foreign = ccy_label != "USD"
    mic_raw = row.get("MIC Primary Exchange")
    mic_str = str(mic_raw).strip().upper() if pd.notna(mic_raw) else ""

    if mic_str in state.skip_exchanges:
        print(f"\n{label} {name} ({ticker}) -- "
              f"auto-skipped ({mic_str})")
        return None

    # --- Qualify the contract -----------------------------------------
    order_contract = Contract(conId=conid)
    try:
        details = ib.reqContractDetails(order_contract)
        if details:
            order_contract = details[0].contract
    except Exception:
        pass

    return _OrderParams(
        row=row,
        order_contract=order_contract,
        idx_label=label,
        name=name,
        ticker=ticker,
        conid=conid,
        limit_price=limit_price,
        quantity=quantity,
        side=side,
        multiplier=multiplier,
        ccy_label=ccy_label,
        is_foreign=is_foreign,
        fx=fx,
        dollar_alloc=dollar_alloc,
        net_qty_raw=net_qty_raw,
        mic_str=mic_str,
        is_option=is_option,
    )


# ==================================================================
# Main loop
# ==================================================================

def run_order_loop(ib: IB, df: pd.DataFrame,
                   auto_mode: bool = False) -> list[dict]:
    """Iterate over the portfolio and place orders.

    When *auto_mode* is ``True``, all orders are auto-confirmed,
    large orders are rejected outright, and a Telegram summary is
    sent at the end.  In interactive mode no Telegram is sent.

    Returns a list of summary records for successfully placed orders.
    """
    ensure_connected(ib)

    placed_orders: list[dict] = []
    deferred_orders: list[_OrderParams] = []
    rejected_orders: list[dict] = []
    large_orders: list[dict] = []
    state = _AutoState(confirm_all=auto_mode)
    total = len(df)

    for idx, row in df.iterrows():
        params = _prepare_order_params(ib, row, idx, total, state)
        if params is None:
            continue
        signal = _place_single_order(
            ib, params, placed_orders, state,
            deferred_orders=deferred_orders,
            rejected_orders=rejected_orders,
            large_orders=large_orders,
            auto_mode=auto_mode,
        )
        if signal == "quit":
            break

    # Process deferred large orders (interactive only — in auto mode
    # large orders are rejected immediately so nothing is deferred).
    if deferred_orders:
        n = len(deferred_orders)
        print(f"\n{'=' * 78}")
        print(f"  {n} LARGE ORDER(S) DEFERRED — MANUAL APPROVAL REQUIRED")
        print(f"  (USD amount > "
              f"{_format_currency(MAXIMUM_AMOUNT_AUTOMATIC_ORDER)})")
        print(f"{'=' * 78}")

        for params in deferred_orders:
            signal = _place_single_order(
                ib, params, placed_orders, _AutoState(),
                allow_auto=False,
                rejected_orders=rejected_orders,
                large_orders=large_orders,
            )
            if signal == "quit":
                break

    # Telegram notification (auto mode only).
    if auto_mode:
        from src.telegram import notify_flagged_orders
        notify_flagged_orders(rejected_orders, large_orders)

    return placed_orders


def print_order_summary(orders: list[dict]) -> None:
    """Pretty-print a summary table of placed orders."""
    if not orders:
        print("\nNo orders were placed.")
        return

    width = 92
    print("\n" + "=" * width)
    print("  ORDER SUMMARY")
    print("=" * width)
    header = (
        f"{'Ticker':<12} {'Name':<26} {'Side':<6} {'Qty':>8} "
        f"{'Limit':>10} {'Amount':>14} {'Order ID':>12}"
    )
    print(header)
    print("-" * width)
    total_usd = 0.0
    for o in orders:
        usd = o.get("usd_amount", 0.0)
        total_usd += usd
        print(
            f"{o['ticker']:<12} {o['name'][:24]:<26} {o['side']:<6} "
            f"{o['quantity']:>8} "
            f"{_format_currency(o['limit_price']):>10} "
            f"{_format_currency(usd):>14} "
            f"{str(o['order_id']):>12}"
        )
    print("=" * width)
    print(f"  Total orders placed: {len(orders)}    "
          f"Total amount: {_format_currency(total_usd)}\n")

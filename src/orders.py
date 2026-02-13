"""Interactive order placement with user confirmation via ib_async.

For each row in the portfolio table the user is prompted to confirm,
modify, skip, or quit.  Placed orders are tracked and a summary is
printed at the end.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

import pandas as pd
from ib_async import IB, Contract, LimitOrder, Trade

from src.config import MAXIMUM_AMOUNT_AUTOMATIC_ORDER
from src.connection import suppress_errors
from src.contracts import exchange_to_mic
from src.exchange_hours import is_exchange_open
from src.market_data import snap_to_tick


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
    net_qty_raw: object  # float | NaN | None (pandas scalar)
    mic_str: str


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
            with suppress_errors(202):
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

def _compute_usd_amount(limit_price: float, quantity: int,
                        multiplier: int, fx: float) -> float:
    """Return the USD notional value of an order."""
    local_amount = limit_price * quantity * multiplier
    return round(local_amount / fx, 2) if fx > 0 else local_amount


def _format_order_details(p: _OrderParams) -> str:
    """Build the human-readable order summary shown before each prompt."""
    local_amount = round(p.limit_price * p.quantity * p.multiplier, 2)
    lines = [
        f"\n{p.idx_label} {p.name} ({p.ticker})",
        f"  Side              : {p.side}",
        f"  Exchange          : {p.mic_str or '?'}",
        f"  Currency          : {p.ccy_label}",
        f"  Limit Price       : {p.limit_price:,.2f} {p.ccy_label}",
        f"  Quantity          : {p.quantity}",
        f"  Amount            : {local_amount:,.2f} {p.ccy_label}",
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
        f"  New limit price [{_format_currency(p.limit_price)}]: "
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


def _place_single_order(
    ib: IB,
    p: _OrderParams,
    placed_orders: list[dict],
    state: _AutoState,
    *,
    allow_auto: bool = True,
    deferred_orders: list[_OrderParams] | None = None,
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
            # Guardrail: defer large orders for explicit approval.
            usd_val = _compute_usd_amount(
                p.limit_price, p.quantity, p.multiplier, p.fx)
            if (deferred_orders is not None
                    and usd_val > MAXIMUM_AMOUNT_AUTOMATIC_ORDER):
                print(
                    f"  (deferred — USD amount "
                    f"{_format_currency(usd_val)} exceeds "
                    f"{_format_currency(MAXIMUM_AMOUNT_AUTOMATIC_ORDER)}"
                    f" auto-confirm threshold)")
                deferred_orders.append(p)
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

            order = LimitOrder(p.side, p.quantity, p.limit_price)
            order.tif = "DAY"

            try:
                trade = _place_order(ib, p.order_contract, order)
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
                            f"{_format_currency(p.limit_price)} "
                            f"rejected (tick-size). Retrying at "
                            f"{_format_currency(adjusted)} …")
                        p.limit_price = adjusted
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
                        "ticker": p.ticker,
                        "name": p.name,
                        "conid": p.conid,
                        "side": p.side,
                        "quantity": p.quantity,
                        "limit_price": p.limit_price,
                        "order_id": order_id,
                    })
            except Exception as exc:
                print(f"    [!] Order failed: {exc}")
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


# ------------------------------------------------------------------
# Order-params preparation
# ------------------------------------------------------------------

def _resolve_fx(row: pd.Series) -> float | None:
    """Extract the FX rate from a row. Returns 1.0 for USD, None if missing."""
    ccy = row.get("currency")
    fx_raw = row.get("fx_rate")
    if pd.isna(ccy) or str(ccy).upper() == "USD":
        return 1.0
    if pd.notna(fx_raw) and float(fx_raw) > 0:
        return float(fx_raw)
    return None


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
    fx = _resolve_fx(row)

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
        quantity = math.floor(local_alloc / (limit_price * multiplier))
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
    )


# ------------------------------------------------------------------
# Main loop
# ------------------------------------------------------------------

def run_order_loop(ib: IB, df: pd.DataFrame) -> list[dict]:
    """Iterate over the portfolio and interactively place orders.

    Returns a list of summary records for successfully placed orders.
    """
    get_account_id(ib)
    placed_orders: list[dict] = []
    deferred_orders: list[_OrderParams] = []
    state = _AutoState()
    total = len(df)

    for idx, row in df.iterrows():
        params = _prepare_order_params(ib, row, idx, total, state)
        if params is None:
            continue
        signal = _place_single_order(
            ib, params, placed_orders, state,
            deferred_orders=deferred_orders,
        )
        if signal == "quit":
            return placed_orders

    # Process deferred large orders (explicit manual approval required).
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
            )
            if signal == "quit":
                break

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

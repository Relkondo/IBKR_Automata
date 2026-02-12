"""Interactive order placement with user confirmation.

For each row in the portfolio table the user is prompted to confirm,
modify, skip, or quit.  Placed orders are tracked and a summary is
printed at the end.
"""

import math
import re

import pandas as pd

from src.api_client import IBKRClient

# Maximum number of reply-confirmation round-trips before giving up.
MAX_REPLY_ROUNDS = 5

# Regex to detect IBKR's minimum-tick-size rejection and extract the tick size.
_MIN_TICK_RE = re.compile(
    r"minimum price variation of ([\d.]+)"
)


# ------------------------------------------------------------------
# Account
# ------------------------------------------------------------------

def get_account_id(client: IBKRClient) -> str:
    """Retrieve the first brokerage account ID."""
    data = client.get_accounts()

    # The response shape varies; handle both common layouts.
    if isinstance(data, dict):
        accounts = data.get("accounts", [])
    elif isinstance(data, list):
        accounts = data
    else:
        accounts = []

    if not accounts:
        raise RuntimeError(
            "No brokerage accounts returned by /iserver/accounts. "
            "Is the session authenticated?"
        )

    account_id = accounts[0] if isinstance(accounts[0], str) else accounts[0].get("accountId", accounts[0].get("id"))
    print(f"Using account: {account_id}\n")
    return account_id


# ------------------------------------------------------------------
# Cancel all orders
# ------------------------------------------------------------------

def cancel_all_orders(client: IBKRClient,
                      all_exchanges: bool = True) -> None:
    """Fetch every open order and attempt to cancel each one.

    Parameters
    ----------
    all_exchanges : bool
        When ``False``, only cancel orders whose exchange is currently
        open.  Orders on closed exchanges are skipped with a message.
    """
    import time

    from src.contracts import exchange_to_mic
    from src.exchange_hours import is_exchange_open

    account_id = get_account_id(client)

    print("Fetching open orders ...")
    orders = client.get_live_orders()

    # Filter to active orders only.
    active = []
    for o in orders:
        status = str(o.get("status", "")).lower()
        if status in ("cancelled", "filled", "inactive"):
            continue
        oid = o.get("orderId") or o.get("order_id")
        if oid is not None:
            active.append(o)

    if not active:
        print("No active orders to cancel.\n")
        return

    print(f"Found {len(active)} active order(s). Cancelling ...\n")

    cancelled = 0
    failed = 0
    skipped = 0

    # Per-exchange consent tracking.
    cancel_confirm_all: bool = False
    cancel_skip_all: bool = False
    cancel_confirm_exchanges: set[str] = set()
    cancel_skip_exchanges: set[str] = set()

    for o in active:
        oid = str(o.get("orderId") or o.get("order_id"))
        ticker = o.get("ticker") or o.get("symbol") or ""
        side = o.get("side", "")
        remaining = o.get("remainingQuantity") or o.get("remaining_quantity") or ""
        price = o.get("price", "")

        # Determine the MIC for this order.
        raw_exchange = o.get("exchange") or o.get("listingExchange") or ""
        mic = exchange_to_mic(str(raw_exchange)) if raw_exchange else ""

        # Exchange filtering when not using -all-exchanges.
        if not all_exchanges and mic:
            if not is_exchange_open(mic):
                print(f"  Skipped order {oid}  {side} {remaining} {ticker} @ {price}"
                      f"  (exchange {mic} closed)")
                skipped += 1
                continue

        # --- Per-exchange auto-skip check ---
        if cancel_skip_all or mic in cancel_skip_exchanges:
            print(f"  Skipped order {oid}  {side} {remaining} {ticker} @ {price}"
                  f"  (auto-skip)")
            skipped += 1
            continue

        # --- Per-exchange auto-confirm check ---
        if cancel_confirm_all or mic in cancel_confirm_exchanges:
            auto = True
        else:
            auto = False

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
                print(f"    Skipped.")
                skipped += 1
                continue
            elif choice == "N":
                cancel_skip_all = True
                print(f"    Skipped.")
                skipped += 1
                continue
            elif choice == "S":
                print(f"    Skipped.")
                skipped += 1
                continue
            elif choice != "Y":
                print(f"    Invalid choice, skipping.")
                skipped += 1
                continue

        # Proceed with cancellation.
        try:
            client.cancel_order(account_id, oid)
            auto_tag = " (auto)" if auto else ""
            print(f"  Cancelled order {oid}  {side} {remaining} {ticker} @ {price}{auto_tag}")
            cancelled += 1
            time.sleep(0.2)
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

def _submit_and_confirm(client: IBKRClient, account_id: str,
                        order_ticket: dict) -> dict:
    """Place an order and handle the reply-confirmation loop.

    IBKR may return precautionary messages that must be confirmed via
    ``POST /iserver/reply/{replyId}`` before the order is accepted.

    Returns the final response dict (with ``order_id`` on success).
    """
    response = client.place_order(account_id, [order_ticket])

    for _ in range(MAX_REPLY_ROUNDS):
        # If the response is a list of message objects, we need to confirm.
        if isinstance(response, list):
            # Look for a reply id in the message objects.
            for msg in response:
                reply_id = msg.get("id")
                if reply_id is not None:
                    # Show the precautionary message.
                    message_text = "\n".join(msg.get("message", []))
                    if message_text:
                        print(f"    IBKR message: {message_text}")
                    response = client.confirm_reply(str(reply_id))
                    break
            else:
                # No id found – treat the list as the final answer.
                break
        else:
            break

    return response


def _format_currency(value: float) -> str:
    return f"${value:,.2f}"


def _snap_to_tick(price: float, tick_size: float, side: str) -> float:
    """Round *price* to the nearest multiple of *tick_size*.

    When the price falls exactly at the midpoint between two ticks,
    choose the more aggressive side:
      - BUY  -> round **down** (lower price is more aggressive for buyer)
      - SELL -> round **up** (higher price is more aggressive for seller)
    """
    lower = round(math.floor(price / tick_size) * tick_size, 10)
    upper = round(lower + tick_size, 10)

    dist_lower = abs(price - lower)
    dist_upper = abs(price - upper)

    if abs(dist_lower - dist_upper) < 1e-12:
        # Exactly at the midpoint — pick the aggressive side.
        return round(lower if side == "BUY" else upper, 2)
    elif dist_lower < dist_upper:
        return round(lower, 2)
    else:
        return round(upper, 2)


def _extract_error_text(response) -> str:
    """Pull all error / message text out of an IBKR order response."""
    parts: list[str] = []
    items = response if isinstance(response, list) else [response]
    for item in items:
        if isinstance(item, dict):
            if "error" in item:
                parts.append(str(item["error"]))
            for msg in item.get("message", []):
                parts.append(str(msg))
    return " ".join(parts)


# ------------------------------------------------------------------
# Interactive loop
# ------------------------------------------------------------------

def run_order_loop(client: IBKRClient, df: pd.DataFrame) -> list[dict]:
    """Iterate over the portfolio and interactively place orders.

    Parameters
    ----------
    client : IBKRClient
        Authenticated API client.
    df : pd.DataFrame
        Portfolio table with ``conid``, ``limit_price``, ``Dollar Allocation``,
        ``Name``, and ``clean_ticker`` columns.

    Returns
    -------
    list[dict]
        Summary records of all successfully placed orders.
    """
    account_id = get_account_id(client)
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
        bid = row.get("bid")
        ask = row.get("ask")

        # Determine FX rate early so we can include it in skip checks.
        ccy = row.get("currency")
        fx_raw = row.get("fx_rate")
        is_usd = pd.isna(ccy) or str(ccy).upper() == "USD"
        if is_usd:
            fx = 1.0
        elif pd.notna(fx_raw) and float(fx_raw) > 0:
            fx = float(fx_raw)
        else:
            fx = None  # non-USD with no usable rate

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
            print(
                f"[{idx + 1}/{total}] Skipping '{name}' ({ticker}) -- "
                f"{', '.join(skip_reasons)}."
            )
            continue

        conid = int(conid)
        limit_price = float(limit_price)
        dollar_alloc = float(dollar_alloc)

        # If reconciliation was performed, ``net_quantity`` tells us
        # exactly how many shares to order (signed).  Otherwise fall
        # back to the original dollar-allocation computation.
        net_qty_raw = row.get("net_quantity")
        if pd.notna(net_qty_raw):
            # Reconciled mode – use the pre-computed net quantity.
            net_qty = int(net_qty_raw)
            if net_qty == 0:
                print(
                    f"[{idx + 1}/{total}] '{name}' ({ticker}) -- "
                    "already on target, nothing to order."
                )
                continue
            side = "SELL" if net_qty < 0 else "BUY"
            abs_alloc = abs(dollar_alloc)
            quantity_initial = abs(net_qty)
        else:
            # Buy-all mode – compute from dollar allocation.
            # Dollar Allocation is in USD; limit_price is in local currency.
            side = "SELL" if dollar_alloc < 0 else "BUY"
            abs_alloc = abs(dollar_alloc)
            local_alloc = abs_alloc * fx
            quantity_initial = math.floor(local_alloc / limit_price) if limit_price > 0 else 0

        quantity = quantity_initial

        # Currency / exchange info for display and consent logic.
        ccy = row.get("currency")
        ccy_label = str(ccy) if pd.notna(ccy) else "USD"
        is_foreign = ccy_label != "USD"
        mic_raw = row.get("MIC Primary Exchange")
        mic_str = str(mic_raw).strip().upper() if pd.notna(mic_raw) else ""

        # --- Per-exchange auto-skip check ---
        if mic_str in auto_skip_exchanges:
            print(
                f"\n[{idx + 1}/{total}] {name} ({ticker}) -- "
                f"auto-skipped ({mic_str})"
            )
            continue

        # --- Prompt loop ---
        while True:
            local_amount = round(limit_price * quantity, 2)
            details = (
                f"\n[{idx + 1}/{total}] {name} ({ticker})\n"
                f"  Side              : {side}\n"
                f"  Exchange          : {mic_str or '?'}\n"
                f"  Currency          : {ccy_label}\n"
                f"  Limit Price       : {limit_price:,.2f} {ccy_label}\n"
                f"  Quantity          : {quantity}\n"
                f"  Amount            : {local_amount:,.2f} {ccy_label}\n"
            )
            if is_foreign:
                usd_amount = round(local_amount / fx, 2) if fx > 0 else None
                if usd_amount is not None:
                    details += f"  Amount (USD)      : {_format_currency(usd_amount)}\n"
            # Show reconciliation context if available.
            if pd.notna(net_qty_raw):
                existing = row.get("existing_qty", 0)
                pending = row.get("pending_qty", 0)
                target = row.get("target_qty", 0)
                details += (
                    f"  --- reconciliation ---\n"
                    f"  Target qty        : {target}\n"
                    f"  Existing position : {int(existing)}\n"
                    f"  Pending orders    : {int(pending)}\n"
                    f"  Net to order      : {int(net_qty_raw)}\n"
                )
            else:
                details += (
                    f"  Dollar Allocation : {_format_currency(dollar_alloc)}\n"
                )
            print(details)

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
                order_ticket = {
                    "conid": conid,
                    "side": side,
                    "orderType": "LMT",
                    "price": limit_price,
                    "quantity": quantity,
                    "tif": "DAY",
                }
                try:
                    result = _submit_and_confirm(client, account_id, order_ticket)

                    # Check the response body for a tick-size rejection.
                    resp_text = _extract_error_text(result)
                    tick_match = _MIN_TICK_RE.search(resp_text)
                    if tick_match:
                        tick_size = float(tick_match.group(1))
                        adjusted = _snap_to_tick(limit_price, tick_size, side)
                        print(f"    Price {_format_currency(limit_price)} doesn't "
                              f"conform to tick size {tick_size}. "
                              f"Retrying at {_format_currency(adjusted)} ...")
                        limit_price = adjusted
                        continue  # loops back, re-displays & re-submits

                    # Extract order id from response.
                    order_id = None
                    if isinstance(result, list):
                        for item in result:
                            if "order_id" in item:
                                order_id = item["order_id"]
                                break
                    elif isinstance(result, dict):
                        order_id = result.get("order_id")

                    if order_id:
                        print(f"    Order placed -- order_id: {order_id}")
                        placed_orders.append({
                            "ticker": ticker,
                            "name": name,
                            "conid": conid,
                            "side": side,
                            "quantity": quantity,
                            "limit_price": limit_price,
                            "order_id": order_id,
                        })
                    else:
                        print(f"    Order response: {result}")
                        # Still record it optimistically.
                        placed_orders.append({
                            "ticker": ticker,
                            "name": name,
                            "conid": conid,
                            "side": side,
                            "quantity": quantity,
                            "limit_price": limit_price,
                            "order_id": str(result),
                        })
                except Exception as exc:
                    # Check for tick-size error in exception text
                    # (may come from HTTP 400 or confirmation replies).
                    error_text = str(exc)
                    if hasattr(exc, "response") and exc.response is not None:
                        try:
                            error_text = exc.response.text
                        except Exception:
                            pass
                    tick_match = _MIN_TICK_RE.search(error_text)
                    if tick_match:
                        tick_size = float(tick_match.group(1))
                        adjusted = _snap_to_tick(limit_price, tick_size, side)
                        print(f"    Price {_format_currency(limit_price)} doesn't "
                              f"conform to tick size {tick_size}. "
                              f"Retrying at {_format_currency(adjusted)} ...")
                        limit_price = adjusted
                        continue  # loops back, re-displays & re-submits

                    print(f"    [!] Order failed: {exc}")
                    if confirm_all:
                        print("    Skipping (auto-confirm mode).")
                    else:
                        retry = input("    [R] Retry  [S] Skip  > ").strip().upper()
                        if retry == "R":
                            continue
                break  # Move to next ticker.

            elif choice == "M":
                new_qty = input(
                    f"  New quantity [{quantity}]: "
                ).strip()
                if new_qty:
                    try:
                        quantity = int(new_qty.replace(",", ""))
                    except ValueError:
                        print("    Invalid number, keeping original.")

                new_price = input(
                    f"  New limit price [{_format_currency(limit_price)}]: "
                ).strip()
                if new_price:
                    try:
                        limit_price = float(new_price.replace(",", "").replace("$", ""))
                    except ValueError:
                        print("    Invalid number, keeping original.")

                new_side = input(
                    f"  New side [{side}]: "
                ).strip().upper()
                if new_side in ("BUY", "SELL"):
                    side = new_side
                elif new_side:
                    print("    Invalid side, keeping original.")
                # Loop back to show updated values.
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
                print("    Invalid choice. Please enter Y, A, E, M, S, X, or Q.")

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
            f"{_format_currency(o['limit_price']):>10} {str(o['order_id']):>12}"
        )
    print("=" * 78)
    print(f"  Total orders placed: {len(orders)}\n")

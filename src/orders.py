"""Interactive order placement with user confirmation.

For each row in the portfolio table the user is prompted to confirm,
modify, skip, or quit.  Placed orders are tracked and a summary is
printed at the end.
"""

import math

import pandas as pd

from src.api_client import IBKRClient

# Maximum number of reply-confirmation round-trips before giving up.
MAX_REPLY_ROUNDS = 5


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
    total = len(df)

    for idx, row in df.iterrows():
        conid = row.get("conid")
        name = row.get("Name", "")
        ticker = row.get("clean_ticker", "")
        dollar_alloc = row.get("Dollar Allocation")
        limit_price = row.get("limit_price")
        bid = row.get("bid")
        ask = row.get("ask")

        # Skip rows that cannot be ordered.
        skip_reasons: list[str] = []
        if pd.isna(conid):
            skip_reasons.append("no conid")
        if pd.isna(limit_price):
            skip_reasons.append("no limit price")
        if pd.isna(dollar_alloc):
            skip_reasons.append("no dollar allocation")

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
            side = "SELL" if dollar_alloc < 0 else "BUY"
            abs_alloc = abs(dollar_alloc)
            quantity_initial = math.floor(abs_alloc / limit_price) if limit_price > 0 else 0

        quantity = quantity_initial

        # --- Prompt loop ---
        while True:
            planned_alloc = round(limit_price * quantity, 2)
            details = (
                f"\n[{idx + 1}/{total}] {name} ({ticker})\n"
                f"  Side              : {side}\n"
                f"  Limit Price       : {_format_currency(limit_price)}\n"
                f"  Quantity          : {quantity}\n"
                f"  Dollar Amount     : {_format_currency(planned_alloc)}\n"
            )
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

            if confirm_all:
                choice = "Y"
                print("  (auto-confirmed)")
            else:
                choice = input(
                    "  [Y] Confirm  [A] Confirm All  [M] Modify  "
                    "[S] Skip  [Q] Quit  > "
                ).strip().upper()

            if choice in ("Y", "A"):
                if choice == "A":
                    confirm_all = True
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

            elif choice == "S":
                print("    Skipped.")
                breakFS

            elif choice == "Q":
                print("    Quitting order loop.")
                return placed_orders

            else:
                print("    Invalid choice. Please enter Y, A, M, S, or Q.")

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

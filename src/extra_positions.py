"""Handle IBKR positions that are not present in the input file.

When reconciling, any position held on IBKR whose conid does not appear
in the Project Portfolio is treated as if the target quantity is 0.
This module fetches the necessary market data, cancels stale orders, and
builds synthetic DataFrame rows so the order loop can liquidate (or
cover) those positions.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd
from ib_async import IB, Contract

from src.cancel import (
    CancelState, signed_order_qty,
    resolve_cancel_decision, execute_cancel,
)
from src.config import MINIMUM_TRADING_AMOUNT
from src.contracts import exchange_to_mic
from src.exchange_hours import is_exchange_open
from src.market_data import (
    _snapshot_batch, _calc_limit_price, _resolve_fx_rate, snap_to_tick,
    SNAPSHOT_BATCH_SIZE,
)


def compute_net_quantity(
    target: int | float,
    existing: float,
    pending: float,
    limit_price: float | None = None,
    fx_rate: float | None = None,
) -> int:
    """Compute the net quantity to order and apply the min-trade filter.

    Parameters
    ----------
    target : int | float
        Desired position size (from the project portfolio).
    existing : float
        Current IBKR position size.
    pending : float
        Signed sum of remaining open-order quantities.
    limit_price : float | None
        Estimated limit price in the security's local currency.
    fx_rate : float | None
        ``local_currency / USD`` rate (1.0 for USD-denominated).

    Returns
    -------
    int
        Net shares to order.  Zeroed out when the USD value of the
        trade would be below ``MINIMUM_TRADING_AMOUNT``.
    """
    net = round(target) - round(existing) - round(pending)
    if net == 0:
        return 0
    if (limit_price is not None and limit_price > 0
            and fx_rate is not None and fx_rate > 0):
        usd_value = abs(net) * limit_price / fx_rate
        if usd_value < MINIMUM_TRADING_AMOUNT:
            return 0
    return net


# ==================================================================
# Per-conid metadata
# ==================================================================

@dataclass
class _ExtraInfo:
    """Bundled metadata for one extra IBKR position."""

    contract: Contract | None
    currency: str
    fx_rate: float | None
    market_rules: str
    long_name: str


# ==================================================================
# Phase 1: Qualify contracts and gather metadata
# ==================================================================

def _fetch_extra_metadata(
    ib: IB,
    extra_conids: list[int],
    position_meta: dict[int, dict],
) -> dict[int, _ExtraInfo]:
    """Qualify contracts and fetch currencies, market rules, long names,
    and FX rates for every extra conid.
    """
    extra_contracts = [Contract(conId=cid) for cid in extra_conids]
    qualified = ib.qualifyContracts(*extra_contracts)
    cid_to_contract = {c.conId: c for c in qualified if c.conId}

    info: dict[int, _ExtraInfo] = {}
    for cid in extra_conids:
        qc = cid_to_contract.get(cid)
        pm = position_meta.get(cid, {})
        fallback_name = pm.get("ticker", str(cid))

        if qc:
            currency = (qc.currency or "USD").upper()
            market_rules = ""
            long_name = fallback_name
            try:
                cds = ib.reqContractDetails(qc)
                if cds:
                    market_rules = cds[0].marketRuleIds or ""
                    if cds[0].longName:
                        long_name = cds[0].longName
            except Exception:
                pass
        else:
            currency = "USD"
            market_rules = ""
            long_name = fallback_name

        info[cid] = _ExtraInfo(
            contract=qc,
            currency=currency,
            fx_rate=None,       # filled below
            market_rules=market_rules,
            long_name=long_name,
        )

    # Resolve FX rates for unique non-USD currencies.
    unique_ccys = {ei.currency for ei in info.values() if ei.currency != "USD"}
    fx_rates: dict[str, float] = {"USD": 1.0}
    for ccy in sorted(unique_ccys):
        resolved = _resolve_fx_rate(ib, ccy)
        if resolved is not None:
            fx_rates[ccy] = resolved

    for ei in info.values():
        ei.fx_rate = fx_rates.get(ei.currency)

    return info


# ==================================================================
# Phase 2: Fetch market-data snapshots
# ==================================================================

def _fetch_extra_snapshots(
    ib: IB,
    extra_conids: list[int],
    info: dict[int, _ExtraInfo],
) -> dict[int, dict]:
    """Batch-fetch market-data snapshots for all extra positions."""
    contracts_list = [
        info[cid].contract for cid in extra_conids
        if info[cid].contract is not None
    ]

    snapshot: dict[int, dict] = {}
    total_batches = (math.ceil(len(contracts_list) / SNAPSHOT_BATCH_SIZE)
                     if contracts_list else 0)
    for i in range(0, len(contracts_list), SNAPSHOT_BATCH_SIZE):
        batch = contracts_list[i : i + SNAPSHOT_BATCH_SIZE]
        batch_num = i // SNAPSHOT_BATCH_SIZE + 1
        print(f"  Extra batch {batch_num}/{total_batches} "
              f"({len(batch)} contracts) …")
        snapshot.update(_snapshot_batch(ib, batch))

    return snapshot


# ==================================================================
# Phase 3: Cancel stale orders on extra positions
# ==================================================================

def _cancel_extra_orders(
    ib: IB,
    extra_conids: list[int],
    orders_by_conid: dict[int, list[dict]],
    position_meta: dict[int, dict],
    info: dict[int, _ExtraInfo],
    all_exchanges: bool,
    state: CancelState,
    dry_run: bool,
) -> tuple[dict[int, float], int]:
    """Cancel stale orders for extra positions and track pending qty.

    For extra positions the target is 0, so every open order is stale.
    Orders that are *kept* (not cancelled) have their signed quantity
    recorded so ``net_quantity`` accounts for them later.

    Returns
    -------
    pending_by_conid : dict[int, float]
        Signed pending quantity per conid (from kept orders only).
    cancelled_count : int
    """
    cancelled = 0
    pending: dict[int, float] = {}

    for cid in extra_conids:
        conid_orders = orders_by_conid.get(cid, [])
        ei = info[cid]
        pm = position_meta.get(cid, {})
        raw_exchange = pm.get("exchange", "")
        mic = exchange_to_mic(raw_exchange) if raw_exchange else ""

        # In dry-run mode, treat every order as kept (no cancellation).
        if dry_run:
            for order in conid_orders:
                pending[cid] = pending.get(cid, 0) + signed_order_qty(order)
            continue

        can_cancel = all_exchanges or (bool(mic) and is_exchange_open(mic))

        for order in conid_orders:
            header = (
                f"\n  Extra-position stale order "
                f"{order['orderId']} for '{ei.long_name}' "
                f"(price={order.get('price')})\n"
                f"  Exchange: {mic or '?'}"
            )

            decision, is_auto = resolve_cancel_decision(
                mic, can_cancel, state, prompt_header=header)

            if decision == "skip":
                reason = ("exchange closed" if not can_cancel
                          else "auto-skip" if is_auto else "skipped")
                print(f"  Extra-position order {order['orderId']} "
                      f"for '{ei.long_name}' — {reason}")
                pending[cid] = pending.get(cid, 0) + signed_order_qty(order)
                continue

            # Cancel.
            trade_obj = order.get("trade")
            if trade_obj and execute_cancel(ib, trade_obj.order):
                auto_tag = " (auto)" if is_auto else ""
                print(f"  Cancelled extra-position order "
                      f"{order['orderId']} for '{ei.long_name}'"
                      f"{auto_tag}")
                cancelled += 1
            else:
                print(f"  [!] Failed to cancel order "
                      f"{order['orderId']}")
                pending[cid] = pending.get(cid, 0) + signed_order_qty(order)

    return pending, cancelled


# ==================================================================
# Phase 4: Build synthetic DataFrame rows
# ==================================================================

def _build_extra_rows(
    ib: IB,
    extra_conids: list[int],
    positions: dict[int, float],
    pending_by_conid: dict[int, float],
    position_meta: dict[int, dict],
    snapshot: dict[int, dict],
    info: dict[int, _ExtraInfo],
) -> list[dict]:
    """Build synthetic row dicts for extra positions.

    Each row mirrors the columns of the main portfolio DataFrame so
    it can be appended directly.
    """
    extra_rows: list[dict] = []

    for cid in extra_conids:
        existing = positions.get(cid, 0)
        pending = pending_by_conid.get(cid, 0)
        if existing == 0:
            continue

        ei = info[cid]
        pm = position_meta.get(cid, {})
        raw_exchange = pm.get("exchange", "")
        mic_code = exchange_to_mic(raw_exchange) if raw_exchange else ""
        ticker = pm.get("ticker", str(cid))

        snap = snapshot.get(cid, {})
        row_dict: dict = {
            "conid": float(cid),
            "Name": ei.long_name,
            "clean_ticker": ticker,
            "IBKR Name": ei.long_name,
            "IBKR Ticker": ticker,
            "MIC Primary Exchange": mic_code,
            "currency": ei.currency,
            "fx_rate": ei.fx_rate,
            "Basket Allocation": 0.0,
            "Dollar Allocation": 0.0,
            "bid": snap.get("bid"),
            "ask": snap.get("ask"),
            "last": snap.get("last"),
            "close": snap.get("close"),
            "day_high": snap.get("high"),
            "day_low": snap.get("low"),
            "is_option": False,
            "existing_qty": existing,
            "pending_qty": pending,
            "target_qty": 0,
            "cancelled_orders": 0,
        }

        # Compute limit price using the shared spread-based formula.
        is_sell = existing > 0
        limit_price = _calc_limit_price(row_dict, is_sell=is_sell)

        # Snap limit price to valid tick increment.
        if limit_price is not None and ei.market_rules:
            limit_price = round(
                snap_to_tick(limit_price, ib, ei.market_rules,
                             is_buy=not is_sell),
                10,
            )

        row_dict["limit_price"] = limit_price
        row_dict["market_rule_ids"] = ei.market_rules
        row_dict["net_quantity"] = compute_net_quantity(
            target=0, existing=existing, pending=pending,
            limit_price=limit_price, fx_rate=ei.fx_rate,
        )

        extra_rows.append(row_dict)

    return extra_rows


# ==================================================================
# Public API
# ==================================================================

def reconcile_extra_positions(
    ib: IB,
    extra_conids: list[int],
    positions: dict[int, float],
    position_meta: dict[int, dict],
    orders_by_conid: dict[int, list[dict]],
    all_exchanges: bool,
    cancel_state: CancelState,
    dry_run: bool = False,
) -> tuple[list[dict], int]:
    """Process IBKR positions not in the input file.

    When *dry_run* is ``True``, no orders are cancelled — all open
    orders are counted as pending and synthetic rows are built for
    read-only display.

    Returns
    -------
    extra_rows : list[dict]
        Synthetic row dicts ready to be appended to the DataFrame.
    extra_cancelled : int
        Number of stale orders cancelled for extra positions.
    """
    print(f"\nFound {len(extra_conids)} IBKR position(s) not in the "
          f"input file. Fetching market data to prepare sell orders ...")

    # 1. Qualify contracts and gather metadata.
    info = _fetch_extra_metadata(ib, extra_conids, position_meta)

    # 2. Fetch market-data snapshots.
    snapshot = _fetch_extra_snapshots(ib, extra_conids, info)

    # 3. Cancel stale orders (all orders are stale for extra positions).
    pending_by_conid, extra_cancelled = _cancel_extra_orders(
        ib, extra_conids, orders_by_conid, position_meta, info,
        all_exchanges, cancel_state, dry_run,
    )

    # 4. Build synthetic rows for the order loop.
    extra_rows = _build_extra_rows(
        ib, extra_conids, positions, pending_by_conid,
        position_meta, snapshot, info,
    )

    if extra_rows:
        print(f"  Prepared {len(extra_rows)} extra-position row(s) "
              f"to sell/cover.")
    if extra_cancelled:
        print(f"  Extra-position orders cancelled: {extra_cancelled}")

    return extra_rows, extra_cancelled

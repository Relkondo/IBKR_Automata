"""Shared order-cancellation logic.

Provides the interactive cancel/skip prompt and execution helpers used
by stale-order cancellation (reconcile), extra-position cancellation,
and cancel-all-orders.

The ``CancelState`` dataclass replaces the loose boolean/set variables
that were previously threaded through multiple function signatures and
return tuples.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ib_async import IB

from src.connection import suppress_errors


# ==================================================================
# Consent state
# ==================================================================

@dataclass
class CancelState:
    """Tracks user consent across a batch of cancel decisions.

    Supports per-exchange granularity so the user can
    "Cancel All XNYS" or "Skip All XFRA" without affecting
    other exchanges.  Mutated in-place by ``resolve_cancel_decision``.
    """

    confirm_all: bool = False
    skip_all: bool = False
    confirm_exchanges: set[str] = field(default_factory=set)
    skip_exchanges: set[str] = field(default_factory=set)


# ==================================================================
# Helpers
# ==================================================================

def signed_order_qty(order: dict) -> float:
    """Signed remaining quantity: positive for BUY, negative for SELL."""
    qty = order["remainingQuantity"]
    return qty if order["side"] == "BUY" else -qty


# ==================================================================
# Decision & execution
# ==================================================================

def resolve_cancel_decision(
    mic: str,
    can_cancel: bool,
    state: CancelState,
    prompt_header: str = "",
) -> tuple[str, bool]:
    """Decide whether to cancel an order, prompting the user if needed.

    Parameters
    ----------
    mic : str
        MIC exchange code for per-exchange grouping.
    can_cancel : bool
        ``False`` when the exchange is closed and cancellation should
        be blocked.
    state : CancelState
        Mutable consent state — updated in-place when the user picks
        an "all" or per-exchange option.
    prompt_header : str
        Lines to print *before* the interactive prompt (order details
        and exchange).  Not printed when the decision is automatic.

    Returns
    -------
    decision : str
        ``"cancel"`` or ``"skip"``.
    is_auto : bool
        ``True`` when the decision was made without user interaction
        (exchange closed, auto-skip, or auto-confirm).
    """
    # Exchange closed — can't cancel.
    if not can_cancel:
        return "skip", True

    # User previously chose to skip all / skip this exchange.
    if state.skip_all or mic in state.skip_exchanges:
        return "skip", True

    # User previously chose to confirm all / confirm this exchange.
    if state.confirm_all or mic in state.confirm_exchanges:
        return "cancel", True

    # Interactive prompt.
    if prompt_header:
        print(prompt_header)

    mic_label = mic or "?"
    choice = input(
        f"  [Y] Cancel  [A] Cancel All  "
        f"[E] Cancel All {mic_label}  "
        f"[S] Skip  [X] Skip All {mic_label}  "
        f"[N] Skip All  > "
    ).strip().upper()

    if choice == "A":
        state.confirm_all = True
        return "cancel", False
    if choice == "E":
        state.confirm_exchanges.add(mic)
        return "cancel", False
    if choice == "X":
        state.skip_exchanges.add(mic)
        return "skip", False
    if choice == "N":
        state.skip_all = True
        return "skip", False
    if choice == "Y":
        return "cancel", False
    # S or invalid input — default to skip.
    return "skip", False


def execute_cancel(ib: IB, order_obj) -> bool:
    """Cancel an order via ``ib.cancelOrder()``.

    Suppresses error 202 (order already cancelled) and waits briefly
    for TWS acknowledgement.

    Returns ``True`` on success, ``False`` on failure.
    """
    try:
        with suppress_errors(202):
            ib.cancelOrder(order_obj)
            ib.sleep(0.3)
        return True
    except Exception:
        return False

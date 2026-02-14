"""Connect to TWS via ib_async.

Provides a thin wrapper that establishes a connection to a running
Trader Workstation instance and returns the ``IB`` handle used by
all other modules.  Also provides a ``suppress_errors`` context
manager for silencing specific IBKR error codes during cancellation.
"""

from __future__ import annotations

from contextlib import contextmanager

from ib_async import IB

from src.config import TWS_HOST, TWS_PORT, TWS_CLIENT_ID

# ==================================================================
# Error-code suppression (used during order cancellation etc.)
# ==================================================================

_suppressed_codes: set[int] = set()


@contextmanager
def suppress_errors(*codes: int):
    """Context manager that silences specific IBKR error codes.

    Patches the ``Wrapper.error`` method on the connected IB instance
    so the suppressed codes never reach the logger or error event.

    Usage::

        with suppress_errors(202):
            ib.cancelOrder(order)
            ib.sleep(0.3)
    """
    _suppressed_codes.update(codes)
    try:
        yield
    finally:
        _suppressed_codes.difference_update(codes)


# ==================================================================
# Connection health
# ==================================================================

def ensure_connected(ib: IB) -> None:
    """Raise ``RuntimeError`` if the TWS connection is broken.

    Call this before any operation whose empty result would be
    silently misinterpreted (e.g. fetching positions or orders).
    """
    if not ib.isConnected():
        raise RuntimeError(
            "TWS connection lost. Cannot continue — results would be "
            "unreliable.  Please restart the program with TWS running."
        )


# ==================================================================
# Connection
# ==================================================================

def connect() -> IB:
    """Connect to TWS and return the IB handle.

    TWS must already be running and authenticated.  Market data type
    is set to 3 (delayed): live data is returned when a subscription
    exists, otherwise TWS automatically provides 15-min delayed data.
    """
    ib = IB()
    ib.connect(TWS_HOST, TWS_PORT, clientId=TWS_CLIENT_ID)

    # Patch the wrapper's error method to support ``suppress_errors``.
    _original_error = ib.wrapper.error

    def _filtered_error(
        reqId: int,
        errorCode: int,
        errorString: str,
        advancedOrderRejectJson: str = "",
    ) -> None:
        if errorCode in _suppressed_codes:
            return
        _original_error(reqId, errorCode, errorString,
                        advancedOrderRejectJson)

    ib.wrapper.error = _filtered_error

    # Type 3: live if subscribed, delayed otherwise.
    ib.reqMarketDataType(3)

    accounts = ib.managedAccounts()
    if not accounts:
        raise RuntimeError("No managed accounts returned by TWS.")
    print(f"Connected to TWS (account: {accounts[0]})\n")
    return ib

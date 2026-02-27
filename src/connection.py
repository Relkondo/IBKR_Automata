"""Connect to TWS via ib_async.

Provides a thin wrapper that establishes a connection to a running
Trader Workstation instance and returns the ``IB`` handle used by
all other modules.  Also provides a ``suppress_errors`` context
manager for silencing specific IBKR error codes.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager

from ib_async import IB

from src.config import TWS_HOST, TWS_PORT, TWS_CLIENT_ID

# ==================================================================
# Error-code suppression
# ==================================================================

_suppressed_codes: set[int] = set()

_IB_LOGGER = logging.getLogger("ib_async.wrapper")


class _ErrorCodeFilter(logging.Filter):
    """Drop log records whose message matches a suppressed error code."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not _suppressed_codes:
            return True
        msg = record.getMessage()
        return not any(
            f"Error {code}," in msg or f"Warning {code}," in msg
            for code in _suppressed_codes
        )


_error_filter = _ErrorCodeFilter()


@contextmanager
def suppress_errors(*codes: int):
    """Context manager that silences specific IBKR error codes.

    Only suppresses the log output — the internal request lifecycle
    (``_endReq``, trade updates, etc.) still proceeds normally.

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

    # Attach the log filter so ``suppress_errors`` can mute specific
    # error codes without interfering with the request lifecycle.
    _IB_LOGGER.addFilter(_error_filter)

    # Type 3: live if subscribed, delayed otherwise.
    ib.reqMarketDataType(3)

    accounts = ib.managedAccounts()
    if not accounts:
        raise RuntimeError("No managed accounts returned by TWS.")
    print(f"Connected to TWS (account: {accounts[0]})\n")
    return ib

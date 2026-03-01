"""Connect to IBKR (TWS or IB Gateway) via ib_async.

Provides a thin wrapper that establishes a connection to a running
TWS or IB Gateway instance and returns the ``IB`` handle used by
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

def connect(*, auto_start_gateway: bool = False) -> IB:
    """Connect to IBKR (TWS or IB Gateway) and return the IB handle.

    Parameters
    ----------
    auto_start_gateway:
        When *True*, call :func:`src.gateway.ensure_gateway` before
        attempting to connect so that IB Gateway is launched via IBC
        if it is not already running.  Intended for ``-auto`` /
        cron-job execution.

    Market data type is set to 3 (delayed): live data is returned
    when a subscription exists, otherwise 15-min delayed data is
    provided automatically.
    """
    if auto_start_gateway:
        from src.gateway import ensure_gateway
        ensure_gateway()

    ib = IB()
    ib.connect(TWS_HOST, TWS_PORT, clientId=TWS_CLIENT_ID)

    _IB_LOGGER.addFilter(_error_filter)

    # Type 3: live if subscribed, delayed otherwise.
    ib.reqMarketDataType(3)

    accounts = ib.managedAccounts()
    if not accounts:
        raise RuntimeError("No managed accounts returned by IBKR.")

    # IB Gateway (unlike TWS) doesn't connect to data farms until a
    # request is made.  The startup fetch may return empty because the
    # data-farm link isn't established yet.  Force a fresh sync so
    # downstream code never sees stale/empty caches.
    ib.reqPositions()
    ib.reqAllOpenOrders()
    ib.reqAccountSummary()
    ib.sleep(2)

    print(f"Connected to IBKR (account: {accounts[0]})\n")
    return ib

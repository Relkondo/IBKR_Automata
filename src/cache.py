"""Cache readiness management for IBKR portfolio data.

The IBKR gateway's positions cache takes 10-30 minutes to fully populate
after invalidation.  This module decouples *invalidation* from *reading*
by recording when the cache was last invalidated and blocking order
placement until enough time has elapsed.

Typical lifecycle:

1. ``run_get_cache_ready``  -- cancel orders, invalidate cache, prime,
   record timestamp.
2. User waits >= ``CACHE_READY_WAIT_MINUTES``.
3. Normal run checks ``check_cache_ready`` before ordering.
4. After placing / cancelling orders, ``invalidate_and_record`` is called
   again so the next cycle knows the cache is stale.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone

from src.api_client import IBKRClient
from src.config import ASSETS_DIR
from src.orders import cancel_all_orders, get_account_id

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CACHE_READY_FILE = os.path.join(ASSETS_DIR, ".cache_ready")
CACHE_READY_WAIT_MINUTES = 30


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _read_timestamp() -> datetime | None:
    """Read the ISO timestamp from the cache-ready file, or None."""
    if not os.path.isfile(CACHE_READY_FILE):
        return None
    try:
        with open(CACHE_READY_FILE, "r") as fh:
            raw = fh.read().strip()
        return datetime.fromisoformat(raw)
    except (ValueError, OSError):
        return None


def _write_timestamp() -> None:
    """Write the current UTC time to the cache-ready file."""
    os.makedirs(os.path.dirname(CACHE_READY_FILE), exist_ok=True)
    with open(CACHE_READY_FILE, "w") as fh:
        fh.write(datetime.now(timezone.utc).isoformat())


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def invalidate_and_record(client: IBKRClient, account_id: str) -> None:
    """Invalidate the positions cache, trigger a refill, and record the time.

    Sequence:
      1. ``POST /portfolio/{accountId}/positions/invalidate``
      2. Short delay.
      3. ``GET /portfolio/accounts``  (priming – tells the gateway to
         start reloading portfolio data).
      4. Write current timestamp to ``assets/.cache_ready``.
    """
    print("Invalidating positions cache ...")
    client.invalidate_positions_cache(account_id)
    time.sleep(1)
    client.session.get(f"{client.base_url}/portfolio/accounts")
    _write_timestamp()
    print("Cache invalidated and primed.  Timestamp recorded.\n")


def check_cache_ready() -> tuple[bool, str]:
    """Check whether enough time has passed since the last invalidation.

    Returns
    -------
    (ready, message) : tuple[bool, str]
        *ready* is ``True`` when positions data should be reliable.
        *message* explains the situation when *ready* is ``False``.
    """
    ts = _read_timestamp()
    if ts is None:
        return (
            False,
            "Cache has never been prepared.  "
            "Run with 'get-cache-ready' first.",
        )

    now = datetime.now(timezone.utc)
    # If the stored timestamp is naive, assume UTC.
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    elapsed = now - ts
    elapsed_min = elapsed.total_seconds() / 60

    if elapsed_min < CACHE_READY_WAIT_MINUTES:
        remaining = CACHE_READY_WAIT_MINUTES - elapsed_min
        return (
            False,
            f"Cache was invalidated {elapsed_min:.0f} min ago -- "
            f"need to wait {remaining:.0f} more min before positions "
            f"are reliable.",
        )

    return (True, "")


def run_get_cache_ready(client: IBKRClient,
                        all_exchanges: bool = True) -> None:
    """Full cache-preparation flow.

    1. Cancel all open orders (respecting ``-all-exchanges``).
    2. Invalidate the positions cache and prime for refill.
    3. Record the invalidation timestamp.
    """
    # 1. Cancel orders.
    cancel_all_orders(client, all_exchanges=all_exchanges)

    # 2 & 3. Invalidate + prime + record.
    account_id = get_account_id(client)
    invalidate_and_record(client, account_id)

    print(
        f"get-cache-ready complete.  "
        f"Please wait {CACHE_READY_WAIT_MINUTES} min for the cache "
        f"to fully reload before placing orders.\n"
    )

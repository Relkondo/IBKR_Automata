"""Connect to TWS via ib_async.

Provides a thin wrapper that establishes a connection to a running
Trader Workstation instance and returns the ``IB`` handle used by
all other modules.
"""

from ib_async import IB

from src.config import TWS_HOST, TWS_PORT, TWS_CLIENT_ID


def connect() -> IB:
    """Connect to TWS and return the IB handle.

    TWS must already be running and authenticated.  Market data type
    is set to 3 (delayed): live data is returned when a subscription
    exists, otherwise TWS automatically provides 15-min delayed data.
    """
    ib = IB()
    ib.connect(TWS_HOST, TWS_PORT, clientId=TWS_CLIENT_ID)

    # Type 3: live if subscribed, delayed otherwise.
    ib.reqMarketDataType(3)

    accounts = ib.managedAccounts()
    print(f"Connected to TWS (account: {accounts[0]})\n")
    return ib

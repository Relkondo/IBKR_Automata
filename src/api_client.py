"""Centralized IBKR Client Portal API client.

Wraps all HTTP calls to the CP Gateway in a single requests.Session
with SSL verification disabled (self-signed cert on localhost).
"""

import urllib3
import requests

from src.config import BASE_URL

# Suppress the InsecureRequestWarning from urllib3 since the CP Gateway
# uses a self-signed certificate on localhost.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class IBKRClient:
    """Thin wrapper around the IBKR Client Portal Gateway REST API."""

    def __init__(self, base_url: str = BASE_URL):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.verify = False

    # ------------------------------------------------------------------
    # Session / auth
    # ------------------------------------------------------------------

    def tickle(self) -> dict:
        """Keep the session alive."""
        resp = self.session.post(f"{self.base_url}/tickle")
        resp.raise_for_status()
        return resp.json()

    def auth_status(self) -> dict:
        """Return the current authentication status."""
        resp = self.session.post(f"{self.base_url}/iserver/auth/status")
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Accounts
    # ------------------------------------------------------------------

    def get_accounts(self) -> dict:
        """Retrieve brokerage accounts.

        Returns the JSON payload which typically contains an 'accounts' list.
        """
        resp = self.session.get(f"{self.base_url}/iserver/accounts")
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Contract / secdef
    # ------------------------------------------------------------------

    def search_secdef(self, symbol: str, sec_type: str = "STK",
                      name: bool = False) -> list:
        """Search for a security definition by symbol or company name.

        Parameters
        ----------
        symbol : str
            Ticker symbol (e.g. "AAPL") or company name if *name* is True.
        sec_type : str
            Security type – "STK", "OPT", "FUT", etc.
        name : bool
            If True, the *symbol* field is treated as a company name search
            rather than a ticker lookup.

        Returns
        -------
        list
            List of matching contract objects.
        """
        payload: dict = {"symbol": symbol, "secType": sec_type}
        if name:
            payload["name"] = True
        resp = self.session.post(
            f"{self.base_url}/iserver/secdef/search", json=payload
        )
        resp.raise_for_status()
        return resp.json()

    def get_secdef_info(self, conid: int, sec_type: str = "OPT",
                        month: str | None = None,
                        right: str | None = None,
                        strike: float | None = None) -> list:
        """Retrieve security definition info (primarily for options).

        Parameters
        ----------
        conid : int
            The underlying contract ID.
        sec_type : str
            Security type (typically "OPT").
        month : str | None
            Option expiry month, e.g. "MAR25".
        right : str | None
            "C" for Call, "P" for Put.
        strike : float | None
            Strike price.

        Returns
        -------
        list
            Matching security definitions.
        """
        params: dict = {"conid": conid, "secType": sec_type}
        if month is not None:
            params["month"] = month
        if right is not None:
            params["right"] = right
        if strike is not None:
            params["strike"] = strike
        resp = self.session.get(
            f"{self.base_url}/iserver/secdef/info", params=params
        )
        resp.raise_for_status()
        return resp.json()

    def get_secdef_batch(self, conids: list[int]) -> list[dict]:
        """Fetch security definitions for a batch of conids.

        Uses ``GET /trsrv/secdef?conids=...`` which returns details
        including the trading currency for each contract.

        Parameters
        ----------
        conids : list[int]
            Contract IDs to look up.

        Returns
        -------
        list[dict]
            One dict per conid with fields like ``conid``, ``currency``,
            ``name``, ``assetClass``, etc.
        """
        resp = self.session.get(
            f"{self.base_url}/trsrv/secdef",
            params={"conids": ",".join(str(c) for c in conids)},
        )
        resp.raise_for_status()
        data = resp.json()
        # Response may be {"secdef": [...]} or a bare list.
        if isinstance(data, dict):
            return data.get("secdef", [])
        if isinstance(data, list):
            return data
        return []

    def get_exchange_rate(self, source: str, target: str) -> float | None:
        """Fetch the exchange rate from *source* to *target* currency.

        Uses ``GET /iserver/exchangerate?source=...&target=...``.

        Returns the rate as a float, or None on failure.
        """
        resp = self.session.get(
            f"{self.base_url}/iserver/exchangerate",
            params={"source": source, "target": target},
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("rate")

    # ------------------------------------------------------------------
    # Market data
    # ------------------------------------------------------------------

    def get_market_snapshot(self, conids: list[int],
                           fields: list[str] | None = None) -> list:
        """Request a market data snapshot for the given conids.

        Parameters
        ----------
        conids : list[int]
            Contract IDs to query.
        fields : list[str] | None
            Field codes (e.g. ["84", "86"] for bid/ask).

        Returns
        -------
        listF
            One dict per conid with requested field values.
        """
        params: dict = {"conids": ",".join(str(c) for c in conids)}
        if fields:
            params["fields"] = ",".join(fields)
        resp = self.session.get(
            f"{self.base_url}/iserver/marketdata/snapshot", params=params
        )
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Portfolio positions
    # ------------------------------------------------------------------

    def get_positions(self, account_id: str) -> list[dict]:
        """Retrieve all portfolio positions across all pages.

        Calls ``GET /portfolio/{accountId}/positions/{pageId}`` and
        paginates until an empty page is returned.

        Returns
        -------
        list[dict]
            Flat list of position dicts, each containing at least
            ``conid``, ``position``, ``contractDesc``, ``mktValue``, etc.
        """
        all_positions: list[dict] = []
        page = 0
        while True:
            resp = self.session.get(
                f"{self.base_url}/portfolio/{account_id}/positions/{page}"
            )
            resp.raise_for_status()
            data = resp.json()
            if not data:
                break
            all_positions.extend(data)
            page += 1
        return all_positions

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------

    def get_live_orders(self) -> list[dict]:
        """Retrieve all open/live orders for the day.

        The IBKR API sometimes requires two calls – the first wakes the
        server, the second returns actual data.  We call twice with a
        short pause in between.

        Returns
        -------
        list[dict]
            List of order dicts, each containing at least ``orderId``,
            ``conid``, ``side``, ``price``, ``remainingQuantity``,
            ``filledQuantity``, ``status``, etc.
        """
        import time

        url = f"{self.base_url}/iserver/account/orders"
        # First call to wake the endpoint.
        resp = self.session.get(url)
        resp.raise_for_status()
        time.sleep(0.5)
        # Second call for actual data.
        resp = self.session.get(url)
        resp.raise_for_status()
        data = resp.json()

        # Response may be {"orders": [...]} or a bare list.
        if isinstance(data, dict):
            return data.get("orders", [])
        if isinstance(data, list):
            return data
        return []

    def cancel_order(self, account_id: str, order_id: str) -> dict:
        """Cancel a single open order.

        Parameters
        ----------
        account_id : str
            The brokerage account ID.
        order_id : str
            The IBKR order ID to cancel.

        Returns
        -------
        dict
            Confirmation or error from IBKR.
        """
        resp = self.session.delete(
            f"{self.base_url}/iserver/account/{account_id}/order/{order_id}"
        )
        resp.raise_for_status()
        return resp.json()

    def place_order(self, account_id: str, orders: list[dict]) -> dict | list:
        """Submit one or more order tickets.

        Parameters
        ----------
        account_id : str
            The brokerage account ID.
        orders : list[dict]
            List of order ticket dicts (conid, side, orderType, price, …).

        Returns
        -------
        dict | list
            Response from IBKR – may be an order confirmation, a list of
            precautionary messages, or an error.
        """
        payload = {"orders": orders}
        resp = self.session.post(
            f"{self.base_url}/iserver/account/{account_id}/orders",
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()

    def confirm_reply(self, reply_id: str) -> dict | list:
        """Confirm a precautionary order message.

        Parameters
        ----------
        reply_id : str
            The reply/message ID returned by a previous order submission.

        Returns
        -------
        dict | list
            Updated order status after confirmation.
        """
        resp = self.session.post(
            f"{self.base_url}/iserver/reply/{reply_id}",
            json={"confirmed": True},
        )
        resp.raise_for_status()
        return resp.json()

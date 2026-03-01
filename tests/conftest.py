"""Shared fixtures for IBKR Automata test suite."""

from __future__ import annotations

from dataclasses import dataclass, field
from unittest.mock import MagicMock, PropertyMock

import pandas as pd
import pytest


# ── Mock IBKR objects ──────────────────────────────────────────────


@dataclass
class MockContract:
    conId: int = 0
    symbol: str = ""
    secType: str = "STK"
    currency: str = "USD"
    exchange: str = "SMART"
    primaryExchange: str = ""
    lastTradeDateOrContractMonth: str = ""
    description: str = ""


@dataclass
class MockContractDetails:
    contract: MockContract = field(default_factory=MockContract)
    longName: str = ""
    marketRuleIds: str = ""


@dataclass
class MockOrder:
    orderId: int = 0
    action: str = "BUY"
    totalQuantity: float = 0
    lmtPrice: float = 0.0
    orderType: str = "LMT"
    percentOffset: float = 0.0
    tif: str = "DAY"


@dataclass
class MockOrderStatus:
    status: str = "Submitted"
    remaining: float = 0


@dataclass
class MockLogEntry:
    errorCode: int = 0
    message: str = ""


@dataclass
class MockTrade:
    contract: MockContract = field(default_factory=MockContract)
    order: MockOrder = field(default_factory=MockOrder)
    orderStatus: MockOrderStatus = field(default_factory=MockOrderStatus)
    log: list = field(default_factory=list)


@dataclass
class MockTicker:
    contract: MockContract | None = field(default_factory=MockContract)
    bid: float = -1
    ask: float = -1
    last: float = -1
    close: float = -1
    high: float = -1
    low: float = -1

    def marketPrice(self):
        if self.last > 0:
            return self.last
        if self.close > 0:
            return self.close
        return float("nan")


@dataclass
class MockPosition:
    contract: MockContract = field(default_factory=MockContract)
    position: float = 0
    avgCost: float = 0


@dataclass
class MockPortfolioItem:
    contract: MockContract = field(default_factory=MockContract)
    position: float = 0
    marketValue: float = 0
    averageCost: float = 0


@dataclass
class MockAccountValue:
    tag: str = ""
    value: str = "0"
    currency: str = "USD"
    account: str = ""


@dataclass
class MockPriceIncrement:
    lowEdge: float = 0.0
    increment: float = 0.01


@dataclass
class MockMatchingSymbol:
    contract: MockContract = field(default_factory=MockContract)


# ── Fixtures ───────────────────────────────────────────────────────


@pytest.fixture
def mock_ib():
    """A fully mocked IB connection object."""
    ib = MagicMock()
    ib.isConnected.return_value = True
    ib.managedAccounts.return_value = ["U1234567"]
    ib.positions.return_value = []
    ib.openTrades.return_value = []
    ib.portfolio.return_value = []
    ib.accountSummary.return_value = []
    ib.qualifyContracts.return_value = []
    ib.reqContractDetails.return_value = []
    ib.reqTickers.return_value = []
    ib.reqMarketRule.return_value = []
    ib.reqMatchingSymbols.return_value = []
    ib.reqMktData.return_value = MockTicker()
    ib.sleep.return_value = None
    ib.placeOrder.return_value = MockTrade()
    ib.cancelOrder.return_value = None
    ib.errorEvent = MagicMock()
    ib.errorEvent.__iadd__ = MagicMock(return_value=ib.errorEvent)
    ib.errorEvent.__isub__ = MagicMock(return_value=ib.errorEvent)
    return ib


@pytest.fixture
def sample_portfolio_df():
    """A minimal portfolio DataFrame mimicking post-load_portfolio output."""
    return pd.DataFrame({
        "Ticker": ["AAPL US Equity", "MSFT US Equity", "QQQ US 03/21/26 P500 Equity"],
        "Security Ticker": ["AAPL US Equity", "MSFT US Equity", None],
        "Name": ["APPLE INC", "MICROSOFT CORP", "March 26 Puts on QQQ"],
        "Basket Allocation": [5.0, 3.0, 2.0],
        "MIC Primary Exchange": ["XNAS", "XNAS", "XNAS"],
        "is_option": [False, False, True],
        "clean_ticker": ["AAPL", "MSFT", "QQQ"],
    })


@pytest.fixture
def enriched_portfolio_df():
    """A portfolio DataFrame with conids, market data, and computed fields."""
    return pd.DataFrame({
        "Ticker": ["AAPL US Equity", "MSFT US Equity"],
        "Security Ticker": ["AAPL US Equity", "MSFT US Equity"],
        "Name": ["APPLE INC", "MICROSOFT CORP"],
        "IBKR Name": ["APPLE INC", "MICROSOFT CORP"],
        "IBKR Ticker": ["AAPL", "MSFT"],
        "Name Mismatch": [False, False],
        "is_option": [False, False],
        "clean_ticker": ["AAPL", "MSFT"],
        "MIC Primary Exchange": ["XNAS", "XNAS"],
        "conid": [265598, 272093],
        "currency": ["USD", "USD"],
        "fx_rate": [1.0, 1.0],
        "Basket Allocation": [5.0, 3.0],
        "Dollar Allocation": [5000.0, 3000.0],
        "bid": [175.0, 420.0],
        "ask": [175.50, 420.50],
        "last": [175.25, 420.25],
        "close": [174.0, 419.0],
        "day_high": [176.0, 421.0],
        "day_low": [174.0, 419.0],
        "market_rule_ids": ["26,240", "26,240"],
        "limit_price": [178.50, 428.40],
        "Qty": [28, 7],
        "Actual Dollar Allocation": [4998.0, 2998.8],
    })


@pytest.fixture
def reconciled_df(enriched_portfolio_df):
    """A portfolio DataFrame after reconciliation."""
    df = enriched_portfolio_df.copy()
    df["existing_qty"] = [10, 5]
    df["pending_qty"] = [0, 0]
    df["target_qty"] = [28, 7]
    df["net_quantity"] = [18, 2]
    df["cancelled_orders"] = [0, 0]
    return df

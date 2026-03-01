"""Tests for src/extra_positions.py — compute_net_quantity and extra position handling."""

from unittest.mock import patch, MagicMock

import pandas as pd
from tests.conftest import (
    MockContract, MockContractDetails, MockTicker, MockPosition,
)
from src.cancel import CancelState
from src.extra_positions import (
    compute_net_quantity,
    _ExtraInfo,
    _build_extra_rows,
    _cancel_extra_orders,
    reconcile_extra_positions,
)


# ── compute_net_quantity ───────────────────────────────────────────


class TestComputeNetQuantity:
    def test_basic_buy(self):
        assert compute_net_quantity(100, 0, 0) == 100

    def test_basic_sell(self):
        assert compute_net_quantity(0, 50, 0) == -50

    def test_with_pending(self):
        assert compute_net_quantity(100, 50, 20) == 30

    def test_already_on_target(self):
        assert compute_net_quantity(50, 50, 0) == 0

    def test_already_covered_with_pending(self):
        assert compute_net_quantity(100, 50, 50) == 0

    def test_below_minimum_trading_amount(self):
        with patch("src.extra_positions.MINIMUM_TRADING_AMOUNT", 100):
            result = compute_net_quantity(
                target=1, existing=0, pending=0,
                limit_price=50.0, fx_rate=1.0, multiplier=1)
            assert result == 0  # $50 < $100 minimum

    def test_above_minimum_trading_amount(self):
        with patch("src.extra_positions.MINIMUM_TRADING_AMOUNT", 100):
            result = compute_net_quantity(
                target=10, existing=0, pending=0,
                limit_price=50.0, fx_rate=1.0, multiplier=1)
            assert result == 10  # $500 > $100

    def test_no_limit_price_skips_filter(self):
        with patch("src.extra_positions.MINIMUM_TRADING_AMOUNT", 100):
            result = compute_net_quantity(
                target=1, existing=0, pending=0,
                limit_price=None, fx_rate=1.0)
            assert result == 1

    def test_zero_limit_price_skips_filter(self):
        with patch("src.extra_positions.MINIMUM_TRADING_AMOUNT", 100):
            result = compute_net_quantity(
                target=1, existing=0, pending=0,
                limit_price=0.0, fx_rate=1.0)
            assert result == 1

    def test_no_fx_rate_skips_filter(self):
        with patch("src.extra_positions.MINIMUM_TRADING_AMOUNT", 100):
            result = compute_net_quantity(
                target=1, existing=0, pending=0,
                limit_price=50.0, fx_rate=None)
            assert result == 1

    def test_option_multiplier(self):
        with patch("src.extra_positions.MINIMUM_TRADING_AMOUNT", 100):
            result = compute_net_quantity(
                target=1, existing=0, pending=0,
                limit_price=1.0, fx_rate=1.0, multiplier=100)
            assert result == 1

    def test_option_below_minimum(self):
        with patch("src.extra_positions.MINIMUM_TRADING_AMOUNT", 200):
            result = compute_net_quantity(
                target=1, existing=0, pending=0,
                limit_price=1.0, fx_rate=1.0, multiplier=100)
            assert result == 0  # $100 < $200

    def test_foreign_currency(self):
        with patch("src.extra_positions.MINIMUM_TRADING_AMOUNT", 100):
            result = compute_net_quantity(
                target=10, existing=0, pending=0,
                limit_price=1000.0, fx_rate=150.0, multiplier=1)
            # USD value = 10 * 1000 / 150 = $66.67 < $100 → zeroed
            assert result == 0

    def test_negative_net_with_pending_sell(self):
        assert compute_net_quantity(10, 20, -5) == -5  # 10 - 20 - (-5) = -5

    def test_rounding(self):
        assert compute_net_quantity(10.4, 0.3, 0.2) == 10  # round(10.4)-round(0.3)-round(0.2)=10


# ── _build_extra_rows ──────────────────────────────────────────────


class TestBuildExtraRows:
    def test_builds_sell_row(self, mock_ib):
        info = {
            999: _ExtraInfo(
                contract=MockContract(conId=999),
                currency="USD", fx_rate=1.0,
                market_rules="", long_name="EXTRA STOCK",
            )
        }
        positions = {999: 50.0}
        snapshot = {999: {"bid": 100.0, "ask": 101.0, "last": 100.5,
                          "close": 99.0, "high": 102.0, "low": 98.0}}

        rows = _build_extra_rows(
            mock_ib, [999], positions, {}, {999: {"ticker": "XTR", "exchange": ""}},
            snapshot, info)

        assert len(rows) == 1
        assert rows[0]["existing_qty"] == 50
        assert rows[0]["target_qty"] == 0
        assert rows[0]["net_quantity"] <= 0  # should be negative (sell)

    def test_skips_zero_position(self, mock_ib):
        info = {
            999: _ExtraInfo(
                contract=MockContract(conId=999),
                currency="USD", fx_rate=1.0,
                market_rules="", long_name="ZERO POS",
            )
        }
        rows = _build_extra_rows(
            mock_ib, [999], {999: 0.0}, {}, {999: {"ticker": "Z"}},
            {}, info)
        assert len(rows) == 0

    def test_short_position_generates_buy(self, mock_ib):
        info = {
            999: _ExtraInfo(
                contract=MockContract(conId=999),
                currency="USD", fx_rate=1.0,
                market_rules="", long_name="SHORT STOCK",
            )
        }
        snapshot = {999: {"bid": 50.0, "ask": 51.0, "last": 50.5,
                          "close": 50.0, "high": 52.0, "low": 49.0}}

        rows = _build_extra_rows(
            mock_ib, [999], {999: -20.0}, {}, {999: {"ticker": "SHT", "exchange": ""}},
            snapshot, info)

        assert len(rows) == 1
        assert rows[0]["net_quantity"] >= 0  # should buy to close short


# ── _cancel_extra_orders ───────────────────────────────────────────


class TestCancelExtraOrders:
    def test_dry_run_keeps_all_orders(self, mock_ib):
        info = {
            999: _ExtraInfo(
                contract=MockContract(conId=999),
                currency="USD", fx_rate=1.0,
                market_rules="", long_name="TEST",
            )
        }
        orders = {999: [
            {"orderId": 1, "side": "BUY", "remainingQuantity": 10, "price": 100.0},
        ]}
        state = CancelState()

        pending, cancelled = _cancel_extra_orders(
            mock_ib, [999], orders, {999: {"exchange": ""}},
            info, True, state, dry_run=True)

        assert pending[999] == 10
        assert cancelled == 0

    def test_auto_cancel(self, mock_ib):
        info = {
            999: _ExtraInfo(
                contract=MockContract(conId=999),
                currency="USD", fx_rate=1.0,
                market_rules="", long_name="TEST",
            )
        }
        trade = MagicMock()
        trade.order = MagicMock()
        orders = {999: [
            {"orderId": 1, "side": "BUY", "remainingQuantity": 10,
             "price": 100.0, "trade": trade},
        ]}
        state = CancelState(confirm_all=True)

        with patch("src.extra_positions.is_exchange_open", return_value=True):
            pending, cancelled = _cancel_extra_orders(
                mock_ib, [999], orders, {999: {"exchange": "NYSE"}},
                info, True, state, dry_run=False)

        assert cancelled == 1
        assert 999 not in pending or pending.get(999, 0) == 0


# ── reconcile_extra_positions ──────────────────────────────────────


class TestReconcileExtraPositions:
    def test_ignores_named_positions(self, mock_ib):
        mock_ib.qualifyContracts.return_value = [
            MockContract(conId=999, symbol="ENP", currency="USD")]
        mock_ib.reqContractDetails.return_value = []
        mock_ib.reqTickers.return_value = []

        with patch("src.extra_positions.is_name_ignored", return_value=True):
            rows, cancelled = reconcile_extra_positions(
                ib=mock_ib,
                extra_conids=[999],
                positions={999: 50.0},
                position_meta={999: {"ticker": "ENP", "exchange": ""}},
                orders_by_conid={},
                all_exchanges=True,
                cancel_state=CancelState(),
            )

        assert rows == []
        assert cancelled == 0

    def test_empty_extra_conids(self, mock_ib):
        mock_ib.qualifyContracts.return_value = []
        mock_ib.reqTickers.return_value = []

        rows, cancelled = reconcile_extra_positions(
            ib=mock_ib,
            extra_conids=[],
            positions={},
            position_meta={},
            orders_by_conid={},
            all_exchanges=True,
            cancel_state=CancelState(),
        )
        assert rows == []

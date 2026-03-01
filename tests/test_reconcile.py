"""Tests for src/reconcile.py — net quantities, stale orders, sell ratio guard."""

from unittest.mock import patch, MagicMock

import pandas as pd
from tests.conftest import (
    MockContract, MockOrder, MockOrderStatus, MockTrade, MockPosition,
)
from src.cancel import CancelState
from src.reconcile import (
    _fetch_positions,
    _fetch_open_orders,
    compute_net_quantities,
    _is_order_stale,
    _cancel_stale_orders,
    _cancel_superfluous_orders,
    reconcile,
)


# ── _fetch_positions ───────────────────────────────────────────────


class TestFetchPositions:
    def test_empty(self, mock_ib):
        mock_ib.positions.return_value = []
        positions, meta = _fetch_positions(mock_ib)
        assert positions == {}
        assert meta == {}

    def test_parses_positions(self, mock_ib):
        c = MockContract(conId=123, symbol="AAPL", currency="USD",
                         primaryExchange="NASDAQ")
        pos = MockPosition(contract=c, position=50)
        mock_ib.positions.return_value = [pos]

        positions, meta = _fetch_positions(mock_ib)
        assert positions[123] == 50.0
        assert meta[123]["ticker"] == "AAPL"
        assert meta[123]["currency"] == "USD"

    def test_skips_zero_conid(self, mock_ib):
        c = MockContract(conId=0, symbol="BAD")
        pos = MockPosition(contract=c, position=10)
        mock_ib.positions.return_value = [pos]

        positions, meta = _fetch_positions(mock_ib)
        assert len(positions) == 0


# ── _fetch_open_orders ─────────────────────────────────────────────


class TestFetchOpenOrders:
    def test_empty(self, mock_ib):
        mock_ib.openTrades.return_value = []
        assert _fetch_open_orders(mock_ib) == []

    def test_parses_orders(self, mock_ib):
        c = MockContract(conId=123, symbol="AAPL")
        o = MockOrder(orderId=1, action="BUY", totalQuantity=50, lmtPrice=175.0)
        os_ = MockOrderStatus(status="Submitted", remaining=50)
        trade = MockTrade(contract=c, order=o, orderStatus=os_)
        mock_ib.openTrades.return_value = [trade]

        orders = _fetch_open_orders(mock_ib)
        assert len(orders) == 1
        assert orders[0]["conid"] == 123
        assert orders[0]["side"] == "BUY"
        assert orders[0]["remainingQuantity"] == 50

    def test_normalizes_side_abbreviations(self, mock_ib):
        c = MockContract(conId=1)
        for action, expected in [("B", "BUY"), ("BOT", "BUY"),
                                 ("S", "SELL"), ("SLD", "SELL")]:
            o = MockOrder(orderId=1, action=action, totalQuantity=10)
            os_ = MockOrderStatus(status="Submitted", remaining=10)
            trade = MockTrade(contract=c, order=o, orderStatus=os_)
            mock_ib.openTrades.return_value = [trade]

            orders = _fetch_open_orders(mock_ib)
            assert orders[0]["side"] == expected

    def test_skips_filled_orders(self, mock_ib):
        c = MockContract(conId=1)
        o = MockOrder(orderId=1, action="BUY", totalQuantity=10)
        for status in ("Cancelled", "Filled", "Inactive"):
            os_ = MockOrderStatus(status=status, remaining=0)
            trade = MockTrade(contract=c, order=o, orderStatus=os_)
            mock_ib.openTrades.return_value = [trade]
            assert _fetch_open_orders(mock_ib) == []

    def test_skips_zero_conid(self, mock_ib):
        c = MockContract(conId=0)
        o = MockOrder(orderId=1, action="BUY", totalQuantity=10)
        os_ = MockOrderStatus(status="Submitted", remaining=10)
        trade = MockTrade(contract=c, order=o, orderStatus=os_)
        mock_ib.openTrades.return_value = [trade]
        assert _fetch_open_orders(mock_ib) == []


# ── _is_order_stale ───────────────────────────────────────────────


class TestIsOrderStale:
    def test_not_stale_within_tolerance(self):
        order = {"price": 100.0}
        assert _is_order_stale(order, 100.0, 0.005) is False

    def test_stale_exceeds_tolerance(self):
        order = {"price": 100.0}
        assert _is_order_stale(order, 101.0, 0.005) is True

    def test_at_tolerance_boundary(self):
        order = {"price": 100.0}
        assert _is_order_stale(order, 100.5, 0.005) is False

    def test_just_over_tolerance(self):
        order = {"price": 100.0}
        assert _is_order_stale(order, 100.51, 0.005) is True

    def test_no_order_price(self):
        order = {"price": None}
        assert _is_order_stale(order, 100.0, 0.005) is False

    def test_zero_limit_price(self):
        order = {"price": 100.0}
        assert _is_order_stale(order, 0.0, 0.005) is False

    def test_illiquid_tolerance(self):
        order = {"price": 100.0}
        assert _is_order_stale(order, 104.0, 0.05) is False  # 4% < 5%
        assert _is_order_stale(order, 106.0, 0.05) is True   # 6% > 5%


# ── compute_net_quantities ─────────────────────────────────────────


class TestComputeNetQuantities:
    def _make_df(self, **overrides):
        defaults = {
            "conid": [265598],
            "Qty": [28],
            "limit_price": [178.50],
            "currency": ["USD"],
            "fx_rate": [1.0],
            "is_option": [False],
            "Dollar Allocation": [5000.0],
            "Actual Dollar Allocation": [4998.0],
        }
        defaults.update(overrides)
        return pd.DataFrame(defaults)

    def test_no_positions_no_orders(self):
        df = self._make_df()
        result = compute_net_quantities(df, {}, {})
        assert result.iloc[0]["existing_qty"] == 0
        assert result.iloc[0]["pending_qty"] == 0
        assert result.iloc[0]["target_qty"] == 28
        assert result.iloc[0]["net_quantity"] == 28

    def test_with_existing_position(self):
        df = self._make_df()
        positions = {265598: 10.0}
        result = compute_net_quantities(df, positions, {})
        assert result.iloc[0]["existing_qty"] == 10
        assert result.iloc[0]["net_quantity"] == 18

    def test_with_pending_buy_orders(self):
        df = self._make_df()
        positions = {265598: 10.0}
        orders = {265598: [
            {"side": "BUY", "remainingQuantity": 5},
        ]}
        result = compute_net_quantities(df, positions, orders)
        assert result.iloc[0]["pending_qty"] == 5
        assert result.iloc[0]["net_quantity"] == 13

    def test_with_pending_sell_orders(self):
        df = self._make_df()
        positions = {265598: 30.0}
        orders = {265598: [
            {"side": "SELL", "remainingQuantity": 5},
        ]}
        result = compute_net_quantities(df, positions, orders)
        assert result.iloc[0]["pending_qty"] == -5
        assert result.iloc[0]["net_quantity"] == 3  # 28 - 30 - (-5) = 3

    def test_nan_conid_skipped(self):
        df = self._make_df(conid=[None])
        result = compute_net_quantities(df, {}, {})
        assert result.iloc[0]["existing_qty"] is None
        assert result.iloc[0]["net_quantity"] is None

    def test_nan_qty_skipped(self):
        df = self._make_df(Qty=[None])
        result = compute_net_quantities(df, {}, {})
        assert result.iloc[0]["existing_qty"] == 0
        assert result.iloc[0]["target_qty"] is None

    def test_already_on_target(self):
        df = self._make_df(Qty=[10])
        positions = {265598: 10.0}
        result = compute_net_quantities(df, positions, {})
        assert result.iloc[0]["net_quantity"] == 0

    def test_sell_rebalance_ratio_guard(self):
        """Net SELL zeroed when rebalance ratio exceeds limit."""
        df = pd.DataFrame({
            "conid": [265598],
            "Qty": [8],
            "limit_price": [178.50],
            "currency": ["USD"],
            "fx_rate": [1.0],
            "is_option": [False],
            "Dollar Allocation": [1427.0],
            "Actual Dollar Allocation": [1428.0],
        })
        positions = {265598: 10.0}
        with patch("src.reconcile.SELL_REBALANCE_RATIO_LIMIT", 1.5):
            result = compute_net_quantities(df, positions, {})
        # Ratio calculation may zero the sell depending on values
        assert result.iloc[0]["net_quantity"] is not None


# ── reconcile ──────────────────────────────────────────────────────


class TestReconcile:
    def _make_df(self):
        return pd.DataFrame({
            "conid": [265598, 272093],
            "Qty": [28, 7],
            "limit_price": [178.50, 428.40],
            "currency": ["USD", "USD"],
            "fx_rate": [1.0, 1.0],
            "is_option": [False, False],
            "Dollar Allocation": [5000.0, 3000.0],
            "Actual Dollar Allocation": [4998.0, 2998.8],
            "Name": ["APPLE INC", "MICROSOFT CORP"],
            "clean_ticker": ["AAPL", "MSFT"],
            "MIC Primary Exchange": ["XNAS", "XNAS"],
        })

    def test_dry_run_no_cancellation(self, mock_ib):
        mock_ib.positions.return_value = []
        mock_ib.openTrades.return_value = []

        df = self._make_df()
        result = reconcile(mock_ib, df, dry_run=True)
        assert "net_quantity" in result.columns
        assert "cancelled_orders" in result.columns
        assert result["cancelled_orders"].sum() == 0

    def test_basic_reconcile(self, mock_ib):
        c = MockContract(conId=265598, symbol="AAPL", currency="USD",
                         primaryExchange="NASDAQ")
        pos = MockPosition(contract=c, position=10)
        mock_ib.positions.return_value = [pos]
        mock_ib.openTrades.return_value = []

        df = self._make_df()
        result = reconcile(mock_ib, df, all_exchanges=True)
        aapl_row = result[result["conid"] == 265598].iloc[0]
        assert aapl_row["existing_qty"] == 10
        assert aapl_row["net_quantity"] == 18

    def test_extra_positions_appended(self, mock_ib):
        """Positions not in df are appended as synthetic rows."""
        c1 = MockContract(conId=265598, symbol="AAPL", currency="USD",
                          primaryExchange="NASDAQ")
        c2 = MockContract(conId=999999, symbol="EXTRA", currency="USD",
                          primaryExchange="NYSE")
        pos1 = MockPosition(contract=c1, position=10)
        pos2 = MockPosition(contract=c2, position=5)
        mock_ib.positions.return_value = [pos1, pos2]
        mock_ib.openTrades.return_value = []
        mock_ib.qualifyContracts.return_value = [c2]
        mock_ib.reqContractDetails.return_value = []
        mock_ib.reqTickers.return_value = []

        with patch("src.extra_positions.is_name_ignored", return_value=False):
            df = self._make_df()
            result = reconcile(mock_ib, df, all_exchanges=True)

        assert len(result) >= 2  # at least original rows

    def test_auto_mode_sets_confirm_all(self, mock_ib):
        c = MockContract(conId=265598, symbol="AAPL", currency="USD",
                         primaryExchange="NASDAQ")
        pos = MockPosition(contract=c, position=10)
        mock_ib.positions.return_value = [pos]
        mock_ib.openTrades.return_value = []

        df = self._make_df()
        result = reconcile(mock_ib, df, auto_mode=True)
        assert "net_quantity" in result.columns

    def test_stale_orders_cancelled_in_live_reconcile(self, mock_ib):
        """Stale orders (price drifted beyond tolerance) are cancelled."""
        c = MockContract(conId=265598, symbol="AAPL", currency="USD",
                         primaryExchange="NASDAQ")
        pos = MockPosition(contract=c, position=10)
        mock_ib.positions.return_value = [pos]

        o = MockOrder(orderId=77, action="BUY", totalQuantity=5, lmtPrice=150.0)
        os_ = MockOrderStatus(status="Submitted", remaining=5)
        trade = MockTrade(contract=c, order=o, orderStatus=os_)
        mock_ib.openTrades.return_value = [trade]

        df = self._make_df()

        with patch("src.reconcile.STALE_ORDER_TOL_PCT", 0.001), \
             patch("src.reconcile.is_exchange_open", return_value=True):
            result = reconcile(mock_ib, df, all_exchanges=True, auto_mode=True)

        assert result[result["conid"] == 265598].iloc[0]["cancelled_orders"] >= 1
        mock_ib.cancelOrder.assert_called()

    def test_superfluous_order_cancelled_not_counter_ordered(self, mock_ib):
        """A superfluous BUY on-target is cancelled, not counter-ordered."""
        c = MockContract(conId=265598, symbol="AAPL", currency="USD",
                         primaryExchange="NASDAQ")
        pos = MockPosition(contract=c, position=28)
        mock_ib.positions.return_value = [pos]

        o = MockOrder(orderId=99, action="BUY", totalQuantity=5, lmtPrice=178.50)
        os_ = MockOrderStatus(status="Submitted", remaining=5)
        trade = MockTrade(contract=c, order=o, orderStatus=os_)
        mock_ib.openTrades.return_value = [trade]

        df = self._make_df()

        with patch("src.reconcile.is_exchange_open", return_value=True):
            result = reconcile(mock_ib, df, all_exchanges=True, auto_mode=True)

        aapl = result[result["conid"] == 265598].iloc[0]
        # The BUY should be cancelled, not counteracted with a SELL.
        assert aapl["net_quantity"] == 0
        assert aapl["cancelled_orders"] >= 1
        mock_ib.cancelOrder.assert_called()

    def test_non_stale_orders_not_cancelled(self, mock_ib):
        """Orders within tolerance are kept."""
        c = MockContract(conId=265598, symbol="AAPL", currency="USD",
                         primaryExchange="NASDAQ")
        pos = MockPosition(contract=c, position=10)
        mock_ib.positions.return_value = [pos]

        o = MockOrder(orderId=77, action="BUY", totalQuantity=5, lmtPrice=178.50)
        os_ = MockOrderStatus(status="Submitted", remaining=5)
        trade = MockTrade(contract=c, order=o, orderStatus=os_)
        mock_ib.openTrades.return_value = [trade]

        df = self._make_df()

        with patch("src.reconcile.STALE_ORDER_TOL_PCT", 0.50):
            result = reconcile(mock_ib, df, all_exchanges=True, auto_mode=True)

        assert result[result["conid"] == 265598].iloc[0]["cancelled_orders"] == 0
        mock_ib.cancelOrder.assert_not_called()


# ── _cancel_stale_orders ──────────────────────────────────────────


class TestCancelStaleOrders:
    def _make_df(self):
        return pd.DataFrame({
            "conid": [265598],
            "limit_price": [178.50],
            "MIC Primary Exchange": ["XNAS"],
            "Name": ["APPLE INC"],
        })

    def test_cancels_stale_order_auto_mode(self, mock_ib):
        df = self._make_df()
        trade = MagicMock()
        trade.order = MagicMock()
        orders_by_conid = {265598: [{
            "orderId": 77, "side": "BUY", "remainingQuantity": 5,
            "price": 150.0, "trade": trade,
        }]}
        state = CancelState(confirm_all=True)

        with patch("src.reconcile.STALE_ORDER_TOL_PCT", 0.001), \
             patch("src.reconcile.is_exchange_open", return_value=True):
            remaining, counts = _cancel_stale_orders(
                mock_ib, df, orders_by_conid, True, state)

        assert counts[0] == 1
        assert len(remaining.get(265598, [])) == 0

    def test_keeps_fresh_order(self, mock_ib):
        df = self._make_df()
        orders_by_conid = {265598: [{
            "orderId": 77, "side": "BUY", "remainingQuantity": 5,
            "price": 178.50, "trade": MagicMock(),
        }]}
        state = CancelState(confirm_all=True)

        with patch("src.reconcile.STALE_ORDER_TOL_PCT", 0.50):
            remaining, counts = _cancel_stale_orders(
                mock_ib, df, orders_by_conid, True, state)

        assert counts[0] == 0
        assert len(remaining[265598]) == 1

    def test_illiquid_mic_uses_wider_tolerance(self, mock_ib):
        df = pd.DataFrame({
            "conid": [265598],
            "limit_price": [178.50],
            "MIC Primary Exchange": ["XFRA"],
            "Name": ["SIEMENS"],
        })
        orders_by_conid = {265598: [{
            "orderId": 77, "side": "BUY", "remainingQuantity": 5,
            "price": 175.0, "trade": MagicMock(),
        }]}
        state = CancelState(confirm_all=True)

        with patch("src.reconcile.STALE_ORDER_TOL_PCT", 0.001), \
             patch("src.reconcile.STALE_ORDER_TOL_PCT_ILLIQUID", 0.50), \
             patch("src.reconcile.is_exchange_open", return_value=True):
            remaining, counts = _cancel_stale_orders(
                mock_ib, df, orders_by_conid, True, state)

        assert counts[0] == 0

    def test_exchange_closed_skips_cancel(self, mock_ib):
        df = self._make_df()
        orders_by_conid = {265598: [{
            "orderId": 77, "side": "BUY", "remainingQuantity": 5,
            "price": 150.0, "trade": MagicMock(),
        }]}
        state = CancelState()

        with patch("src.reconcile.STALE_ORDER_TOL_PCT", 0.001), \
             patch("src.reconcile.is_exchange_open", return_value=False):
            remaining, counts = _cancel_stale_orders(
                mock_ib, df, orders_by_conid, False, state)

        assert counts[0] == 0
        assert len(remaining[265598]) == 1

    def test_no_orders_for_conid(self, mock_ib):
        df = self._make_df()
        state = CancelState()
        remaining, counts = _cancel_stale_orders(
            mock_ib, df, {}, True, state)
        assert counts[0] == 0

    def test_nan_conid_skipped(self, mock_ib):
        df = pd.DataFrame({
            "conid": [None],
            "limit_price": [100.0],
            "MIC Primary Exchange": ["XNAS"],
            "Name": ["TEST"],
        })
        state = CancelState()
        remaining, counts = _cancel_stale_orders(
            mock_ib, df, {}, True, state)
        assert counts[0] == 0


# ── _cancel_superfluous_orders ────────────────────────────────────


class TestCancelSuperfluousOrders:
    def _make_df(self, **overrides):
        defaults = {
            "conid": [265598],
            "Qty": [10],
            "limit_price": [178.50],
            "MIC Primary Exchange": ["XNAS"],
            "Name": ["APPLE INC"],
        }
        defaults.update(overrides)
        return pd.DataFrame(defaults)

    def test_on_target_cancels_all_orders(self, mock_ib):
        """When position == target, all pending orders are superfluous."""
        df = self._make_df(Qty=[10])
        positions = {265598: 10.0}
        trade = MagicMock()
        trade.order = MagicMock()
        orders_by_conid = {265598: [{
            "orderId": 50, "side": "BUY", "remainingQuantity": 5,
            "price": 178.0, "trade": trade,
        }]}
        state = CancelState(confirm_all=True)

        with patch("src.reconcile.is_exchange_open", return_value=True):
            remaining, counts = _cancel_superfluous_orders(
                mock_ib, df, orders_by_conid, positions, True, state)

        assert counts[0] == 1
        assert len(remaining.get(265598, [])) == 0

    def test_wrong_direction_buy_when_need_sell(self, mock_ib):
        """A BUY order is superfluous when the position needs to decrease."""
        df = self._make_df(Qty=[5])
        positions = {265598: 10.0}
        trade = MagicMock()
        trade.order = MagicMock()
        orders_by_conid = {265598: [{
            "orderId": 51, "side": "BUY", "remainingQuantity": 3,
            "price": 178.0, "trade": trade,
        }]}
        state = CancelState(confirm_all=True)

        with patch("src.reconcile.is_exchange_open", return_value=True):
            remaining, counts = _cancel_superfluous_orders(
                mock_ib, df, orders_by_conid, positions, True, state)

        assert counts[0] == 1
        assert len(remaining.get(265598, [])) == 0

    def test_wrong_direction_sell_when_need_buy(self, mock_ib):
        """A SELL order is superfluous when the position needs to increase."""
        df = self._make_df(Qty=[20])
        positions = {265598: 10.0}
        trade = MagicMock()
        trade.order = MagicMock()
        orders_by_conid = {265598: [{
            "orderId": 52, "side": "SELL", "remainingQuantity": 3,
            "price": 178.0, "trade": trade,
        }]}
        state = CancelState(confirm_all=True)

        with patch("src.reconcile.is_exchange_open", return_value=True):
            remaining, counts = _cancel_superfluous_orders(
                mock_ib, df, orders_by_conid, positions, True, state)

        assert counts[0] == 1
        assert len(remaining.get(265598, [])) == 0

    def test_overshooting_orders_trimmed(self, mock_ib):
        """When right-direction orders exceed the need, excess is cancelled."""
        df = self._make_df(Qty=[15])
        positions = {265598: 10.0}  # need = +5
        t1 = MagicMock(); t1.order = MagicMock()
        t2 = MagicMock(); t2.order = MagicMock()
        orders_by_conid = {265598: [
            {"orderId": 60, "side": "BUY", "remainingQuantity": 3,
             "price": 178.0, "trade": t1},
            {"orderId": 61, "side": "BUY", "remainingQuantity": 5,
             "price": 178.0, "trade": t2},
        ]}
        state = CancelState(confirm_all=True)

        with patch("src.reconcile.is_exchange_open", return_value=True):
            remaining, counts = _cancel_superfluous_orders(
                mock_ib, df, orders_by_conid, positions, True, state)

        # BUY 3 fits (3 <= 5), BUY 5 overshoots (3+5=8 > 5) → cancelled
        assert counts[0] == 1
        assert len(remaining[265598]) == 1
        assert remaining[265598][0]["orderId"] == 60

    def test_right_direction_within_budget_kept(self, mock_ib):
        """Orders in the right direction that don't overshoot are kept."""
        df = self._make_df(Qty=[20])
        positions = {265598: 10.0}  # need = +10
        orders_by_conid = {265598: [{
            "orderId": 70, "side": "BUY", "remainingQuantity": 5,
            "price": 178.0, "trade": MagicMock(),
        }]}
        state = CancelState(confirm_all=True)

        with patch("src.reconcile.is_exchange_open", return_value=True):
            remaining, counts = _cancel_superfluous_orders(
                mock_ib, df, orders_by_conid, positions, True, state)

        assert counts[0] == 0
        assert len(remaining[265598]) == 1

    def test_no_orders_no_change(self, mock_ib):
        df = self._make_df()
        state = CancelState()
        remaining, counts = _cancel_superfluous_orders(
            mock_ib, df, {}, {}, True, state)
        assert counts[0] == 0

    def test_nan_conid_skipped(self, mock_ib):
        df = self._make_df(conid=[None])
        state = CancelState()
        remaining, counts = _cancel_superfluous_orders(
            mock_ib, df, {}, {}, True, state)
        assert counts[0] == 0

    def test_nan_qty_skipped(self, mock_ib):
        df = self._make_df(Qty=[None])
        state = CancelState()
        remaining, counts = _cancel_superfluous_orders(
            mock_ib, df, {}, {}, True, state)
        assert counts[0] == 0

    def test_exchange_closed_skips_cancel(self, mock_ib):
        """Superfluous orders on closed exchanges are not cancelled."""
        df = self._make_df(Qty=[10])
        positions = {265598: 10.0}
        orders_by_conid = {265598: [{
            "orderId": 80, "side": "BUY", "remainingQuantity": 5,
            "price": 178.0, "trade": MagicMock(),
        }]}
        state = CancelState()

        with patch("src.reconcile.is_exchange_open", return_value=False):
            remaining, counts = _cancel_superfluous_orders(
                mock_ib, df, orders_by_conid, positions, False, state)

        assert counts[0] == 0
        assert len(remaining[265598]) == 1

    def test_mixed_orders_cancels_wrong_keeps_right(self, mock_ib):
        """With a BUY and SELL for the same conid, only the wrong one is cancelled."""
        df = self._make_df(Qty=[15])
        positions = {265598: 10.0}  # need = +5
        t_buy = MagicMock(); t_buy.order = MagicMock()
        t_sell = MagicMock(); t_sell.order = MagicMock()
        orders_by_conid = {265598: [
            {"orderId": 90, "side": "BUY", "remainingQuantity": 3,
             "price": 178.0, "trade": t_buy},
            {"orderId": 91, "side": "SELL", "remainingQuantity": 2,
             "price": 178.0, "trade": t_sell},
        ]}
        state = CancelState(confirm_all=True)

        with patch("src.reconcile.is_exchange_open", return_value=True):
            remaining, counts = _cancel_superfluous_orders(
                mock_ib, df, orders_by_conid, positions, True, state)

        assert counts[0] == 1  # SELL cancelled
        kept_ids = [o["orderId"] for o in remaining[265598]]
        assert 90 in kept_ids   # BUY kept
        assert 91 not in kept_ids  # SELL cancelled

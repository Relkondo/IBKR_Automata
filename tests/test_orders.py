"""Tests for src/orders.py — formatting, params, placement loop."""

from unittest.mock import patch, MagicMock

import pandas as pd
import pytest
from tests.conftest import (
    MockContract, MockContractDetails, MockOrder,
    MockOrderStatus, MockTrade, MockLogEntry,
    MockPriceIncrement,
)
from src.orders import (
    _format_currency,
    _compute_usd_amount,
    _format_order_details,
    _prepare_order_params,
    _place_order,
    _place_single_order,
    _handle_tick_error,
    _prompt_modify,
    _order_summary,
    _AutoState,
    _OrderParams,
    cancel_all_orders,
    run_order_loop,
    print_order_summary,
)


# ── _format_currency ───────────────────────────────────────────────


class TestFormatCurrency:
    def test_usd(self):
        assert _format_currency(1234.56) == "$1,234.56"
        assert _format_currency(1234.56, "USD") == "$1,234.56"

    def test_foreign(self):
        assert _format_currency(1234.56, "EUR") == "1,234.56 EUR"

    def test_zero(self):
        assert _format_currency(0) == "$0.00"

    def test_negative(self):
        assert _format_currency(-500.0) == "$-500.00"


# ── _compute_usd_amount ───────────────────────────────────────────


class TestComputeUsdAmount:
    def test_usd_stock(self):
        assert _compute_usd_amount(100.0, 10, 1, 1.0) == 1000.0

    def test_option(self):
        assert _compute_usd_amount(5.0, 2, 100, 1.0) == 1000.0

    def test_foreign_currency(self):
        result = _compute_usd_amount(1000.0, 10, 1, 150.0)
        assert result == round(10000.0 / 150.0, 2)

    def test_zero_fx(self):
        result = _compute_usd_amount(100.0, 10, 1, 0.0)
        assert result == 1000.0  # fx <= 0 → local_amount returned as-is


# ── _prepare_order_params ──────────────────────────────────────────


class TestPrepareOrderParams:
    def _make_row(self, **overrides):
        defaults = {
            "Name": "APPLE INC",
            "clean_ticker": "AAPL",
            "conid": 265598,
            "Dollar Allocation": 5000.0,
            "limit_price": 178.50,
            "currency": "USD",
            "fx_rate": 1.0,
            "is_option": False,
            "MIC Primary Exchange": "XNAS",
            "net_quantity": None,
        }
        defaults.update(overrides)
        return pd.Series(defaults)

    def test_basic_buy(self, mock_ib):
        mock_ib.reqContractDetails.return_value = []
        row = self._make_row()
        state = _AutoState()
        params = _prepare_order_params(mock_ib, row, 0, 10, state)
        assert params is not None
        assert params.side == "BUY"
        assert params.quantity > 0
        assert params.conid == 265598

    def test_sell_from_negative_allocation(self, mock_ib):
        mock_ib.reqContractDetails.return_value = []
        row = self._make_row(**{"Dollar Allocation": -3000.0})
        state = _AutoState()
        params = _prepare_order_params(mock_ib, row, 0, 10, state)
        assert params is not None
        assert params.side == "SELL"

    def test_with_net_quantity(self, mock_ib):
        mock_ib.reqContractDetails.return_value = []
        row = self._make_row(net_quantity=15)
        state = _AutoState()
        params = _prepare_order_params(mock_ib, row, 0, 10, state)
        assert params is not None
        assert params.side == "BUY"
        assert params.quantity == 15

    def test_net_quantity_sell(self, mock_ib):
        mock_ib.reqContractDetails.return_value = []
        row = self._make_row(net_quantity=-5)
        state = _AutoState()
        params = _prepare_order_params(mock_ib, row, 0, 10, state)
        assert params is not None
        assert params.side == "SELL"
        assert params.quantity == 5

    def test_net_quantity_zero_skips(self, mock_ib):
        row = self._make_row(net_quantity=0)
        state = _AutoState()
        params = _prepare_order_params(mock_ib, row, 0, 10, state)
        assert params is None

    def test_missing_conid_skips(self, mock_ib):
        row = self._make_row(conid=None)
        state = _AutoState()
        params = _prepare_order_params(mock_ib, row, 0, 10, state)
        assert params is None

    def test_missing_limit_price_skips(self, mock_ib):
        row = self._make_row(limit_price=None)
        state = _AutoState()
        params = _prepare_order_params(mock_ib, row, 0, 10, state)
        assert params is None

    def test_zero_limit_price_skips(self, mock_ib):
        row = self._make_row(limit_price=0.0)
        state = _AutoState()
        params = _prepare_order_params(mock_ib, row, 0, 10, state)
        assert params is None

    def test_missing_dollar_allocation_skips(self, mock_ib):
        row = self._make_row(**{"Dollar Allocation": None})
        state = _AutoState()
        params = _prepare_order_params(mock_ib, row, 0, 10, state)
        assert params is None

    def test_no_fx_rate_skips(self, mock_ib):
        row = self._make_row(currency="JPY", fx_rate=None)
        state = _AutoState()
        params = _prepare_order_params(mock_ib, row, 0, 10, state)
        assert params is None

    def test_auto_skip_exchange(self, mock_ib):
        row = self._make_row()
        state = _AutoState()
        state.skip_exchanges.add("XNAS")
        params = _prepare_order_params(mock_ib, row, 0, 10, state)
        assert params is None

    def test_zero_computed_quantity_skips(self, mock_ib):
        row = self._make_row(**{
            "Dollar Allocation": 1.0,  # very small
            "limit_price": 10000.0,    # very expensive
        })
        state = _AutoState()
        params = _prepare_order_params(mock_ib, row, 0, 10, state)
        assert params is None  # quantity rounds to 0

    def test_option_multiplier(self, mock_ib):
        mock_ib.reqContractDetails.return_value = []
        row = self._make_row(is_option=True, **{
            "Dollar Allocation": 5000.0,
            "limit_price": 5.0,
        })
        state = _AutoState()
        params = _prepare_order_params(mock_ib, row, 0, 10, state)
        assert params is not None
        assert params.multiplier == 100
        assert params.is_option is True


# ── _format_order_details ──────────────────────────────────────────


class TestFormatOrderDetails:
    def test_basic_output(self):
        row = pd.Series({
            "existing_qty": 0, "pending_qty": 0, "target_qty": 28,
        })
        p = _OrderParams(
            row=row, order_contract=MockContract(),
            idx_label="[1/10]", name="APPLE INC", ticker="AAPL",
            conid=265598, limit_price=178.50, quantity=28,
            side="BUY", multiplier=1, ccy_label="USD",
            is_foreign=False, fx=1.0, dollar_alloc=5000.0,
            net_qty_raw=None, mic_str="XNAS", is_option=False,
        )
        output = _format_order_details(p)
        assert "APPLE INC" in output
        assert "BUY" in output
        assert "178.50" in output

    def test_reconciled_order(self):
        row = pd.Series({
            "existing_qty": 10, "pending_qty": 0, "target_qty": 28,
        })
        p = _OrderParams(
            row=row, order_contract=MockContract(),
            idx_label="[1/10]", name="APPLE", ticker="AAPL",
            conid=265598, limit_price=178.50, quantity=18,
            side="BUY", multiplier=1, ccy_label="USD",
            is_foreign=False, fx=1.0, dollar_alloc=5000.0,
            net_qty_raw=18, mic_str="XNAS", is_option=False,
        )
        output = _format_order_details(p)
        assert "reconciliation" in output
        assert "Existing position" in output

    def test_foreign_currency_shows_usd(self):
        row = pd.Series({})
        p = _OrderParams(
            row=row, order_contract=MockContract(),
            idx_label="[1/1]", name="SONY", ticker="SONY",
            conid=1, limit_price=15000.0, quantity=5,
            side="BUY", multiplier=1, ccy_label="JPY",
            is_foreign=True, fx=150.0, dollar_alloc=500.0,
            net_qty_raw=None, mic_str="XFRA", is_option=False,
        )
        output = _format_order_details(p)
        assert "JPY" in output
        assert "Amount (USD)" in output

    def test_option_label(self):
        row = pd.Series({})
        p = _OrderParams(
            row=row, order_contract=MockContract(),
            idx_label="[1/1]", name="QQQ PUT", ticker="QQQ",
            conid=1, limit_price=5.0, quantity=2,
            side="BUY", multiplier=100, ccy_label="USD",
            is_foreign=False, fx=1.0, dollar_alloc=1000.0,
            net_qty_raw=None, mic_str="XNAS", is_option=True,
        )
        output = _format_order_details(p)
        assert "(OPTION)" in output


# ── _order_summary ─────────────────────────────────────────────────


class TestOrderSummary:
    def test_basic(self):
        row = pd.Series({})
        p = _OrderParams(
            row=row, order_contract=MockContract(),
            idx_label="[1/1]", name="AAPL", ticker="AAPL",
            conid=265598, limit_price=178.50, quantity=28,
            side="BUY", multiplier=1, ccy_label="USD",
            is_foreign=False, fx=1.0, dollar_alloc=5000.0,
            net_qty_raw=None, mic_str="XNAS", is_option=False,
        )
        summary = _order_summary(p, reason="test")
        assert summary["ticker"] == "AAPL"
        assert summary["reason"] == "test"
        assert summary["usd_amount"] > 0


# ── cancel_all_orders ──────────────────────────────────────────────


class TestCancelAllOrders:
    def test_no_orders(self, mock_ib):
        mock_ib.openTrades.return_value = []
        cancel_all_orders(mock_ib)
        # Should not raise

    def test_cancels_active_orders(self, mock_ib):
        c = MockContract(conId=1, symbol="AAPL", primaryExchange="NASDAQ")
        o = MockOrder(orderId=42, action="BUY", totalQuantity=10, lmtPrice=175.0)
        os_ = MockOrderStatus(status="Submitted")
        trade = MockTrade(contract=c, order=o, orderStatus=os_)
        mock_ib.openTrades.return_value = [trade]

        with patch("src.orders.is_exchange_open", return_value=True):
            cancel_all_orders(mock_ib, all_exchanges=True, auto_mode=True)

        mock_ib.cancelOrder.assert_called()

    def test_skips_filled_orders(self, mock_ib):
        c = MockContract(conId=1, symbol="AAPL")
        o = MockOrder(orderId=42, action="BUY", totalQuantity=10)
        os_ = MockOrderStatus(status="Filled")
        trade = MockTrade(contract=c, order=o, orderStatus=os_)
        mock_ib.openTrades.return_value = [trade]

        cancel_all_orders(mock_ib, all_exchanges=True, auto_mode=True)
        mock_ib.cancelOrder.assert_not_called()


# ── run_order_loop ─────────────────────────────────────────────────


class TestRunOrderLoop:
    def test_empty_df(self, mock_ib):
        df = pd.DataFrame(columns=["conid", "limit_price", "Dollar Allocation"])
        result = run_order_loop(mock_ib, df)
        assert result == []

    def test_skips_rows_with_missing_data(self, mock_ib):
        df = pd.DataFrame({
            "conid": [None],
            "limit_price": [None],
            "Dollar Allocation": [None],
            "Name": ["TEST"],
            "clean_ticker": ["TST"],
            "currency": ["USD"],
            "fx_rate": [1.0],
            "is_option": [False],
            "MIC Primary Exchange": ["XNAS"],
            "net_quantity": [None],
        })
        result = run_order_loop(mock_ib, df)
        assert result == []

    def test_auto_mode_places_order(self, mock_ib):
        trade = MockTrade(
            contract=MockContract(conId=1),
            order=MockOrder(orderId=99),
            orderStatus=MockOrderStatus(status="Submitted"),
        )
        mock_ib.placeOrder.return_value = trade
        mock_ib.reqContractDetails.return_value = []

        df = pd.DataFrame({
            "conid": [265598],
            "limit_price": [178.50],
            "Dollar Allocation": [500.0],
            "Name": ["APPLE INC"],
            "clean_ticker": ["AAPL"],
            "currency": ["USD"],
            "fx_rate": [1.0],
            "is_option": [False],
            "MIC Primary Exchange": ["XNAS"],
            "net_quantity": [3],
        })

        with patch("src.telegram.notify_flagged_orders"):
            result = run_order_loop(mock_ib, df, auto_mode=True)
        assert len(result) == 1
        assert result[0]["ticker"] == "AAPL"

    def test_quit_signal_stops_loop(self, mock_ib):
        mock_ib.reqContractDetails.return_value = []
        df = pd.DataFrame({
            "conid": [1, 2],
            "limit_price": [100.0, 200.0],
            "Dollar Allocation": [1000.0, 2000.0],
            "Name": ["A", "B"],
            "clean_ticker": ["A", "B"],
            "currency": ["USD", "USD"],
            "fx_rate": [1.0, 1.0],
            "is_option": [False, False],
            "MIC Primary Exchange": ["XNAS", "XNAS"],
            "net_quantity": [10, 20],
        })

        with patch("builtins.input", return_value="Q"):
            result = run_order_loop(mock_ib, df)
        assert len(result) == 0


# ── print_order_summary ───────────────────────────────────────────


class TestPrintOrderSummary:
    def test_no_orders(self, capsys):
        print_order_summary([])
        output = capsys.readouterr().out
        assert "No orders were placed" in output

    def test_with_orders(self, capsys):
        orders = [{
            "ticker": "AAPL", "name": "APPLE INC",
            "conid": 265598, "side": "BUY",
            "quantity": 28, "limit_price": 178.50,
            "order_id": 99, "usd_amount": 4998.0,
        }]
        print_order_summary(orders)
        output = capsys.readouterr().out
        assert "AAPL" in output
        assert "BUY" in output
        assert "ORDER SUMMARY" in output
        assert "Total orders placed: 1" in output


# ── _place_order ──────────────────────────────────────────────────


class TestPlaceOrder:
    def test_returns_trade_and_empty_error(self, mock_ib):
        trade = MockTrade(
            contract=MockContract(conId=1),
            order=MockOrder(orderId=1),
            orderStatus=MockOrderStatus(status="Submitted"),
        )
        mock_ib.placeOrder.return_value = trade
        result_trade, error = _place_order(
            mock_ib, MockContract(conId=1), MagicMock())
        assert result_trade is trade
        assert error == ""

    def test_calls_placeOrder_and_sleep(self, mock_ib):
        trade = MockTrade(
            order=MockOrder(orderId=1),
            orderStatus=MockOrderStatus(status="Submitted"),
        )
        mock_ib.placeOrder.return_value = trade
        contract = MockContract(conId=123)
        order = MagicMock()

        result_trade, error = _place_order(mock_ib, contract, order)
        assert result_trade is trade
        assert error == ""
        mock_ib.placeOrder.assert_called_once_with(contract, order)
        mock_ib.sleep.assert_called_once_with(1)


# ── _handle_tick_error ────────────────────────────────────────────


class TestHandleTickError:
    def _make_params(self, **overrides):
        defaults = dict(
            row=pd.Series({"market_rule_ids": "99"}),
            order_contract=MockContract(),
            idx_label="[1/1]", name="AAPL", ticker="AAPL",
            conid=1, limit_price=100.03, quantity=10,
            side="BUY", multiplier=1, ccy_label="USD",
            is_foreign=False, fx=1.0, dollar_alloc=1000.0,
            net_qty_raw=None, mic_str="XNAS", is_option=False,
        )
        defaults.update(overrides)
        return _OrderParams(**defaults)

    def test_snaps_and_returns_adjusted_price(self, mock_ib):
        mock_ib.reqMarketRule.return_value = [
            MockPriceIncrement(lowEdge=0.0, increment=0.05)]
        from src.market_data import _market_rule_cache
        _market_rule_cache.clear()
        p = self._make_params()
        result = _handle_tick_error(mock_ib, p)
        assert result == 100.0

    def test_returns_none_when_no_market_rules(self, mock_ib):
        p = self._make_params(
            row=pd.Series({"market_rule_ids": ""}))
        mock_ib.reqContractDetails.return_value = []
        result = _handle_tick_error(mock_ib, p)
        assert result is None

    def test_returns_none_when_snap_unchanged(self, mock_ib):
        mock_ib.reqMarketRule.return_value = [
            MockPriceIncrement(lowEdge=0.0, increment=0.01)]
        from src.market_data import _market_rule_cache
        _market_rule_cache.clear()
        p = self._make_params(limit_price=100.0)
        result = _handle_tick_error(mock_ib, p)
        assert result is None

    def test_fetches_rules_from_contract_details(self, mock_ib):
        cd = MockContractDetails(
            contract=MockContract(conId=1),
            marketRuleIds="50")
        mock_ib.reqContractDetails.return_value = [cd]
        mock_ib.reqMarketRule.return_value = [
            MockPriceIncrement(lowEdge=0.0, increment=0.05)]
        from src.market_data import _market_rule_cache
        _market_rule_cache.clear()
        p = self._make_params(
            row=pd.Series({"market_rule_ids": float("nan")}))
        result = _handle_tick_error(mock_ib, p)
        assert result is not None


# ── _prompt_modify ────────────────────────────────────────────────


class TestPromptModify:
    def _make_params(self):
        return _OrderParams(
            row=pd.Series({}), order_contract=MockContract(),
            idx_label="[1/1]", name="AAPL", ticker="AAPL",
            conid=1, limit_price=100.0, quantity=10,
            side="BUY", multiplier=1, ccy_label="USD",
            is_foreign=False, fx=1.0, dollar_alloc=1000.0,
            net_qty_raw=None, mic_str="XNAS", is_option=False,
        )

    def test_modify_quantity(self):
        p = self._make_params()
        with patch("builtins.input", side_effect=["20", "", ""]):
            _prompt_modify(p)
        assert p.quantity == 20

    def test_modify_price(self):
        p = self._make_params()
        with patch("builtins.input", side_effect=["", "150.25", ""]):
            _prompt_modify(p)
        assert p.limit_price == 150.25

    def test_modify_side(self):
        p = self._make_params()
        with patch("builtins.input", side_effect=["", "", "SELL"]):
            _prompt_modify(p)
        assert p.side == "SELL"

    def test_invalid_quantity_keeps_original(self):
        p = self._make_params()
        with patch("builtins.input", side_effect=["abc", "", ""]):
            _prompt_modify(p)
        assert p.quantity == 10

    def test_invalid_price_keeps_original(self):
        p = self._make_params()
        with patch("builtins.input", side_effect=["", "xyz", ""]):
            _prompt_modify(p)
        assert p.limit_price == 100.0

    def test_invalid_side_keeps_original(self):
        p = self._make_params()
        with patch("builtins.input", side_effect=["", "", "HOLD"]):
            _prompt_modify(p)
        assert p.side == "BUY"

    def test_empty_inputs_keep_all_original(self):
        p = self._make_params()
        with patch("builtins.input", side_effect=["", "", ""]):
            _prompt_modify(p)
        assert p.quantity == 10
        assert p.limit_price == 100.0
        assert p.side == "BUY"

    def test_strips_dollar_and_commas_from_price(self):
        p = self._make_params()
        with patch("builtins.input", side_effect=["", "$1,234.56", ""]):
            _prompt_modify(p)
        assert p.limit_price == 1234.56

    def test_strips_commas_from_quantity(self):
        p = self._make_params()
        with patch("builtins.input", side_effect=["1,000", "", ""]):
            _prompt_modify(p)
        assert p.quantity == 1000


# ── _place_single_order ───────────────────────────────────────────


class TestPlaceSingleOrder:
    def _make_params(self, limit_price=178.50, quantity=3, **overrides):
        defaults = dict(
            row=pd.Series({}), order_contract=MockContract(conId=1),
            idx_label="[1/1]", name="APPLE INC", ticker="AAPL",
            conid=265598, limit_price=limit_price, quantity=quantity,
            side="BUY", multiplier=1, ccy_label="USD",
            is_foreign=False, fx=1.0, dollar_alloc=500.0,
            net_qty_raw=3, mic_str="XNAS", is_option=False,
        )
        defaults.update(overrides)
        return _OrderParams(**defaults)

    def _mock_successful_trade(self, mock_ib, order_id=99):
        trade = MockTrade(
            contract=MockContract(conId=1),
            order=MockOrder(orderId=order_id),
            orderStatus=MockOrderStatus(status="Submitted"),
        )
        mock_ib.placeOrder.return_value = trade
        mock_ib.sleep.return_value = None
        return trade

    def test_auto_rejects_large_order_in_auto_mode(self, mock_ib):
        """Orders exceeding MAXIMUM_AMOUNT_AUTOMATIC_ORDER are rejected in -auto."""
        p = self._make_params(limit_price=1000.0, quantity=10)
        state = _AutoState(confirm_all=True)
        placed, rejected, large = [], [], []
        with patch("src.orders.MAXIMUM_AMOUNT_AUTOMATIC_ORDER", 500):
            signal = _place_single_order(
                mock_ib, p, placed, state,
                auto_mode=True, rejected_orders=rejected,
                large_orders=large)
        assert signal == "next"
        assert len(placed) == 0
        assert len(rejected) == 1
        assert "auto limit" in rejected[0]["reason"]
        assert len(large) == 1

    def test_interactive_defers_large_order(self, mock_ib):
        """Orders exceeding threshold are deferred in interactive auto-confirm."""
        p = self._make_params(limit_price=1000.0, quantity=10)
        state = _AutoState(confirm_all=True)
        placed, deferred, large = [], [], []
        with patch("src.orders.MAXIMUM_AMOUNT_AUTOMATIC_ORDER", 500):
            signal = _place_single_order(
                mock_ib, p, placed, state,
                auto_mode=False, deferred_orders=deferred,
                large_orders=large)
        assert signal == "next"
        assert len(placed) == 0
        assert len(deferred) == 1
        assert deferred[0] is p
        assert len(large) == 1

    def test_auto_confirm_small_order_places(self, mock_ib):
        """Orders below threshold are placed immediately in auto-confirm."""
        self._mock_successful_trade(mock_ib)
        p = self._make_params(limit_price=10.0, quantity=3)
        state = _AutoState(confirm_all=True)
        placed = []
        with patch("src.orders.MAXIMUM_AMOUNT_AUTOMATIC_ORDER", 500):
            signal = _place_single_order(
                mock_ib, p, placed, state, auto_mode=True)
        assert signal == "next"
        assert len(placed) == 1

    def test_large_placed_order_tracked(self, mock_ib):
        """A large manually-placed order is recorded in large_orders."""
        self._mock_successful_trade(mock_ib)
        p = self._make_params(limit_price=1000.0, quantity=10)
        placed, large = [], []
        with patch("builtins.input", return_value="Y"), \
             patch("src.orders.MAXIMUM_AMOUNT_AUTOMATIC_ORDER", 500):
            _place_single_order(
                mock_ib, p, placed, _AutoState(),
                large_orders=large)
        assert len(placed) == 1
        assert len(large) == 1

    def test_interactive_confirm_all_sets_state(self, mock_ib):
        """Choosing 'A' sets state.confirm_all = True."""
        self._mock_successful_trade(mock_ib)
        p = self._make_params()
        state = _AutoState()
        with patch("builtins.input", return_value="A"):
            _place_single_order(mock_ib, p, [], state)
        assert state.confirm_all is True

    def test_interactive_confirm_exchange_sets_state(self, mock_ib):
        """Choosing 'E' adds the exchange to state.confirm_exchanges."""
        self._mock_successful_trade(mock_ib)
        p = self._make_params()
        state = _AutoState()
        with patch("builtins.input", return_value="E"):
            _place_single_order(mock_ib, p, [], state)
        assert "XNAS" in state.confirm_exchanges

    def test_interactive_skip(self, mock_ib):
        """Choosing 'S' skips the order."""
        p = self._make_params()
        placed = []
        with patch("builtins.input", return_value="S"):
            signal = _place_single_order(
                mock_ib, p, placed, _AutoState())
        assert signal == "next"
        assert len(placed) == 0

    def test_interactive_skip_exchange(self, mock_ib):
        """Choosing 'X' skips and adds exchange to skip set."""
        p = self._make_params()
        state = _AutoState()
        with patch("builtins.input", return_value="X"):
            signal = _place_single_order(
                mock_ib, p, [], state)
        assert signal == "next"
        assert "XNAS" in state.skip_exchanges

    def test_interactive_quit(self, mock_ib):
        """Choosing 'Q' returns quit signal."""
        p = self._make_params()
        with patch("builtins.input", return_value="Q"):
            signal = _place_single_order(
                mock_ib, p, [], _AutoState())
        assert signal == "quit"

    def test_interactive_invalid_then_skip(self, mock_ib):
        """Invalid choice prints message, loops, then skip exits."""
        p = self._make_params()
        with patch("builtins.input", side_effect=["INVALID", "S"]):
            signal = _place_single_order(
                mock_ib, p, [], _AutoState())
        assert signal == "next"

    def test_modify_then_confirm(self, mock_ib):
        """'M' triggers modify prompt, then loops for confirmation."""
        self._mock_successful_trade(mock_ib)
        p = self._make_params()
        with patch("builtins.input", side_effect=["M", "20", "", "", "Y"]):
            signal = _place_single_order(
                mock_ib, p, [], _AutoState())
        assert signal == "next"
        assert p.quantity == 20

    def test_failed_order_status_not_counted(self, mock_ib):
        """Order with Cancelled/Inactive status is not added to placed."""
        trade = MockTrade(
            contract=MockContract(conId=1),
            order=MockOrder(orderId=99),
            orderStatus=MockOrderStatus(status="Inactive"),
            log=[MockLogEntry(errorCode=0, message="Order rejected")],
        )
        mock_ib.placeOrder.return_value = trade
        mock_ib.sleep.return_value = None
        p = self._make_params()
        placed, rejected = [], []
        with patch("builtins.input", return_value="Y"):
            _place_single_order(
                mock_ib, p, placed, _AutoState(),
                rejected_orders=rejected)
        assert len(placed) == 0
        assert len(rejected) == 1

    def test_exception_during_placement_auto_skips(self, mock_ib):
        """Exception in auto-confirm mode skips the order."""
        mock_ib.placeOrder.side_effect = Exception("Network error")
        mock_ib.sleep.return_value = None
        p = self._make_params()
        state = _AutoState(confirm_all=True)
        placed, rejected = [], []
        with patch("src.orders.MAXIMUM_AMOUNT_AUTOMATIC_ORDER", 99999):
            signal = _place_single_order(
                mock_ib, p, placed, state,
                auto_mode=True, rejected_orders=rejected)
        assert len(placed) == 0
        assert len(rejected) == 1
        assert "Network error" in rejected[0]["reason"]

    def test_exception_interactive_retry_then_skip(self, mock_ib):
        """Exception in interactive mode: retry once, then skip."""
        call_count = [0]
        def side_effect(contract, order):
            call_count[0] += 1
            if call_count[0] <= 1:
                raise Exception("Timeout")
            return MockTrade(
                order=MockOrder(orderId=99),
                orderStatus=MockOrderStatus(status="Submitted"),
                contract=MockContract(conId=1),
            )
        mock_ib.placeOrder.side_effect = side_effect
        mock_ib.sleep.return_value = None
        p = self._make_params()
        placed = []
        with patch("builtins.input", side_effect=["Y", "R", "Y"]):
            _place_single_order(mock_ib, p, placed, _AutoState())
        assert len(placed) == 1

    def test_tick_error_retries_with_adjusted_price(self, mock_ib):
        """Error 110 in trade.log triggers tick-size retry."""
        tick_error_trade = MockTrade(
            contract=MockContract(conId=1),
            order=MockOrder(orderId=99),
            orderStatus=MockOrderStatus(status="PreSubmitted"),
            log=[MockLogEntry(errorCode=110, message="tick size")],
        )
        success_trade = MockTrade(
            contract=MockContract(conId=1),
            order=MockOrder(orderId=100),
            orderStatus=MockOrderStatus(status="Submitted"),
        )
        call_count = [0]
        def place_side_effect(contract, order):
            call_count[0] += 1
            return tick_error_trade if call_count[0] == 1 else success_trade
        mock_ib.placeOrder.side_effect = place_side_effect
        mock_ib.sleep.return_value = None

        p = self._make_params(limit_price=100.03)
        p.row = pd.Series({"market_rule_ids": "99"})
        mock_ib.reqMarketRule.return_value = [
            MockPriceIncrement(lowEdge=0.0, increment=0.05)]
        from src.market_data import _market_rule_cache
        _market_rule_cache.clear()

        placed = []
        state = _AutoState(confirm_all=True)
        with patch("src.orders.MAXIMUM_AMOUNT_AUTOMATIC_ORDER", 99999):
            _place_single_order(
                mock_ib, p, placed, state, auto_mode=True)
        assert len(placed) == 1
        assert p.limit_price == 100.0


# ── run_order_loop (MAXIMUM_AMOUNT_AUTOMATIC_ORDER) ───────────────


class TestRunOrderLoopMaxAmount:
    def _make_df(self, limit_price=178.50, quantity=3):
        return pd.DataFrame({
            "conid": [265598],
            "limit_price": [limit_price],
            "Dollar Allocation": [500.0],
            "Name": ["APPLE INC"],
            "clean_ticker": ["AAPL"],
            "currency": ["USD"],
            "fx_rate": [1.0],
            "is_option": [False],
            "MIC Primary Exchange": ["XNAS"],
            "net_quantity": [quantity],
        })

    def test_auto_mode_rejects_large_order(self, mock_ib):
        mock_ib.reqContractDetails.return_value = []
        df = self._make_df(limit_price=1000.0, quantity=10)

        with patch("src.orders.MAXIMUM_AMOUNT_AUTOMATIC_ORDER", 500), \
             patch("src.telegram.notify_flagged_orders") as mock_notify:
            result = run_order_loop(mock_ib, df, auto_mode=True)

        assert len(result) == 0
        mock_notify.assert_called_once()
        rejected = mock_notify.call_args[0][0]
        assert len(rejected) == 1

    def test_interactive_defers_then_presents_large_order(self, mock_ib):
        mock_ib.reqContractDetails.return_value = []
        trade = MockTrade(
            contract=MockContract(conId=1),
            order=MockOrder(orderId=99),
            orderStatus=MockOrderStatus(status="Submitted"),
        )
        mock_ib.placeOrder.return_value = trade
        mock_ib.sleep.return_value = None

        df = self._make_df(limit_price=1000.0, quantity=10)

        with patch("src.orders.MAXIMUM_AMOUNT_AUTOMATIC_ORDER", 500), \
             patch("builtins.input", return_value="Y"):
            state = _AutoState(confirm_all=True)
            result = run_order_loop(mock_ib, df, auto_mode=False)

        assert len(result) == 1


# ── cancel_all_orders edge cases ──────────────────────────────────


class TestCancelAllOrdersEdgeCases:
    def test_failed_cancel_counted(self, mock_ib, capsys):
        c = MockContract(conId=1, symbol="AAPL", primaryExchange="NASDAQ")
        o = MockOrder(orderId=42, action="BUY", totalQuantity=10, lmtPrice=175.0)
        os_ = MockOrderStatus(status="Submitted")
        trade = MockTrade(contract=c, order=o, orderStatus=os_)
        mock_ib.openTrades.return_value = [trade]
        mock_ib.cancelOrder.side_effect = Exception("cancel failed")

        with patch("src.orders.is_exchange_open", return_value=True):
            cancel_all_orders(mock_ib, all_exchanges=True, auto_mode=True)

        output = capsys.readouterr().out
        assert "Failed" in output or "failed" in output

    def test_exchange_closed_skips_order(self, mock_ib, capsys):
        c = MockContract(conId=1, symbol="AAPL", primaryExchange="NASDAQ")
        o = MockOrder(orderId=42, action="BUY", totalQuantity=10, lmtPrice=175.0)
        os_ = MockOrderStatus(status="Submitted")
        trade = MockTrade(contract=c, order=o, orderStatus=os_)
        mock_ib.openTrades.return_value = [trade]

        with patch("src.orders.is_exchange_open", return_value=False):
            cancel_all_orders(mock_ib, all_exchanges=False)

        output = capsys.readouterr().out
        assert "Skipped" in output or "skipped" in output
        mock_ib.cancelOrder.assert_not_called()

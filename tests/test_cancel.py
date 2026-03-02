"""Tests for src/cancel.py — CancelState, decision logic, execution."""

from unittest.mock import patch, MagicMock

from src.cancel import (
    CancelState,
    signed_order_qty,
    resolve_cancel_decision,
    execute_cancel,
)


# ── signed_order_qty ───────────────────────────────────────────────


class TestSignedOrderQty:
    def test_buy_positive(self):
        order = {"side": "BUY", "remainingQuantity": 100}
        assert signed_order_qty(order) == 100

    def test_sell_negative(self):
        order = {"side": "SELL", "remainingQuantity": 50}
        assert signed_order_qty(order) == -50

    def test_zero_quantity(self):
        order = {"side": "BUY", "remainingQuantity": 0}
        assert signed_order_qty(order) == 0


# ── CancelState ────────────────────────────────────────────────────


class TestCancelState:
    def test_default_state(self):
        s = CancelState()
        assert s.confirm_all is False
        assert s.skip_all is False
        assert len(s.confirm_exchanges) == 0
        assert len(s.skip_exchanges) == 0

    def test_auto_mode_state(self):
        s = CancelState(confirm_all=True)
        assert s.confirm_all is True


# ── resolve_cancel_decision ────────────────────────────────────────


class TestResolveCancelDecision:
    def test_exchange_closed(self):
        state = CancelState()
        decision, is_auto = resolve_cancel_decision("XNYS", False, state)
        assert decision == "skip"
        assert is_auto is True

    def test_confirm_all(self):
        state = CancelState(confirm_all=True)
        decision, is_auto = resolve_cancel_decision("XNYS", True, state)
        assert decision == "cancel"
        assert is_auto is True

    def test_skip_all(self):
        state = CancelState(skip_all=True)
        decision, is_auto = resolve_cancel_decision("XNYS", True, state)
        assert decision == "skip"
        assert is_auto is True

    def test_confirm_specific_exchange(self):
        state = CancelState()
        state.confirm_exchanges.add("XNYS")
        decision, is_auto = resolve_cancel_decision("XNYS", True, state)
        assert decision == "cancel"
        assert is_auto is True

    def test_skip_specific_exchange(self):
        state = CancelState()
        state.skip_exchanges.add("XNYS")
        decision, is_auto = resolve_cancel_decision("XNYS", True, state)
        assert decision == "skip"
        assert is_auto is True

    def test_other_exchange_not_affected(self):
        state = CancelState()
        state.confirm_exchanges.add("XNYS")
        # XLON is not in confirm_exchanges, so we go to interactive prompt
        with patch("builtins.input", return_value="S"):
            decision, is_auto = resolve_cancel_decision("XLON", True, state)
        assert decision == "skip"
        assert is_auto is False

    def test_interactive_yes(self):
        state = CancelState()
        with patch("builtins.input", return_value="Y"):
            decision, is_auto = resolve_cancel_decision("XNYS", True, state)
        assert decision == "cancel"
        assert is_auto is False
        assert state.confirm_all is False

    def test_interactive_cancel_all(self):
        state = CancelState()
        with patch("builtins.input", return_value="A"):
            decision, is_auto = resolve_cancel_decision("XNYS", True, state)
        assert decision == "cancel"
        assert state.confirm_all is True

    def test_interactive_cancel_exchange(self):
        state = CancelState()
        with patch("builtins.input", return_value="E"):
            decision, is_auto = resolve_cancel_decision("XNYS", True, state)
        assert decision == "cancel"
        assert "XNYS" in state.confirm_exchanges

    def test_interactive_skip(self):
        state = CancelState()
        with patch("builtins.input", return_value="S"):
            decision, is_auto = resolve_cancel_decision("XNYS", True, state)
        assert decision == "skip"
        assert is_auto is False

    def test_interactive_skip_exchange(self):
        state = CancelState()
        with patch("builtins.input", return_value="X"):
            decision, is_auto = resolve_cancel_decision("XNYS", True, state)
        assert decision == "skip"
        assert "XNYS" in state.skip_exchanges

    def test_interactive_skip_all(self):
        state = CancelState()
        with patch("builtins.input", return_value="N"):
            decision, is_auto = resolve_cancel_decision("XNYS", True, state)
        assert decision == "skip"
        assert state.skip_all is True

    def test_invalid_input_defaults_to_skip(self):
        state = CancelState()
        with patch("builtins.input", return_value="INVALID"):
            decision, is_auto = resolve_cancel_decision("XNYS", True, state)
        assert decision == "skip"

    def test_empty_input_defaults_to_skip(self):
        state = CancelState()
        with patch("builtins.input", return_value=""):
            decision, is_auto = resolve_cancel_decision("XNYS", True, state)
        assert decision == "skip"


# ── execute_cancel ─────────────────────────────────────────────────


class TestExecuteCancel:
    def _setup_error_event(self, mock_ib):
        """Wire up mock errorEvent so += captures the handler and sleep
        can fire it.  Returns a list that collects registered handlers."""
        handlers: list = []

        def on_add(handler):
            handlers.append(handler)
            return mock_ib.errorEvent
        mock_ib.errorEvent.__iadd__ = MagicMock(side_effect=on_add)
        return handlers

    def test_successful_cancel(self, mock_ib):
        self._setup_error_event(mock_ib)
        order = MagicMock()
        assert execute_cancel(mock_ib, order) is True
        mock_ib.cancelOrder.assert_called_once_with(order)

    def test_failed_cancel_exception(self, mock_ib):
        self._setup_error_event(mock_ib)
        mock_ib.cancelOrder.side_effect = Exception("cancel failed")
        order = MagicMock()
        assert execute_cancel(mock_ib, order) is False

    def test_already_cancelled_order(self, mock_ib):
        """Error 202 (already cancelled) is treated as success."""
        handlers = self._setup_error_event(mock_ib)

        def fire_202(*_args, **_kwargs):
            for h in handlers:
                h(0, 202, "Order Cancelled", None)
        mock_ib.sleep.side_effect = fire_202

        order = MagicMock()
        assert execute_cancel(mock_ib, order) is True

    def test_error_10147_order_not_found(self, mock_ib):
        """Error 10147 (order not found) must return False so the order
        stays in the pending count and no duplicate is placed."""
        handlers = self._setup_error_event(mock_ib)

        def fire_10147(*_args, **_kwargs):
            for h in handlers:
                h(0, 10147,
                  "OrderId 0 that needs to be cancelled is not found.",
                  None)
        mock_ib.sleep.side_effect = fire_10147

        order = MagicMock()
        assert execute_cancel(mock_ib, order) is False

    def test_any_non_202_error_returns_false(self, mock_ib):
        """Any error code other than 202 should cause a failure return."""
        handlers = self._setup_error_event(mock_ib)

        def fire_error(*_args, **_kwargs):
            for h in handlers:
                h(0, 999, "Some unexpected error", None)
        mock_ib.sleep.side_effect = fire_error

        order = MagicMock()
        assert execute_cancel(mock_ib, order) is False

    def test_handler_cleaned_up_on_success(self, mock_ib):
        """errorEvent -= is always called (via finally) even on success."""
        self._setup_error_event(mock_ib)
        execute_cancel(mock_ib, MagicMock())
        mock_ib.errorEvent.__isub__.assert_called_once()

    def test_handler_cleaned_up_on_exception(self, mock_ib):
        """errorEvent -= is always called (via finally) even on exception."""
        self._setup_error_event(mock_ib)
        mock_ib.cancelOrder.side_effect = Exception("boom")
        execute_cancel(mock_ib, MagicMock())
        mock_ib.errorEvent.__isub__.assert_called_once()

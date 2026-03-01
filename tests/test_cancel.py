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
    def test_successful_cancel(self, mock_ib):
        order = MagicMock()
        assert execute_cancel(mock_ib, order) is True
        mock_ib.cancelOrder.assert_called_once_with(order)

    def test_failed_cancel(self, mock_ib):
        mock_ib.cancelOrder.side_effect = Exception("cancel failed")
        order = MagicMock()
        assert execute_cancel(mock_ib, order) is False

    def test_already_cancelled_order(self, mock_ib):
        """Error 202 (already cancelled) is suppressed — should still return True."""
        order = MagicMock()
        assert execute_cancel(mock_ib, order) is True

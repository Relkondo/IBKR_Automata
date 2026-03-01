"""Tests for src/telegram.py — send_message and notify_flagged_orders."""

from unittest.mock import patch, MagicMock

from src.telegram import (
    _is_configured,
    send_message,
    notify_flagged_orders,
)


# ── _is_configured ─────────────────────────────────────────────────


class TestIsConfigured:
    def test_both_set(self):
        with patch("src.telegram.TELEGRAM_BOT_TOKEN", "token123"), \
             patch("src.telegram.TELEGRAM_CHAT_ID", "chat456"):
            assert _is_configured() is True

    def test_missing_token(self):
        with patch("src.telegram.TELEGRAM_BOT_TOKEN", None), \
             patch("src.telegram.TELEGRAM_CHAT_ID", "chat456"):
            assert _is_configured() is False

    def test_missing_chat_id(self):
        with patch("src.telegram.TELEGRAM_BOT_TOKEN", "token123"), \
             patch("src.telegram.TELEGRAM_CHAT_ID", None):
            assert _is_configured() is False

    def test_empty_strings(self):
        with patch("src.telegram.TELEGRAM_BOT_TOKEN", ""), \
             patch("src.telegram.TELEGRAM_CHAT_ID", ""):
            assert _is_configured() is False


# ── send_message ───────────────────────────────────────────────────


class TestSendMessage:
    def test_not_configured(self):
        with patch("src.telegram.TELEGRAM_BOT_TOKEN", None), \
             patch("src.telegram.TELEGRAM_CHAT_ID", None):
            assert send_message("test") is False

    def test_successful_send(self):
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)

        with patch("src.telegram.TELEGRAM_BOT_TOKEN", "tok"), \
             patch("src.telegram.TELEGRAM_CHAT_ID", "123"), \
             patch("src.telegram.urllib.request.urlopen", return_value=mock_resp):
            assert send_message("hello") is True

    def test_network_error(self):
        with patch("src.telegram.TELEGRAM_BOT_TOKEN", "tok"), \
             patch("src.telegram.TELEGRAM_CHAT_ID", "123"), \
             patch("src.telegram.urllib.request.urlopen",
                   side_effect=Exception("timeout")):
            assert send_message("hello") is False


# ── notify_flagged_orders ──────────────────────────────────────────


class TestNotifyFlaggedOrders:
    def test_empty_lists_no_send(self):
        with patch("src.telegram.send_message") as mock_send:
            notify_flagged_orders([], [])
            mock_send.assert_not_called()

    def test_not_configured_no_send(self):
        with patch("src.telegram.TELEGRAM_BOT_TOKEN", None), \
             patch("src.telegram.TELEGRAM_CHAT_ID", None):
            notify_flagged_orders(
                [{"ticker": "A", "name": "A", "side": "BUY",
                  "quantity": 1, "usd_amount": 100, "exchange": "XNAS",
                  "reason": "test"}],
                [],
            )
            # Should return without sending

    def test_sends_rejected(self):
        rejected = [{
            "ticker": "AAPL", "name": "APPLE", "side": "BUY",
            "quantity": 10, "usd_amount": 1785.0, "exchange": "XNAS",
            "reason": "Error 110",
        }]
        with patch("src.telegram.TELEGRAM_BOT_TOKEN", "tok"), \
             patch("src.telegram.TELEGRAM_CHAT_ID", "123"), \
             patch("src.telegram.send_message", return_value=True) as mock_send:
            notify_flagged_orders(rejected, [])
            mock_send.assert_called_once()
            msg = mock_send.call_args[0][0]
            assert "Rejected" in msg
            assert "AAPL" in msg

    def test_sends_large(self):
        large = [{
            "ticker": "MSFT", "name": "MICROSOFT", "side": "BUY",
            "quantity": 5, "usd_amount": 2100.0, "exchange": "XNAS",
            "status": "placed",
        }]
        with patch("src.telegram.TELEGRAM_BOT_TOKEN", "tok"), \
             patch("src.telegram.TELEGRAM_CHAT_ID", "123"), \
             patch("src.telegram.send_message", return_value=True) as mock_send:
            notify_flagged_orders([], large)
            mock_send.assert_called_once()
            msg = mock_send.call_args[0][0]
            assert "Large Orders" in msg
            assert "MSFT" in msg

    def test_combined_rejected_and_large(self):
        rejected = [{
            "ticker": "A", "name": "A", "side": "BUY",
            "quantity": 1, "usd_amount": 100, "exchange": "?",
            "reason": "err",
        }]
        large = [{
            "ticker": "B", "name": "B", "side": "SELL",
            "quantity": 2, "usd_amount": 200, "exchange": "?",
            "status": "deferred",
        }]
        with patch("src.telegram.TELEGRAM_BOT_TOKEN", "tok"), \
             patch("src.telegram.TELEGRAM_CHAT_ID", "123"), \
             patch("src.telegram.send_message", return_value=True) as mock_send:
            notify_flagged_orders(rejected, large)
            msg = mock_send.call_args[0][0]
            assert "Rejected" in msg
            assert "Large Orders" in msg

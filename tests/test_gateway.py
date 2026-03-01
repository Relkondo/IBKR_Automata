"""Tests for src/gateway.py — IB Gateway lifecycle management."""

import socket
from unittest.mock import patch, MagicMock

import pytest

from src.gateway import is_gateway_running, start_gateway, ensure_gateway


class TestIsGatewayRunning:
    def test_returns_true_when_port_open(self):
        mock_sock = MagicMock()
        with patch("src.gateway.socket.socket") as mock_socket_cls:
            mock_socket_cls.return_value.__enter__ = MagicMock(
                return_value=mock_sock
            )
            mock_socket_cls.return_value.__exit__ = MagicMock(return_value=False)
            assert is_gateway_running("127.0.0.1", 4001) is True
            mock_sock.connect.assert_called_once_with(("127.0.0.1", 4001))

    def test_returns_false_on_connection_refused(self):
        mock_sock = MagicMock()
        mock_sock.connect.side_effect = ConnectionRefusedError
        with patch("src.gateway.socket.socket") as mock_socket_cls:
            mock_socket_cls.return_value.__enter__ = MagicMock(
                return_value=mock_sock
            )
            mock_socket_cls.return_value.__exit__ = MagicMock(return_value=False)
            assert is_gateway_running("127.0.0.1", 4001) is False

    def test_returns_false_on_os_error(self):
        mock_sock = MagicMock()
        mock_sock.connect.side_effect = OSError("timeout")
        with patch("src.gateway.socket.socket") as mock_socket_cls:
            mock_socket_cls.return_value.__enter__ = MagicMock(
                return_value=mock_sock
            )
            mock_socket_cls.return_value.__exit__ = MagicMock(return_value=False)
            assert is_gateway_running("127.0.0.1", 4001) is False


class TestStartGateway:
    @patch("src.gateway._wait_for_gateway")
    @patch("src.gateway.subprocess.Popen")
    @patch("src.gateway.os.path.isfile", return_value=True)
    @patch("src.gateway.os.makedirs")
    @patch("builtins.open", new_callable=MagicMock)
    def test_launches_ibc_script(self, mock_open, mock_makedirs,
                                  mock_isfile, mock_popen, mock_wait):
        proc = start_gateway(timeout=10)
        mock_popen.assert_called_once()
        cmd = mock_popen.call_args.kwargs.get("args", mock_popen.call_args[0][0])
        assert "--gateway" in cmd
        assert mock_wait.call_count == 1
        assert mock_wait.call_args[0][0] == 10

    @patch("src.gateway.os.path.isfile", return_value=False)
    def test_raises_if_script_missing(self, mock_isfile):
        with pytest.raises(FileNotFoundError, match="IBC start script not found"):
            start_gateway()

    @patch("src.gateway._wait_for_gateway")
    @patch("src.gateway.subprocess.Popen")
    @patch("src.gateway.os.path.isfile", return_value=True)
    @patch("src.gateway.os.makedirs")
    @patch("builtins.open", new_callable=MagicMock)
    def test_cmd_contains_required_args(self, mock_open, mock_makedirs,
                                         mock_isfile, mock_popen, mock_wait):
        start_gateway(timeout=5)
        cmd = mock_popen.call_args.kwargs.get("args", mock_popen.call_args[0][0])
        cmd_str = " ".join(cmd)
        assert "--gateway" in cmd_str
        assert "--tws-path=" in cmd_str
        assert "--ibc-path=" in cmd_str
        assert "--ibc-ini=" in cmd_str
        assert "--mode=" in cmd_str


class TestWaitForGateway:
    @patch("src.gateway.time.sleep")
    @patch("src.gateway.is_gateway_running", return_value=True)
    def test_returns_immediately_if_running(self, mock_running, mock_sleep):
        from src.gateway import _wait_for_gateway
        _wait_for_gateway(60)
        mock_sleep.assert_not_called()

    @patch("src.gateway.time.sleep")
    @patch("src.gateway.is_gateway_running", side_effect=[False, False, True])
    def test_polls_until_running(self, mock_running, mock_sleep):
        from src.gateway import _wait_for_gateway
        _wait_for_gateway(60)
        assert mock_sleep.call_count == 2

    @patch("src.gateway.time.sleep")
    @patch("src.gateway.is_gateway_running", return_value=False)
    def test_raises_on_timeout(self, mock_running, mock_sleep):
        from src.gateway import _wait_for_gateway
        with pytest.raises(RuntimeError, match="did not start within"):
            _wait_for_gateway(6)


class TestEnsureGateway:
    @patch("src.gateway.is_gateway_running", return_value=True)
    def test_noop_when_already_running(self, mock_running):
        ensure_gateway()
        mock_running.assert_called_once()

    @patch("src.gateway.start_gateway")
    @patch("src.gateway.is_gateway_running", return_value=False)
    def test_starts_gateway_when_not_running(self, mock_running, mock_start):
        ensure_gateway()
        mock_start.assert_called_once()

    @patch("src.gateway.start_gateway", side_effect=FileNotFoundError("no IBC"))
    @patch("src.gateway.is_gateway_running", return_value=False)
    def test_propagates_start_errors(self, mock_running, mock_start):
        with pytest.raises(FileNotFoundError, match="no IBC"):
            ensure_gateway()

"""Tests for src/main.py — CLI args, workflow orchestration."""

import os
import sys
from unittest.mock import patch, MagicMock, call

import pandas as pd
import pytest

from src.main import main, _load_project_portfolio


# ── _load_project_portfolio ────────────────────────────────────────


class TestLoadProjectPortfolio:
    def test_loads_existing_csv(self, tmp_path):
        csv_path = tmp_path / "Project_Portfolio.csv"
        df = pd.DataFrame({"Ticker": ["AAPL"], "conid": [265598]})
        df.to_csv(str(csv_path), index=False)

        with patch("src.main.OUTPUT_DIR", str(tmp_path)):
            result = _load_project_portfolio()
        assert len(result) == 1
        assert result.iloc[0]["Ticker"] == "AAPL"

    def test_raises_when_missing(self, tmp_path):
        with patch("src.main.OUTPUT_DIR", str(tmp_path)):
            with pytest.raises(FileNotFoundError, match="Project_Portfolio.csv"):
                _load_project_portfolio()


# ── CLI argument parsing (main) ────────────────────────────────────


class TestMainArgParsing:
    def test_mutually_exclusive_modes(self):
        """Two exclusive modes should cause sys.exit(1)."""
        with patch("sys.argv", ["main", "noop", "project-portfolio"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    def test_noop_and_cancel_exclusive(self):
        with patch("sys.argv", ["main", "noop", "cancel-all-orders"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    def test_buy_all_with_noop_error(self):
        with patch("sys.argv", ["main", "buy-all", "noop"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    def test_buy_all_with_cancel_error(self):
        with patch("sys.argv", ["main", "buy-all", "cancel-all-orders"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    def test_three_exclusive_modes(self):
        with patch("sys.argv", ["main", "noop", "noop-recalculate",
                                 "cancel-all-orders"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1


# ── Workflow orchestration ─────────────────────────────────────────


class TestMainWorkflows:
    def _mock_connect(self):
        ib = MagicMock()
        ib.isConnected.return_value = True
        ib.disconnect.return_value = None
        return ib

    @patch("src.main.connect")
    @patch("src.main.cancel_all_orders")
    def test_cancel_all_orders_mode(self, mock_cancel, mock_connect):
        mock_connect.return_value = self._mock_connect()
        with patch("sys.argv", ["main", "cancel-all-orders"]):
            main()
        mock_cancel.assert_called_once()

    @patch("src.main.connect")
    @patch("src.main.load_portfolio")
    @patch("src.main.get_investable_amount", return_value=100000.0)
    @patch("src.main.resolve_conids")
    @patch("src.main.resolve_currencies")
    @patch("src.main.fetch_market_data")
    @patch("src.main.save_project_portfolio")
    def test_noop_mode(self, mock_save, mock_fetch, mock_currencies,
                       mock_conids, mock_invest, mock_load, mock_connect):
        ib = self._mock_connect()
        mock_connect.return_value = ib
        df = pd.DataFrame({
            "Basket Allocation": [5.0],
            "Ticker": ["AAPL"],
        })
        mock_load.return_value = df
        mock_conids.return_value = df
        mock_currencies.return_value = df
        mock_fetch.return_value = df

        with patch("sys.argv", ["main", "noop"]):
            main()

        mock_load.assert_called_once()
        mock_save.assert_called_once()
        ib.disconnect.assert_called_once()

    @patch("src.main.connect")
    @patch("src.main._load_project_portfolio")
    @patch("src.main.reconcile")
    @patch("src.main.filter_df_by_open_exchange")
    @patch("src.main.run_order_loop", return_value=[])
    @patch("src.main.print_order_summary")
    def test_project_portfolio_mode(self, mock_summary, mock_loop,
                                     mock_filter, mock_reconcile,
                                     mock_load, mock_connect):
        ib = self._mock_connect()
        mock_connect.return_value = ib
        df = pd.DataFrame({"Ticker": ["AAPL"], "conid": [265598]})
        mock_load.return_value = df
        mock_reconcile.return_value = df
        mock_filter.return_value = df

        with patch("sys.argv", ["main", "project-portfolio"]):
            main()

        mock_load.assert_called_once()
        mock_reconcile.assert_called_once()
        mock_loop.assert_called_once()

    @patch("src.main.connect")
    @patch("src.main._load_project_portfolio")
    @patch("src.main.reconcile")
    @patch("src.main.generate_project_vs_current")
    def test_print_comparison_mode(self, mock_gen, mock_reconcile,
                                    mock_load, mock_connect):
        ib = self._mock_connect()
        mock_connect.return_value = ib
        df = pd.DataFrame({"Ticker": ["AAPL"], "conid": [265598]})
        mock_load.return_value = df
        mock_reconcile.return_value = df

        with patch("sys.argv", ["main", "print-project-vs-current"]):
            main()

        mock_gen.assert_called_once()

    @patch("src.main.connect")
    @patch("src.main.load_portfolio")
    @patch("src.main.get_investable_amount", return_value=100000.0)
    @patch("src.main.resolve_conids")
    @patch("src.main.resolve_currencies")
    @patch("src.main.fetch_market_data")
    @patch("src.main.save_project_portfolio")
    @patch("src.main.filter_df_by_open_exchange")
    @patch("src.main.run_order_loop", return_value=[])
    @patch("src.main.print_order_summary")
    def test_buy_all_mode_skips_reconcile(
        self, mock_summary, mock_loop, mock_filter, mock_save,
        mock_fetch, mock_currencies, mock_conids, mock_invest,
        mock_load, mock_connect,
    ):
        ib = self._mock_connect()
        mock_connect.return_value = ib
        df = pd.DataFrame({
            "Basket Allocation": [5.0], "Ticker": ["AAPL"],
        })
        mock_load.return_value = df
        mock_conids.return_value = df
        mock_currencies.return_value = df
        mock_fetch.return_value = df
        mock_filter.return_value = df

        with patch("sys.argv", ["main", "buy-all"]), \
             patch("src.main.reconcile") as mock_reconcile:
            main()

        mock_reconcile.assert_not_called()
        mock_loop.assert_called_once()

    @patch("src.main.connect")
    def test_connection_failure_auto_mode_sends_telegram(self, mock_connect):
        mock_connect.side_effect = ConnectionError("TWS not running")
        with patch("sys.argv", ["main", "-auto"]), \
             patch("src.telegram.send_message") as mock_send:
            with pytest.raises(ConnectionError):
                main()

    @patch("src.main.connect")
    @patch("src.main._load_project_portfolio")
    @patch("src.main.get_investable_amount", return_value=100000.0)
    @patch("src.main.resolve_currencies")
    @patch("src.main.fetch_market_data")
    @patch("src.main.save_project_portfolio")
    def test_noop_recalculate_mode(
        self, mock_save, mock_fetch, mock_currencies,
        mock_invest, mock_load, mock_connect,
    ):
        ib = self._mock_connect()
        mock_connect.return_value = ib
        df = pd.DataFrame({
            "Basket Allocation": [5.0], "Ticker": ["AAPL"],
        })
        mock_load.return_value = df
        mock_currencies.return_value = df
        mock_fetch.return_value = df

        with patch("sys.argv", ["main", "noop-recalculate"]):
            main()

        mock_load.assert_called_once()
        mock_invest.assert_called_once()
        mock_save.assert_called_once()

    @patch("src.main.connect")
    @patch("src.main.load_portfolio")
    @patch("src.main.get_investable_amount", return_value=100000.0)
    @patch("src.main.resolve_conids")
    @patch("src.main.resolve_currencies")
    @patch("src.main.fetch_market_data")
    @patch("src.main.save_project_portfolio")
    @patch("src.main.reconcile")
    @patch("src.main.filter_df_by_open_exchange")
    @patch("src.main.run_order_loop", return_value=[])
    @patch("src.main.print_order_summary")
    def test_all_exchanges_flag(
        self, mock_summary, mock_loop, mock_filter, mock_reconcile,
        mock_save, mock_fetch, mock_currencies, mock_conids,
        mock_invest, mock_load, mock_connect,
    ):
        ib = self._mock_connect()
        mock_connect.return_value = ib
        df = pd.DataFrame({
            "Basket Allocation": [5.0], "Ticker": ["AAPL"],
        })
        mock_load.return_value = df
        mock_conids.return_value = df
        mock_currencies.return_value = df
        mock_fetch.return_value = df
        mock_reconcile.return_value = df

        with patch("sys.argv", ["main", "-all-exchanges"]):
            main()

        # filter_df_by_open_exchange should NOT be called with -all-exchanges
        mock_filter.assert_not_called()

    @patch("src.main.connect")
    @patch("src.main.load_portfolio")
    @patch("src.main.get_investable_amount", return_value=100000.0)
    @patch("src.main.resolve_conids")
    @patch("src.main.resolve_currencies")
    @patch("src.main.fetch_market_data")
    @patch("src.main.save_project_portfolio")
    @patch("src.main.reconcile")
    @patch("src.main.filter_df_by_open_exchange")
    @patch("src.main.run_order_loop", return_value=[])
    @patch("src.main.print_order_summary")
    def test_normal_mode_filters_exchanges(
        self, mock_summary, mock_loop, mock_filter, mock_reconcile,
        mock_save, mock_fetch, mock_currencies, mock_conids,
        mock_invest, mock_load, mock_connect,
    ):
        ib = self._mock_connect()
        mock_connect.return_value = ib
        df = pd.DataFrame({
            "Basket Allocation": [5.0], "Ticker": ["AAPL"],
        })
        mock_load.return_value = df
        mock_conids.return_value = df
        mock_currencies.return_value = df
        mock_fetch.return_value = df
        mock_reconcile.return_value = df
        mock_filter.return_value = df

        with patch("sys.argv", ["main"]):
            main()

        mock_filter.assert_called_once()

    @patch("src.main.connect")
    def test_disconnect_always_called(self, mock_connect):
        ib = self._mock_connect()
        mock_connect.return_value = ib

        with patch("sys.argv", ["main", "noop"]), \
             patch("src.main.load_portfolio", side_effect=ValueError("bad")):
            with pytest.raises(ValueError):
                main()

        ib.disconnect.assert_called_once()

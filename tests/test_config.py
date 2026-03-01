"""Tests for src/config.py — verify configuration values and paths."""

import os

from src.config import (
    TWS_HOST, TWS_PORT, TWS_CLIENT_ID,
    PROJECT_ROOT, ASSETS_DIR, OUTPUT_DIR,
    MINIMUM_TRADING_AMOUNT, MAXIMUM_AMOUNT_AUTOMATIC_ORDER,
    MINIMUM_CASH_RESERVE, SELL_REBALANCE_RATIO_LIMIT,
    PRICE_OFFSET, LIMIT_PRICE_OFFSET,
    STALE_ORDER_TOL_PCT, STALE_ORDER_TOL_PCT_ILLIQUID,
    OPTION_TICKER_REDIRECTS, STOCK_TICKER_REDIRECTS,
    IGNORE_NAMES, PROJECT_PORTFOLIO_COLUMNS,
)


class TestConfigValues:
    def test_tws_host_is_localhost(self):
        assert TWS_HOST == "127.0.0.1"

    def test_tws_port_is_int(self):
        assert isinstance(TWS_PORT, int)
        assert TWS_PORT > 0

    def test_tws_client_id_is_int(self):
        assert isinstance(TWS_CLIENT_ID, int)

    def test_project_root_contains_src(self):
        assert os.path.isdir(os.path.join(PROJECT_ROOT, "src"))

    def test_assets_dir_under_project_root(self):
        assert ASSETS_DIR.startswith(PROJECT_ROOT)

    def test_output_dir_under_project_root(self):
        assert OUTPUT_DIR.startswith(PROJECT_ROOT)


class TestThresholds:
    def test_minimum_trading_amount_positive(self):
        assert MINIMUM_TRADING_AMOUNT > 0

    def test_maximum_auto_order_greater_than_minimum(self):
        assert MAXIMUM_AMOUNT_AUTOMATIC_ORDER > MINIMUM_TRADING_AMOUNT

    def test_cash_reserve_non_negative(self):
        assert MINIMUM_CASH_RESERVE >= 0

    def test_sell_rebalance_ratio_limit_above_one(self):
        assert SELL_REBALANCE_RATIO_LIMIT > 1.0

    def test_price_offset_non_negative(self):
        assert PRICE_OFFSET >= 0

    def test_limit_price_offset_non_negative(self):
        assert LIMIT_PRICE_OFFSET >= 0

    def test_stale_order_tolerances(self):
        assert 0 < STALE_ORDER_TOL_PCT < 1
        assert 0 < STALE_ORDER_TOL_PCT_ILLIQUID < 1
        assert STALE_ORDER_TOL_PCT_ILLIQUID > STALE_ORDER_TOL_PCT


class TestRedirectsAndIgnore:
    def test_option_ticker_redirects_is_dict(self):
        assert isinstance(OPTION_TICKER_REDIRECTS, dict)

    def test_stock_ticker_redirects_is_dict(self):
        assert isinstance(STOCK_TICKER_REDIRECTS, dict)

    def test_ignore_names_is_list(self):
        assert isinstance(IGNORE_NAMES, list)
        assert all(isinstance(n, str) for n in IGNORE_NAMES)


class TestPortfolioColumns:
    def test_columns_list_not_empty(self):
        assert len(PROJECT_PORTFOLIO_COLUMNS) > 0

    def test_essential_columns_present(self):
        for col in ("Ticker", "conid", "limit_price", "Qty"):
            assert col in PROJECT_PORTFOLIO_COLUMNS

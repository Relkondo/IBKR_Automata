"""Tests for src/comparison.py — market value, safe diff, comparison output."""

import os
from unittest.mock import patch, MagicMock

import pandas as pd
from tests.conftest import MockContract, MockPortfolioItem
from src.comparison import (
    _market_value_usd,
    _safe_diff,
    generate_project_vs_current,
)


# ── _market_value_usd ─────────────────────────────────────────────


class TestMarketValueUsd:
    def test_usd_position(self):
        mkt_values = {123: 5000.0}
        assert _market_value_usd(123, mkt_values, 1.0) == 5000.0

    def test_foreign_position(self):
        mkt_values = {123: 750000.0}  # JPY
        result = _market_value_usd(123, mkt_values, 150.0)
        assert result == 5000.0

    def test_position_not_held(self):
        mkt_values = {}
        assert _market_value_usd(123, mkt_values, 1.0) == 0.0

    def test_nan_conid(self):
        mkt_values = {123: 5000.0}
        assert _market_value_usd(None, mkt_values, 1.0) == 0.0

    def test_none_fx(self):
        mkt_values = {123: 5000.0}
        assert _market_value_usd(123, mkt_values, None) is None

    def test_conid_as_float(self):
        mkt_values = {123: 5000.0}
        assert _market_value_usd(123.0, mkt_values, 1.0) == 5000.0


# ── _safe_diff ─────────────────────────────────────────────────────


class TestSafeDiff:
    def test_both_present(self):
        assert _safe_diff(100.0, 40.0) == 60.0

    def test_negative_result(self):
        assert _safe_diff(40.0, 100.0) == -60.0

    def test_b_is_none(self):
        assert _safe_diff(100.0, None) is None

    def test_a_is_nan(self):
        assert _safe_diff(float("nan"), 50.0) is None

    def test_a_is_none(self):
        assert _safe_diff(None, 50.0) is None

    def test_zero_values(self):
        assert _safe_diff(0.0, 0.0) == 0.0


# ── generate_project_vs_current ────────────────────────────────────


class TestGenerateProjectVsCurrent:
    def test_creates_excel_file(self, mock_ib, reconciled_df, tmp_path):
        c = MockContract(conId=265598, symbol="AAPL")
        item = MockPortfolioItem(contract=c, marketValue=5000.0)
        mock_ib.portfolio.return_value = [item]

        with patch("src.comparison.OUTPUT_DIR", str(tmp_path)):
            generate_project_vs_current(mock_ib, reconciled_df)

        out_path = os.path.join(str(tmp_path), "Project_VS_Current.xlsx")
        assert os.path.isfile(out_path)

        loaded = pd.read_excel(out_path)
        assert "IBKR Name" in loaded.columns
        assert "Current Dollar Allocation" in loaded.columns
        assert "Project VS Current" in loaded.columns
        assert len(loaded) == 2

    def test_handles_empty_portfolio(self, mock_ib, reconciled_df, tmp_path):
        mock_ib.portfolio.return_value = []

        with patch("src.comparison.OUTPUT_DIR", str(tmp_path)):
            generate_project_vs_current(mock_ib, reconciled_df)

        out_path = os.path.join(str(tmp_path), "Project_VS_Current.xlsx")
        assert os.path.isfile(out_path)

"""Tests for src/portfolio.py — loading, filtering, option detection, redirects."""

import os
import re
from unittest.mock import patch

import pandas as pd
import pytest

from src.portfolio import (
    _is_option,
    _clean_ticker,
    _latest_xlsx,
    _parse_ignore_sets,
    is_name_ignored,
    _ticker_prefix,
    _apply_ticker_redirects,
    load_portfolio,
    OPT_TICKER_RE,
)


# ── Option ticker regex ───────────────────────────────────────────


class TestOptTickerRegex:
    def test_standard_option_ticker(self):
        m = OPT_TICKER_RE.match("QQQ US 02/27/26 P600 Equity")
        assert m is not None
        assert m.group("underlying") == "QQQ"
        assert m.group("month") == "02"
        assert m.group("day") == "27"
        assert m.group("year") == "26"
        assert m.group("right") == "P"
        assert m.group("strike") == "600"

    def test_call_option(self):
        m = OPT_TICKER_RE.match("SPY US 12/20/26 C550")
        assert m is not None
        assert m.group("right") == "C"
        assert m.group("strike") == "550"

    def test_fractional_strike(self):
        m = OPT_TICKER_RE.match("AAPL US 01/15/27 P142.5 Equity")
        assert m is not None
        assert m.group("strike") == "142.5"

    def test_no_trailing_suffix(self):
        m = OPT_TICKER_RE.match("TSLA US 06/30/26 C800")
        assert m is not None

    def test_non_option_ticker_no_match(self):
        assert OPT_TICKER_RE.match("AAPL US Equity") is None

    def test_empty_string_no_match(self):
        assert OPT_TICKER_RE.match("") is None


# ── _is_option ─────────────────────────────────────────────────────


class TestIsOption:
    def test_calls_on_in_name(self):
        row = pd.Series({"Name": "March 26 Calls on SPX", "Ticker": ""})
        assert _is_option(row) is True

    def test_puts_on_in_name(self):
        row = pd.Series({"Name": "Feb 26 Puts on QQQ", "Ticker": ""})
        assert _is_option(row) is True

    def test_option_ticker_pattern(self):
        row = pd.Series({
            "Name": "Some Option", "Ticker": "QQQ US 02/27/26 P600 Equity"
        })
        assert _is_option(row) is True

    def test_stock_name_and_ticker(self):
        row = pd.Series({"Name": "APPLE INC", "Ticker": "AAPL US Equity"})
        assert _is_option(row) is False

    def test_missing_fields(self):
        row = pd.Series({"Name": "", "Ticker": ""})
        assert _is_option(row) is False


# ── _clean_ticker ──────────────────────────────────────────────────


class TestCleanTicker:
    def test_strips_us_equity_suffix(self):
        row = pd.Series({
            "Security Ticker": "AAPL US Equity", "Ticker": "AAPL US Equity"
        })
        assert _clean_ticker(row) == "AAPL"

    def test_strips_index_suffix(self):
        row = pd.Series({
            "Security Ticker": "SPX US Index", "Ticker": ""
        })
        assert _clean_ticker(row) == "SPX"

    def test_prefers_security_ticker(self):
        row = pd.Series({
            "Security Ticker": "MSFT US Equity", "Ticker": "OLD_TICKER"
        })
        assert _clean_ticker(row) == "MSFT"

    def test_falls_back_to_ticker(self):
        row = pd.Series({
            "Security Ticker": None, "Ticker": "TSLA US Equity"
        })
        assert _clean_ticker(row) == "TSLA"

    def test_empty_security_ticker_falls_back(self):
        row = pd.Series({
            "Security Ticker": "   ", "Ticker": "GOOG US Equity"
        })
        assert _clean_ticker(row) == "GOOG"

    def test_no_suffix_to_strip(self):
        row = pd.Series({
            "Security Ticker": "AMZN", "Ticker": ""
        })
        assert _clean_ticker(row) == "AMZN"

    def test_case_insensitive_suffix(self):
        row = pd.Series({
            "Security Ticker": "NVDA us equity", "Ticker": ""
        })
        assert _clean_ticker(row) == "NVDA"


# ── _ticker_prefix ─────────────────────────────────────────────────


class TestTickerPrefix:
    def test_stock_uses_security_ticker(self):
        row = pd.Series({
            "is_option": False,
            "Security Ticker": "AAPL US Equity",
            "Ticker": "AAPL US Equity",
        })
        assert _ticker_prefix(row) == "AAPL"

    def test_option_uses_ticker(self):
        row = pd.Series({
            "is_option": True,
            "Security Ticker": "something",
            "Ticker": "QQQ US 02/27/26 P600 Equity",
        })
        assert _ticker_prefix(row) == "QQQ"

    def test_empty_ticker(self):
        row = pd.Series({
            "is_option": False,
            "Security Ticker": None,
            "Ticker": "",
        })
        assert _ticker_prefix(row) == ""


# ── _parse_ignore_sets and is_name_ignored ─────────────────────────


class TestIgnoreNames:
    def test_parse_ignore_sets_separates_option_suffix(self):
        with patch("src.portfolio.IGNORE_NAMES", [
            "COMPANY A",
            "COMPANY B (OPTION)",
        ]):
            ignore_all, ignore_opt = _parse_ignore_sets()
            assert "COMPANY A" in ignore_all
            assert "COMPANY B" in ignore_opt
            assert "COMPANY B (OPTION)" not in ignore_all

    def test_is_name_ignored_all(self):
        assert is_name_ignored("ENPLAS CORP", is_option=False) is True
        assert is_name_ignored("ENPLAS CORP", is_option=True) is True

    def test_is_name_ignored_option_only(self):
        assert is_name_ignored("TIC SOLUTIONS INC", is_option=True) is True
        assert is_name_ignored("TIC SOLUTIONS INC", is_option=False) is False

    def test_is_name_ignored_case_insensitive(self):
        assert is_name_ignored("enplas corp", is_option=False) is True

    def test_is_name_ignored_with_whitespace(self):
        assert is_name_ignored("  ENPLAS CORP  ", is_option=False) is True

    def test_not_ignored(self):
        assert is_name_ignored("APPLE INC", is_option=False) is False


# ── _latest_xlsx ───────────────────────────────────────────────────


class TestLatestXlsx:
    def test_returns_most_recent(self, tmp_path):
        import time
        f1 = tmp_path / "old.xlsx"
        f1.write_bytes(b"fake")
        time.sleep(0.05)
        f2 = tmp_path / "new.xlsx"
        f2.write_bytes(b"fake")
        assert _latest_xlsx(str(tmp_path)) == str(f2)

    def test_raises_when_no_xlsx(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="No .xlsx"):
            _latest_xlsx(str(tmp_path))

    def test_ignores_tilde_prefixed(self, tmp_path):
        (tmp_path / "~$locked.xlsx").write_bytes(b"lock")
        (tmp_path / "real.xlsx").write_bytes(b"data")
        assert _latest_xlsx(str(tmp_path)).endswith("real.xlsx")


# ── _apply_ticker_redirects ────────────────────────────────────────


class TestApplyTickerRedirects:
    def test_no_redirects_returns_unchanged(self):
        df = pd.DataFrame({
            "Ticker": ["AAPL US Equity"],
            "Security Ticker": ["AAPL US Equity"],
            "is_option": [False],
            "Basket Allocation": [5.0],
        })
        with patch("src.portfolio.OPTION_TICKER_REDIRECTS", {}), \
             patch("src.portfolio.STOCK_TICKER_REDIRECTS", {}):
            result = _apply_ticker_redirects(df)
            assert len(result) == 1

    def test_option_redirect_merges_allocation(self):
        df = pd.DataFrame({
            "Ticker": [
                "SPXW US 03/21/26 P5000 Equity",
                "QQQ US 03/21/26 P500 Equity",
            ],
            "Security Ticker": [None, None],
            "is_option": [True, True],
            "Basket Allocation": [2.0, 3.0],
        })
        with patch("src.portfolio.OPTION_TICKER_REDIRECTS", {"SPXW": "QQQ"}), \
             patch("src.portfolio.STOCK_TICKER_REDIRECTS", {}):
            result = _apply_ticker_redirects(df)
            assert len(result) == 1
            assert result.iloc[0]["Basket Allocation"] == 5.0

    def test_redirect_no_target_rows_skips(self):
        df = pd.DataFrame({
            "Ticker": ["SPXW US 03/21/26 P5000 Equity"],
            "Security Ticker": [None],
            "is_option": [True],
            "Basket Allocation": [2.0],
        })
        with patch("src.portfolio.OPTION_TICKER_REDIRECTS", {"SPXW": "XYZ"}), \
             patch("src.portfolio.STOCK_TICKER_REDIRECTS", {}):
            result = _apply_ticker_redirects(df)
            assert len(result) == 1  # source row preserved

    def test_redirect_proportional_split(self):
        df = pd.DataFrame({
            "Ticker": [
                "SRC US 03/21/26 P100 Equity",
                "TGT US 03/21/26 P200 Equity",
                "TGT US 03/21/26 C300 Equity",
            ],
            "Security Ticker": [None, None, None],
            "is_option": [True, True, True],
            "Basket Allocation": [4.0, 6.0, 4.0],
        })
        with patch("src.portfolio.OPTION_TICKER_REDIRECTS", {"SRC": "TGT"}), \
             patch("src.portfolio.STOCK_TICKER_REDIRECTS", {}):
            result = _apply_ticker_redirects(df)
            assert len(result) == 2
            total = result["Basket Allocation"].sum()
            assert abs(total - 14.0) < 0.01

    def test_redirect_zero_target_allocation_splits_evenly(self):
        df = pd.DataFrame({
            "Ticker": [
                "SRC US 03/21/26 P100 Equity",
                "TGT US 03/21/26 P200 Equity",
                "TGT US 03/21/26 C300 Equity",
            ],
            "Security Ticker": [None, None, None],
            "is_option": [True, True, True],
            "Basket Allocation": [4.0, 0.0, 0.0],
        })
        with patch("src.portfolio.OPTION_TICKER_REDIRECTS", {"SRC": "TGT"}), \
             patch("src.portfolio.STOCK_TICKER_REDIRECTS", {}):
            result = _apply_ticker_redirects(df)
            assert len(result) == 2
            assert result.iloc[0]["Basket Allocation"] == 2.0
            assert result.iloc[1]["Basket Allocation"] == 2.0


# ── load_portfolio (integration-ish) ──────────────────────────────


class TestLoadPortfolio:
    def test_loads_from_explicit_path(self, tmp_path):
        xlsx_path = tmp_path / "test_holdings.xlsx"
        df = pd.DataFrame({
            "Ticker": ["AAPL US Equity", "MSFT US Equity"],
            "Security Ticker": ["AAPL US Equity", "MSFT US Equity"],
            "Name": ["APPLE INC", "MICROSOFT CORP"],
            "Basket Allocation": [5.0, 3.0],
            "MIC Primary Exchange": ["XNAS", "XNAS"],
        })
        df.to_excel(str(xlsx_path), index=False, engine="openpyxl")

        with patch("src.portfolio.IGNORE_NAMES", []):
            result = load_portfolio(str(xlsx_path))

        assert len(result) == 2
        assert "is_option" in result.columns
        assert "clean_ticker" in result.columns
        assert result["is_option"].sum() == 0

    def test_filters_empty_name_and_dashes(self, tmp_path):
        xlsx_path = tmp_path / "test.xlsx"
        df = pd.DataFrame({
            "Ticker": ["AAPL US Equity", "", "CASH"],
            "Security Ticker": ["AAPL US Equity", "", ""],
            "Name": ["APPLE INC", "", "-"],
            "Basket Allocation": [5.0, 0.0, 0.0],
            "MIC Primary Exchange": ["XNAS", "", ""],
        })
        df.to_excel(str(xlsx_path), index=False, engine="openpyxl")

        with patch("src.portfolio.IGNORE_NAMES", []):
            result = load_portfolio(str(xlsx_path))

        assert len(result) == 1

    def test_coerces_non_numeric_allocation(self, tmp_path):
        xlsx_path = tmp_path / "test.xlsx"
        df = pd.DataFrame({
            "Ticker": ["AAPL US Equity"],
            "Security Ticker": ["AAPL US Equity"],
            "Name": ["APPLE INC"],
            "Basket Allocation": ["not_a_number"],
            "MIC Primary Exchange": ["XNAS"],
        })
        df.to_excel(str(xlsx_path), index=False, engine="openpyxl")

        with patch("src.portfolio.IGNORE_NAMES", []):
            result = load_portfolio(str(xlsx_path))

        assert pd.isna(result.iloc[0]["Basket Allocation"])

    def test_detects_options_in_loaded_data(self, tmp_path):
        xlsx_path = tmp_path / "test.xlsx"
        df = pd.DataFrame({
            "Ticker": ["AAPL US Equity", "QQQ US 03/21/26 P500 Equity"],
            "Security Ticker": ["AAPL US Equity", None],
            "Name": ["APPLE INC", "March 26 Puts on QQQ"],
            "Basket Allocation": [5.0, 2.0],
            "MIC Primary Exchange": ["XNAS", "XNAS"],
        })
        df.to_excel(str(xlsx_path), index=False, engine="openpyxl")

        with patch("src.portfolio.IGNORE_NAMES", []):
            result = load_portfolio(str(xlsx_path))

        assert result.iloc[1]["is_option"] == True
        assert result.iloc[0]["is_option"] == False

    def test_filters_ignored_names(self, tmp_path):
        xlsx_path = tmp_path / "test.xlsx"
        df = pd.DataFrame({
            "Ticker": ["AAPL US Equity", "ENP JP Equity"],
            "Security Ticker": ["AAPL US Equity", "ENP JP Equity"],
            "Name": ["APPLE INC", "ENPLAS CORP"],
            "Basket Allocation": [5.0, 2.0],
            "MIC Primary Exchange": ["XNAS", "XTKS"],
        })
        df.to_excel(str(xlsx_path), index=False, engine="openpyxl")

        result = load_portfolio(str(xlsx_path))
        assert len(result) == 1
        assert result.iloc[0]["Name"] == "APPLE INC"

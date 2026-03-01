"""Tests for src/contracts.py — exchange mapping, dedup, stock/option resolution."""

from unittest.mock import patch, MagicMock

import pandas as pd
from tests.conftest import (
    MockContract, MockContractDetails, MockMatchingSymbol, MockPosition,
)
from src.contracts import (
    exchange_to_mic,
    _mics_of,
    _safe_mic,
    _dedup_rule_ids,
    _result_from,
    _query_on_exchanges,
    _resolve_stock,
    _resolve_direct,
    _resolve_redirected,
    _resolve_option,
    resolve_conids,
    _IBKR_TO_MIC,
    _MIC_TO_IBKR,
    _REDIRECT_MICS,
)


# ── exchange_to_mic ────────────────────────────────────────────────


class TestExchangeToMic:
    def test_known_exchange(self):
        assert exchange_to_mic("NYSE") == "XNYS"
        assert exchange_to_mic("NASDAQ") == "XNAS"
        assert exchange_to_mic("TSEJ") == "XTKS"
        assert exchange_to_mic("SEHK") == "XHKG"

    def test_case_insensitive(self):
        assert exchange_to_mic("nyse") == "XNYS"

    def test_unknown_exchange_returns_uppercased(self):
        assert exchange_to_mic("UNKNOWN") == "UNKNOWN"

    def test_empty_string(self):
        assert exchange_to_mic("") == ""


# ── _mics_of ──────────────────────────────────────────────────────


class TestMicsOf:
    def test_known_primary_exchange(self):
        c = MockContract(primaryExchange="NYSE")
        result = _mics_of(c)
        assert "XNYS" in result

    def test_unknown_primary_exchange(self):
        c = MockContract(primaryExchange="CUSTOM")
        result = _mics_of(c)
        assert result == ["CUSTOM"]

    def test_empty_primary_exchange(self):
        c = MockContract(primaryExchange="")
        result = _mics_of(c)
        assert result == [""]


# ── _safe_mic ──────────────────────────────────────────────────────


class TestSafeMic:
    def test_none(self):
        assert _safe_mic(None) is None

    def test_nan(self):
        assert _safe_mic(float("nan")) is None

    def test_empty_string(self):
        assert _safe_mic("") is None

    def test_whitespace(self):
        assert _safe_mic("   ") is None

    def test_valid_mic(self):
        assert _safe_mic("xnas") == "XNAS"

    def test_with_whitespace(self):
        assert _safe_mic("  XNYS  ") == "XNYS"


# ── _dedup_rule_ids ────────────────────────────────────────────────


class TestDedupRuleIds:
    def test_none(self):
        assert _dedup_rule_ids(None) == ""

    def test_empty_string(self):
        assert _dedup_rule_ids("") == ""

    def test_no_duplicates(self):
        assert _dedup_rule_ids("26,240") == "26,240"

    def test_removes_duplicates(self):
        assert _dedup_rule_ids("26,240,26,240") == "26,240"

    def test_preserves_order(self):
        assert _dedup_rule_ids("240,26,240") == "240,26"

    def test_strips_whitespace(self):
        assert _dedup_rule_ids(" 26 , 240 , 26 ") == "26,240"

    def test_handles_empty_segments(self):
        assert _dedup_rule_ids("26,,240,") == "26,240"


# ── _result_from ───────────────────────────────────────────────────


class TestResultFrom:
    def test_basic_result(self):
        c = MockContract(conId=123, symbol="AAPL",
                         primaryExchange="NASDAQ", currency="USD")
        cd = MockContractDetails(contract=c, longName="APPLE INC",
                                 marketRuleIds="26,240")
        conid, name, sym, mic, ccy, mrids = _result_from(cd)
        assert conid == 123
        assert name == "APPLE INC"
        assert sym == "AAPL"
        assert mic == "XNAS"
        assert ccy == "USD"
        assert mrids == "26,240"

    def test_effective_mic_override(self):
        c = MockContract(conId=456, primaryExchange="FWB2")
        cd = MockContractDetails(contract=c, longName="TEST", marketRuleIds="")
        _, _, _, mic, _, _ = _result_from(cd, eff_mic="OTCM")
        assert mic == "OTCM"


# ── _query_on_exchanges ───────────────────────────────────────────


class TestQueryOnExchanges:
    def test_tries_specific_exchange_first(self, mock_ib):
        cd = MockContractDetails(
            contract=MockContract(conId=1), longName="TEST")
        mock_ib.reqContractDetails.return_value = [cd]
        result = _query_on_exchanges(mock_ib, "AAPL", "XNAS")
        assert len(result) == 1

    def test_falls_back_to_smart(self, mock_ib):
        call_count = [0]
        def side_effect(contract):
            call_count[0] += 1
            if call_count[0] == 1:
                return []  # specific exchange fails
            return [MockContractDetails(
                contract=MockContract(conId=1), longName="TEST")]
        mock_ib.reqContractDetails.side_effect = side_effect

        result = _query_on_exchanges(mock_ib, "AAPL", "XNYS")
        assert len(result) == 1

    def test_no_mic_goes_to_smart(self, mock_ib):
        cd = MockContractDetails(
            contract=MockContract(conId=1), longName="TEST")
        mock_ib.reqContractDetails.return_value = [cd]
        result = _query_on_exchanges(mock_ib, "AAPL", None)
        assert len(result) == 1


# ── _resolve_stock ─────────────────────────────────────────────────


class TestResolveStock:
    def test_resolves_by_ticker(self, mock_ib):
        c = MockContract(conId=265598, symbol="AAPL",
                         primaryExchange="NASDAQ", currency="USD")
        cd = MockContractDetails(contract=c, longName="APPLE INC",
                                 marketRuleIds="26")
        mock_ib.reqContractDetails.return_value = [cd]

        result = _resolve_direct(mock_ib, "AAPL", None, "APPLE INC")
        assert result is not None
        assert result[0] == 265598

    def test_resolves_by_name_fallback(self, mock_ib):
        calls = [0]
        def req_details(contract):
            calls[0] += 1
            if calls[0] <= 1:
                return []  # ticker search fails
            c = MockContract(conId=999, symbol="AAPL2",
                             primaryExchange="NASDAQ", currency="USD")
            return [MockContractDetails(contract=c, longName="APPLE",
                                        marketRuleIds="")]
        mock_ib.reqContractDetails.side_effect = req_details

        ms = MockMatchingSymbol()
        ms.contract = MockContract(conId=0, symbol="AAPL2", secType="STK")
        mock_ib.reqMatchingSymbols.return_value = [ms]

        result = _resolve_direct(mock_ib, "BADTICKER", None, "APPLE INC")
        assert result is not None

    def test_returns_none_when_unresolvable(self, mock_ib):
        mock_ib.reqContractDetails.return_value = []
        mock_ib.reqMatchingSymbols.return_value = []
        result = _resolve_direct(mock_ib, "FAKE", None, "FAKE CORP")
        assert result is None


class TestResolveRedirected:
    def test_falls_back_to_original_when_no_redirect_hits(self, mock_ib):
        mock_ib.reqMatchingSymbols.return_value = []
        result = _resolve_redirected(
            mock_ib, "SONY", "XTKS", "SONY GROUP CORP",
            ["XFRA", "OTCM"], {})
        assert result is None  # no candidates → falls back → also fails

    def test_returns_none_when_no_name(self, mock_ib):
        result = _resolve_redirected(
            mock_ib, "SONY", "XTKS", None, ["XFRA"], {})
        assert result is None


# ── _resolve_option ────────────────────────────────────────────────


class TestResolveOption:
    def test_valid_option(self, mock_ib):
        und_c = MockContract(conId=100, symbol="QQQ",
                             primaryExchange="NASDAQ", currency="USD")
        und_cd = MockContractDetails(contract=und_c, longName="QQQ")
        opt_c = MockContract(conId=200, symbol="QQQ",
                             primaryExchange="", currency="USD",
                             lastTradeDateOrContractMonth="20260321")
        opt_cd = MockContractDetails(contract=opt_c,
                                     longName="QQQ P500",
                                     marketRuleIds="26")
        calls = [0]
        def req_details(contract):
            calls[0] += 1
            if calls[0] == 1:
                return [und_cd]
            return [opt_cd]
        mock_ib.reqContractDetails.side_effect = req_details

        result = _resolve_option(
            mock_ib, "QQQ US 03/21/26 P500 Equity", "XNAS", "Puts on QQQ")
        assert result is not None
        assert result[0] == 200

    def test_invalid_ticker_format(self, mock_ib):
        result = _resolve_option(
            mock_ib, "NOT AN OPTION", "XNAS", "Some Name")
        assert result is None

    def test_underlying_not_found(self, mock_ib):
        mock_ib.reqContractDetails.return_value = []
        result = _resolve_option(
            mock_ib, "XYZ US 03/21/26 P100 Equity", "XNAS", None)
        assert result is None

    def test_option_contract_not_found(self, mock_ib):
        und_c = MockContract(conId=100, symbol="QQQ",
                             primaryExchange="NASDAQ", currency="USD")
        und_cd = MockContractDetails(contract=und_c, longName="QQQ")
        calls = [0]
        def req_details(contract):
            calls[0] += 1
            if calls[0] == 1:
                return [und_cd]
            return []  # option not found
        mock_ib.reqContractDetails.side_effect = req_details

        result = _resolve_option(
            mock_ib, "QQQ US 03/21/26 P500 Equity", "XNAS", None)
        assert result is None

    def test_fallback_underlying_from_name(self, mock_ib):
        """When underlying ticker lookup fails, extract from Name."""
        und_c = MockContract(conId=100, symbol="SPX",
                             primaryExchange="", currency="USD")
        und_cd = MockContractDetails(contract=und_c, longName="SPX")
        opt_c = MockContract(conId=300, symbol="SPX", currency="USD",
                             lastTradeDateOrContractMonth="20260321")
        opt_cd = MockContractDetails(contract=opt_c, longName="SPX P5000",
                                     marketRuleIds="")
        calls = [0]
        def req_details(contract):
            calls[0] += 1
            if calls[0] == 1:
                return []  # SPXW underlying fails
            elif calls[0] == 2:
                return [und_cd]  # SPX from name works
            return [opt_cd]
        mock_ib.reqContractDetails.side_effect = req_details

        result = _resolve_option(
            mock_ib, "SPXW US 03/21/26 P5000 Equity",
            "XNAS", "March 26 Puts on SPX")
        assert result is not None
        assert result[0] == 300


# ── resolve_conids ─────────────────────────────────────────────────


class TestResolveConids:
    def test_adds_expected_columns(self, mock_ib, sample_portfolio_df):
        mock_ib.positions.return_value = []
        c = MockContract(conId=265598, symbol="AAPL",
                         primaryExchange="NASDAQ", currency="USD")
        cd = MockContractDetails(contract=c, longName="APPLE INC",
                                 marketRuleIds="26")
        mock_ib.reqContractDetails.return_value = [cd]

        result = resolve_conids(mock_ib, sample_portfolio_df)

        for col in ("conid", "IBKR Name", "IBKR Ticker", "currency",
                     "market_rule_ids", "Name Mismatch"):
            assert col in result.columns

    def test_flags_name_mismatch(self, mock_ib):
        df = pd.DataFrame({
            "Ticker": ["AAPL US Equity"],
            "Security Ticker": ["AAPL US Equity"],
            "Name": ["APPLE"],
            "Basket Allocation": [5.0],
            "MIC Primary Exchange": ["XNAS"],
            "is_option": [False],
            "clean_ticker": ["AAPL"],
        })
        mock_ib.positions.return_value = []
        c = MockContract(conId=1, symbol="AAPL",
                         primaryExchange="NASDAQ", currency="USD")
        cd = MockContractDetails(contract=c, longName="APPLE INC",
                                 marketRuleIds="")
        mock_ib.reqContractDetails.return_value = [cd]

        result = resolve_conids(mock_ib, df)
        assert result.iloc[0]["Name Mismatch"] == True


# ── Exchange mapping sanity ────────────────────────────────────────


class TestExchangeMappings:
    def test_ibkr_to_mic_has_entries(self):
        assert len(_IBKR_TO_MIC) > 20

    def test_mic_to_ibkr_reverse_mapping(self):
        assert "XNYS" in _MIC_TO_IBKR
        assert "NYSE" in _MIC_TO_IBKR["XNYS"]

    def test_redirect_mics_defined(self):
        assert "XTKS" in _REDIRECT_MICS
        assert "XHKG" in _REDIRECT_MICS

    def test_fwb2_preferred_for_xfra(self):
        assert _MIC_TO_IBKR["XFRA"][0] == "FWB2"

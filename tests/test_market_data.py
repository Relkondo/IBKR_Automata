"""Tests for src/market_data.py — safe_float, limit prices, tick snapping, FX, qty."""

import math
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from tests.conftest import (
    MockAccountValue, MockContract, MockContractDetails,
    MockPriceIncrement, MockTicker,
)
from src.market_data import (
    _safe_float,
    calc_limit_price,
    snap_to_tick,
    _applicable_increment,
    _fetch_single_rule,
    _market_rule_cache,
    _ensure_market_rules,
    _try_forex_snapshot,
    get_fx,
    _multiplier,
    _planned_qty,
    _actual_dollar_alloc,
    _snap_limit_price,
    get_investable_amount,
    save_project_portfolio,
    snapshot_batch,
    fetch_market_data,
    _fetch_web_fx_rate,
    resolve_fx_rate,
    resolve_currencies,
)


# ── _safe_float ────────────────────────────────────────────────────


class TestSafeFloat:
    def test_none(self):
        assert _safe_float(None) is None

    def test_nan(self):
        assert _safe_float(float("nan")) is None

    def test_inf(self):
        assert _safe_float(float("inf")) is None

    def test_negative_inf(self):
        assert _safe_float(float("-inf")) is None

    def test_negative_sentinel(self):
        assert _safe_float(-1) is None

    def test_negative_value(self):
        assert _safe_float(-0.5) is None

    def test_zero(self):
        assert _safe_float(0) == 0.0

    def test_positive_float(self):
        assert _safe_float(123.45) == 123.45

    def test_positive_int(self):
        assert _safe_float(42) == 42.0

    def test_string_number(self):
        assert _safe_float("100.5") == 100.5

    def test_invalid_string(self):
        assert _safe_float("not_a_number") is None

    def test_empty_string(self):
        assert _safe_float("") is None


# ── _applicable_increment ──────────────────────────────────────────


class TestApplicableIncrement:
    def test_empty_rules(self):
        assert _applicable_increment([], 100.0) == 0.0

    def test_single_rule(self):
        rules = [(0.0, 0.01)]
        assert _applicable_increment(rules, 50.0) == 0.01

    def test_tiered_rules(self):
        rules = [(0.0, 0.01), (1.0, 0.05), (100.0, 0.10)]
        assert _applicable_increment(rules, 0.5) == 0.01
        assert _applicable_increment(rules, 50.0) == 0.05
        assert _applicable_increment(rules, 100.0) == 0.10
        assert _applicable_increment(rules, 500.0) == 0.10

    def test_price_at_exact_boundary(self):
        rules = [(0.0, 0.01), (10.0, 0.05)]
        assert _applicable_increment(rules, 10.0) == 0.05

    def test_price_just_below_boundary(self):
        rules = [(0.0, 0.01), (10.0, 0.05)]
        assert _applicable_increment(rules, 9.99) == 0.01


# ── snap_to_tick ───────────────────────────────────────────────────


class TestSnapToTick:
    def setup_method(self):
        _market_rule_cache.clear()

    def test_empty_rule_ids(self, mock_ib):
        assert snap_to_tick(100.0, mock_ib, "", is_buy=True) == 100.0

    def test_zero_price(self, mock_ib):
        assert snap_to_tick(0.0, mock_ib, "26", is_buy=True) == 0.0

    def test_negative_price(self, mock_ib):
        assert snap_to_tick(-5.0, mock_ib, "26", is_buy=True) == -5.0

    def test_buy_rounds_down(self, mock_ib):
        mock_ib.reqMarketRule.return_value = [
            MockPriceIncrement(lowEdge=0.0, increment=0.05)
        ]
        result = snap_to_tick(100.03, mock_ib, "99", is_buy=True)
        assert result == 100.0

    def test_sell_rounds_up(self, mock_ib):
        mock_ib.reqMarketRule.return_value = [
            MockPriceIncrement(lowEdge=0.0, increment=0.05)
        ]
        result = snap_to_tick(100.03, mock_ib, "99", is_buy=False)
        assert result == pytest.approx(100.05)

    def test_multiple_rule_ids_uses_most_restrictive(self, mock_ib):
        call_count = [0]
        def mock_rule(rid):
            call_count[0] += 1
            if rid == 1:
                return [MockPriceIncrement(lowEdge=0.0, increment=0.01)]
            else:
                return [MockPriceIncrement(lowEdge=0.0, increment=0.05)]
        mock_ib.reqMarketRule.side_effect = mock_rule
        _market_rule_cache.clear()

        result = snap_to_tick(100.03, mock_ib, "1,2", is_buy=True)
        assert result == 100.0  # 0.05 is the most restrictive

    def test_caches_market_rules(self, mock_ib):
        mock_ib.reqMarketRule.return_value = [
            MockPriceIncrement(lowEdge=0.0, increment=0.01)
        ]
        _market_rule_cache.clear()
        snap_to_tick(100.0, mock_ib, "42", is_buy=True)
        snap_to_tick(100.0, mock_ib, "42", is_buy=True)
        assert mock_ib.reqMarketRule.call_count == 1

    def test_no_rules_returned(self, mock_ib):
        mock_ib.reqMarketRule.return_value = []
        _market_rule_cache.clear()
        assert snap_to_tick(100.03, mock_ib, "50", is_buy=True) == 100.03

    def test_rule_fetch_failure(self, mock_ib):
        mock_ib.reqMarketRule.side_effect = Exception("timeout")
        _market_rule_cache.clear()
        assert snap_to_tick(100.03, mock_ib, "50", is_buy=True) == 100.03


# ── calc_limit_price ───────────────────────────────────────────────


class TestCalcLimitPrice:
    def test_buy_uses_bid_with_offset(self):
        row = {"bid": 100.0, "ask": 101.0, "last": 100.5,
               "close": 99.0, "Dollar Allocation": 5000.0}
        price = calc_limit_price(row)
        assert price == round(100.0 * 1.02, 2)  # LIMIT_PRICE_OFFSET=2

    def test_sell_uses_ask_with_offset(self):
        row = {"bid": 100.0, "ask": 101.0, "last": 100.5,
               "close": 99.0, "Dollar Allocation": -5000.0}
        price = calc_limit_price(row)
        assert price == round(101.0 * 0.98, 2)

    def test_explicit_is_sell(self):
        row = {"bid": 100.0, "ask": 101.0, "last": 100.5,
               "close": 99.0, "Dollar Allocation": 5000.0}
        price = calc_limit_price(row, is_sell=True)
        assert price == round(101.0 * 0.98, 2)

    def test_fallback_to_last(self):
        row = {"bid": None, "ask": None, "last": 100.0,
               "close": 99.0, "Dollar Allocation": 5000.0}
        price = calc_limit_price(row)
        assert price == round(100.0 * 1.02, 2)

    def test_fallback_to_close(self):
        row = {"bid": None, "ask": None, "last": None,
               "close": 99.0, "Dollar Allocation": 5000.0}
        price = calc_limit_price(row)
        assert price == round(99.0 * 1.02, 2)

    def test_fallback_to_any_price_no_offset(self):
        row = {"bid": 100.0, "ask": None, "last": None,
               "close": None, "Dollar Allocation": -5000.0}
        price = calc_limit_price(row)
        assert price == 100.0  # bid available but we're selling → fallback 3

    def test_all_prices_missing(self):
        row = {"bid": None, "ask": None, "last": None,
               "close": None, "Dollar Allocation": 5000.0}
        assert calc_limit_price(row) is None

    def test_all_prices_zero(self):
        row = {"bid": 0.0, "ask": 0.0, "last": 0.0,
               "close": 0.0, "Dollar Allocation": 5000.0}
        assert calc_limit_price(row) is None

    def test_nan_prices_treated_as_missing(self):
        row = {"bid": float("nan"), "ask": float("nan"),
               "last": float("nan"), "close": float("nan"),
               "Dollar Allocation": 5000.0}
        assert calc_limit_price(row) is None

    def test_no_dollar_allocation_defaults_to_buy(self):
        row = {"bid": 100.0, "ask": 101.0, "last": 100.5,
               "close": 99.0, "Dollar Allocation": None}
        price = calc_limit_price(row)
        # is_sell = False when Dollar Allocation is None/NaN
        assert price == round(100.0 * 1.02, 2)


# ── get_fx ─────────────────────────────────────────────────────────


class TestGetFx:
    def test_usd_returns_one(self):
        assert get_fx({"currency": "USD", "fx_rate": None}) == 1.0

    def test_usd_case_insensitive(self):
        assert get_fx({"currency": "usd", "fx_rate": None}) == 1.0

    def test_foreign_with_rate(self):
        assert get_fx({"currency": "EUR", "fx_rate": 0.92}) == 0.92

    def test_foreign_without_rate(self):
        assert get_fx({"currency": "JPY", "fx_rate": None}) is None

    def test_foreign_zero_rate(self):
        assert get_fx({"currency": "GBP", "fx_rate": 0.0}) is None

    def test_missing_currency(self):
        assert get_fx({"currency": None, "fx_rate": 1.5}) == 1.0


# ── _multiplier ────────────────────────────────────────────────────


class TestMultiplier:
    def test_stock(self):
        assert _multiplier({"is_option": False}) == 1

    def test_option(self):
        assert _multiplier({"is_option": True}) == 100

    def test_falsy_is_option(self):
        assert _multiplier({"is_option": None}) == 1
        assert _multiplier({"is_option": 0}) == 1
        assert _multiplier({}) == 1


# ── _planned_qty ───────────────────────────────────────────────────


class TestPlannedQty:
    def test_basic_buy(self):
        row = {"limit_price": 100.0, "Dollar Allocation": 5000.0,
               "currency": "USD", "fx_rate": 1.0, "is_option": False}
        assert _planned_qty(row) == 50

    def test_sell(self):
        row = {"limit_price": 100.0, "Dollar Allocation": -3000.0,
               "currency": "USD", "fx_rate": 1.0, "is_option": False}
        assert _planned_qty(row) == -30

    def test_option_multiplier(self):
        row = {"limit_price": 5.0, "Dollar Allocation": 1000.0,
               "currency": "USD", "fx_rate": 1.0, "is_option": True}
        # 1000 / (5 * 100) = 2
        assert _planned_qty(row) == 2

    def test_foreign_currency(self):
        row = {"limit_price": 1000.0, "Dollar Allocation": 5000.0,
               "currency": "JPY", "fx_rate": 150.0, "is_option": False}
        # local_alloc = 5000 * 150 = 750000; qty = 750000 / 1000 = 750
        assert _planned_qty(row) == 750

    def test_zero_limit_price(self):
        row = {"limit_price": 0.0, "Dollar Allocation": 5000.0,
               "currency": "USD", "fx_rate": 1.0, "is_option": False}
        assert _planned_qty(row) is None

    def test_missing_limit_price(self):
        row = {"limit_price": None, "Dollar Allocation": 5000.0,
               "currency": "USD", "fx_rate": 1.0, "is_option": False}
        assert _planned_qty(row) is None

    def test_missing_dollar_allocation(self):
        row = {"limit_price": 100.0, "Dollar Allocation": None,
               "currency": "USD", "fx_rate": 1.0, "is_option": False}
        assert _planned_qty(row) is None

    def test_no_fx_rate(self):
        row = {"limit_price": 100.0, "Dollar Allocation": 5000.0,
               "currency": "EUR", "fx_rate": None, "is_option": False}
        assert _planned_qty(row) is None


# ── _actual_dollar_alloc ───────────────────────────────────────────


class TestActualDollarAlloc:
    def test_basic(self):
        row = {"limit_price": 100.0, "Qty": 50,
               "currency": "USD", "fx_rate": 1.0, "is_option": False}
        assert _actual_dollar_alloc(row) == 5000.0

    def test_option(self):
        row = {"limit_price": 5.0, "Qty": 2,
               "currency": "USD", "fx_rate": 1.0, "is_option": True}
        assert _actual_dollar_alloc(row) == 1000.0

    def test_foreign_currency(self):
        row = {"limit_price": 1000.0, "Qty": 10,
               "currency": "JPY", "fx_rate": 150.0, "is_option": False}
        assert _actual_dollar_alloc(row) == round(10000.0 / 150.0, 2)

    def test_missing_qty(self):
        row = {"limit_price": 100.0, "Qty": None,
               "currency": "USD", "fx_rate": 1.0, "is_option": False}
        assert _actual_dollar_alloc(row) is None

    def test_missing_limit_price(self):
        row = {"limit_price": None, "Qty": 50,
               "currency": "USD", "fx_rate": 1.0, "is_option": False}
        assert _actual_dollar_alloc(row) is None


# ── _snap_limit_price ──────────────────────────────────────────────


class TestSnapLimitPrice:
    def setup_method(self):
        _market_rule_cache.clear()

    def test_nan_limit_price(self, mock_ib):
        row = pd.Series({"limit_price": float("nan"),
                         "market_rule_ids": "26", "Dollar Allocation": 5000})
        assert _snap_limit_price(row, mock_ib) is None

    def test_no_market_rules(self, mock_ib):
        row = pd.Series({"limit_price": 100.0,
                         "market_rule_ids": "", "Dollar Allocation": 5000})
        assert _snap_limit_price(row, mock_ib) == 100.0

    def test_nan_market_rules(self, mock_ib):
        row = pd.Series({"limit_price": 100.0,
                         "market_rule_ids": float("nan"),
                         "Dollar Allocation": 5000})
        assert _snap_limit_price(row, mock_ib) == 100.0


# ── get_investable_amount ──────────────────────────────────────────


class TestGetInvestableAmount:
    def test_returns_net_liq_minus_reserve(self, mock_ib):
        mock_ib.accountSummary.return_value = [
            MockAccountValue(tag="NetLiquidation", value="100000", currency="USD"),
        ]
        with patch("src.market_data.MINIMUM_CASH_RESERVE", 5000):
            result = get_investable_amount(mock_ib)
        assert result == 95000.0

    def test_zero_reserve(self, mock_ib):
        mock_ib.accountSummary.return_value = [
            MockAccountValue(tag="NetLiquidation", value="50000", currency="USD"),
        ]
        with patch("src.market_data.MINIMUM_CASH_RESERVE", 0):
            result = get_investable_amount(mock_ib)
        assert result == 50000.0

    def test_raises_when_no_net_liq(self, mock_ib):
        mock_ib.accountSummary.return_value = [
            MockAccountValue(tag="SomeOtherTag", value="100", currency="USD"),
        ]
        with pytest.raises(RuntimeError, match="NetLiquidation"):
            get_investable_amount(mock_ib)

    def test_skips_non_usd_net_liq(self, mock_ib):
        mock_ib.accountSummary.return_value = [
            MockAccountValue(tag="NetLiquidation", value="100000", currency="EUR"),
        ]
        with pytest.raises(RuntimeError, match="NetLiquidation"):
            get_investable_amount(mock_ib)

    def test_skips_zero_net_liq(self, mock_ib):
        mock_ib.accountSummary.return_value = [
            MockAccountValue(tag="NetLiquidation", value="0", currency="USD"),
        ]
        with pytest.raises(RuntimeError, match="NetLiquidation"):
            get_investable_amount(mock_ib)


# ── save_project_portfolio ─────────────────────────────────────────


class TestSaveProjectPortfolio:
    def test_saves_csv(self, tmp_path, enriched_portfolio_df):
        with patch("src.market_data.OUTPUT_DIR", str(tmp_path)):
            path = save_project_portfolio(enriched_portfolio_df)
        assert path.endswith("Project_Portfolio.csv")
        loaded = pd.read_csv(path)
        assert len(loaded) == 2

    def test_column_ordering(self, tmp_path, enriched_portfolio_df):
        with patch("src.market_data.OUTPUT_DIR", str(tmp_path)):
            path = save_project_portfolio(enriched_portfolio_df)
        loaded = pd.read_csv(path)
        assert loaded.columns[0] == "Ticker"


# ── snapshot_batch ─────────────────────────────────────────────────


class TestSnapshotBatch:
    def test_empty_contracts(self, mock_ib):
        assert snapshot_batch(mock_ib, []) == {}

    def test_returns_mapped_data(self, mock_ib):
        c = MockContract(conId=123, symbol="AAPL")
        ticker = MockTicker(
            contract=c, bid=175.0, ask=175.5,
            last=175.25, close=174.0, high=176.0, low=174.0,
        )
        mock_ib.reqTickers.return_value = [ticker]

        result = snapshot_batch(mock_ib, [c])
        assert 123 in result
        assert result[123]["bid"] == 175.0
        assert result[123]["ask"] == 175.5

    def test_filters_negative_sentinel(self, mock_ib):
        c = MockContract(conId=456, symbol="TEST")
        ticker = MockTicker(
            contract=c, bid=-1, ask=-1, last=100.0,
            close=99.0, high=-1, low=-1,
        )
        mock_ib.reqTickers.return_value = [ticker]

        result = snapshot_batch(mock_ib, [c])
        assert result[456]["bid"] is None
        assert result[456]["ask"] is None
        assert result[456]["last"] == 100.0

    def test_handles_reqTickers_exception(self, mock_ib):
        mock_ib.reqTickers.side_effect = Exception("network error")
        c = MockContract(conId=789)
        result = snapshot_batch(mock_ib, [c])
        assert result == {}

    def test_skips_ticker_without_contract(self, mock_ib):
        ticker = MockTicker(contract=None, bid=100.0)
        mock_ib.reqTickers.return_value = [ticker]
        c = MockContract(conId=1)
        result = snapshot_batch(mock_ib, [c])
        assert len(result) == 0


# ── resolve_fx_rate ────────────────────────────────────────────────


class TestResolveFxRate:
    def test_ibkr_snapshot_usd_ccy(self, mock_ib):
        """For non-base currencies (e.g. JPY), tries USD{ccy} first."""
        mock_ib.qualifyContracts.return_value = None
        fx_contract = MagicMock()
        fx_contract.conId = 12345
        mock_ib.qualifyContracts.side_effect = lambda c: setattr(c, 'conId', 12345)

        ticker = MockTicker(last=150.0, close=149.0)
        mock_ib.reqMktData.return_value = ticker

        rate = resolve_fx_rate(mock_ib, "JPY")
        assert rate == 150.0

    def test_auto_mode_raises_on_failure(self, mock_ib):
        mock_ib.qualifyContracts.side_effect = lambda c: setattr(c, 'conId', 0)
        with patch("src.market_data._fetch_web_fx_rate", return_value=None):
            with pytest.raises(RuntimeError, match="Could not resolve FX"):
                resolve_fx_rate(mock_ib, "XYZ", auto_mode=True)


# ── resolve_currencies ─────────────────────────────────────────────


class TestResolveCurrencies:
    def test_no_currency_column(self, mock_ib):
        df = pd.DataFrame({"Name": ["A"]})
        result = resolve_currencies(mock_ib, df)
        assert "fx_rate" in result.columns
        assert result["fx_rate"].iloc[0] is None

    def test_all_usd(self, mock_ib):
        df = pd.DataFrame({"currency": ["USD", "USD"]})
        result = resolve_currencies(mock_ib, df)
        assert result["fx_rate"].tolist() == [1.0, 1.0]

    def test_mixed_currencies(self, mock_ib):
        df = pd.DataFrame({"currency": ["USD", "EUR", "EUR"]})
        mock_ib.qualifyContracts.side_effect = lambda c: setattr(c, 'conId', 12345)
        ticker = MockTicker(last=1.08, close=1.07)
        mock_ib.reqMktData.return_value = ticker

        result = resolve_currencies(mock_ib, df)
        assert result["fx_rate"].iloc[0] == 1.0
        assert result["fx_rate"].iloc[1] is not None


# ── _fetch_web_fx_rate ─────────────────────────────────────────────


class TestFetchWebFxRate:
    def test_returns_cached_rate(self):
        import src.market_data as md
        old_cache = md._web_fx_cache
        try:
            md._web_fx_cache = {"JPY": 150.0, "EUR": 0.92}
            assert _fetch_web_fx_rate("JPY") == 150.0
            assert _fetch_web_fx_rate("EUR") == 0.92
        finally:
            md._web_fx_cache = old_cache

    def test_returns_none_for_missing_currency(self):
        import src.market_data as md
        old_cache = md._web_fx_cache
        try:
            md._web_fx_cache = {"JPY": 150.0}
            assert _fetch_web_fx_rate("XYZ") is None
        finally:
            md._web_fx_cache = old_cache


# ── _try_forex_snapshot ───────────────────────────────────────────


class TestTryForexSnapshot:
    def test_returns_rate_on_success(self, mock_ib):
        mock_ib.qualifyContracts.side_effect = lambda c: setattr(c, 'conId', 123)
        ticker = MockTicker(last=150.0, close=149.0)
        mock_ib.reqMktData.return_value = ticker

        rate = _try_forex_snapshot(mock_ib, "USDJPY")
        assert rate == 150.0

    def test_returns_none_when_unqualified(self, mock_ib):
        mock_ib.qualifyContracts.side_effect = lambda c: setattr(c, 'conId', 0)
        assert _try_forex_snapshot(mock_ib, "USDXYZ") is None

    def test_returns_none_when_no_price(self, mock_ib):
        mock_ib.qualifyContracts.side_effect = lambda c: setattr(c, 'conId', 123)
        ticker = MockTicker(last=-1, close=-1)
        mock_ib.reqMktData.return_value = ticker

        assert _try_forex_snapshot(mock_ib, "USDJPY") is None

    def test_uses_close_when_last_missing(self, mock_ib):
        mock_ib.qualifyContracts.side_effect = lambda c: setattr(c, 'conId', 123)
        ticker = MockTicker(last=-1, close=0.92)
        mock_ib.reqMktData.return_value = ticker

        rate = _try_forex_snapshot(mock_ib, "EURUSD")
        assert rate == 0.92


# ── _ensure_market_rules ──────────────────────────────────────────


class TestEnsureMarketRules:
    def test_returns_unchanged_when_rules_exist(self, mock_ib):
        df = pd.DataFrame({
            "conid": [123],
            "market_rule_ids": ["26,240"],
        })
        result = _ensure_market_rules(mock_ib, df, [])
        assert result["market_rule_ids"].iloc[0] == "26,240"
        mock_ib.reqContractDetails.assert_not_called()

    def test_fetches_rules_when_column_empty(self, mock_ib):
        df = pd.DataFrame({
            "conid": [123],
            "market_rule_ids": [""],
        })
        c = MockContract(conId=123)
        cd = MockContractDetails(contract=c, marketRuleIds="26,240")
        mock_ib.reqContractDetails.return_value = [cd]

        result = _ensure_market_rules(mock_ib, df, [c])
        assert result["market_rule_ids"].iloc[0] == "26,240"

    def test_fetches_rules_when_column_missing(self, mock_ib):
        df = pd.DataFrame({"conid": [123]})
        c = MockContract(conId=123)
        cd = MockContractDetails(contract=c, marketRuleIds="50")
        mock_ib.reqContractDetails.return_value = [cd]

        result = _ensure_market_rules(mock_ib, df, [c])
        assert "market_rule_ids" in result.columns

    def test_handles_exception_gracefully(self, mock_ib):
        df = pd.DataFrame({
            "conid": [123],
            "market_rule_ids": [""],
        })
        c = MockContract(conId=123)
        mock_ib.reqContractDetails.side_effect = Exception("timeout")

        result = _ensure_market_rules(mock_ib, df, [c])
        assert result["market_rule_ids"].iloc[0] == ""


# ── fetch_market_data ─────────────────────────────────────────────


class TestFetchMarketData:
    def test_no_valid_conids(self, mock_ib):
        df = pd.DataFrame({
            "conid": [None, None],
            "Name": ["A", "B"],
        })
        result = fetch_market_data(mock_ib, df)
        assert "bid" in result.columns
        assert "limit_price" in result.columns
        assert result["bid"].iloc[0] is None

    def test_fetches_and_populates_market_data(self, mock_ib):
        c = MockContract(conId=123, symbol="AAPL")
        mock_ib.qualifyContracts.return_value = [c]
        mock_ib.reqContractDetails.return_value = [
            MockContractDetails(contract=c, marketRuleIds="26")]

        ticker = MockTicker(
            contract=c, bid=175.0, ask=175.5,
            last=175.25, close=174.0, high=176.0, low=174.0)
        mock_ib.reqTickers.return_value = [ticker]
        mock_ib.reqMarketRule.return_value = [
            MockPriceIncrement(lowEdge=0.0, increment=0.01)]
        _market_rule_cache.clear()

        df = pd.DataFrame({
            "conid": [123],
            "currency": ["USD"],
            "fx_rate": [1.0],
            "is_option": [False],
            "Dollar Allocation": [5000.0],
            "market_rule_ids": ["26"],
        })

        result = fetch_market_data(mock_ib, df)
        assert result["bid"].iloc[0] == 175.0
        assert result["ask"].iloc[0] == 175.5
        assert result["last"].iloc[0] == 175.25
        assert result["limit_price"].iloc[0] is not None
        assert result["Qty"].iloc[0] is not None

    def test_handles_missing_snapshot(self, mock_ib):
        c = MockContract(conId=123, symbol="AAPL")
        mock_ib.qualifyContracts.return_value = [c]
        mock_ib.reqTickers.return_value = []
        mock_ib.reqContractDetails.return_value = []

        df = pd.DataFrame({
            "conid": [123],
            "currency": ["USD"],
            "fx_rate": [1.0],
            "is_option": [False],
            "Dollar Allocation": [5000.0],
        })

        result = fetch_market_data(mock_ib, df)
        assert result["bid"].iloc[0] is None
        assert result["limit_price"].iloc[0] is None


# ── resolve_fx_rate (additional paths) ────────────────────────────


class TestResolveFxRateAdditional:
    def test_ccy_as_base_inverts_rate(self, mock_ib):
        """EUR, GBP, AUD, NZD use inverted convention."""
        mock_ib.qualifyContracts.side_effect = lambda c: setattr(c, 'conId', 123)
        ticker = MockTicker(last=1.08, close=1.07)
        mock_ib.reqMktData.return_value = ticker

        rate = resolve_fx_rate(mock_ib, "EUR")
        expected = round(1.0 / 1.08, 6)
        assert rate == expected

    def test_falls_back_to_web_api(self, mock_ib):
        mock_ib.qualifyContracts.side_effect = lambda c: setattr(c, 'conId', 0)
        with patch("src.market_data._fetch_web_fx_rate", return_value=23.5):
            rate = resolve_fx_rate(mock_ib, "TWD")
        assert rate == 23.5

    def test_interactive_manual_input(self, mock_ib):
        mock_ib.qualifyContracts.side_effect = lambda c: setattr(c, 'conId', 0)
        with patch("src.market_data._fetch_web_fx_rate", return_value=None), \
             patch("builtins.input", return_value="150.0"):
            rate = resolve_fx_rate(mock_ib, "JPY", auto_mode=False)
        assert rate == 150.0

    def test_interactive_invalid_input_returns_none(self, mock_ib):
        mock_ib.qualifyContracts.side_effect = lambda c: setattr(c, 'conId', 0)
        with patch("src.market_data._fetch_web_fx_rate", return_value=None), \
             patch("builtins.input", return_value="abc"):
            rate = resolve_fx_rate(mock_ib, "JPY", auto_mode=False)
        assert rate is None

    def test_interactive_empty_input_returns_none(self, mock_ib):
        mock_ib.qualifyContracts.side_effect = lambda c: setattr(c, 'conId', 0)
        with patch("src.market_data._fetch_web_fx_rate", return_value=None), \
             patch("builtins.input", return_value=""):
            rate = resolve_fx_rate(mock_ib, "JPY", auto_mode=False)
        assert rate is None

    def test_interactive_negative_rate_returns_none(self, mock_ib):
        mock_ib.qualifyContracts.side_effect = lambda c: setattr(c, 'conId', 0)
        with patch("src.market_data._fetch_web_fx_rate", return_value=None), \
             patch("builtins.input", return_value="-5"):
            rate = resolve_fx_rate(mock_ib, "JPY", auto_mode=False)
        assert rate is None

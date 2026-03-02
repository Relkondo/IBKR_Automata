"""Tests for src/exchange_hours.py — open/closed, holidays, filtering."""

from datetime import datetime, time as dtime
from unittest.mock import patch, MagicMock
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from src.exchange_hours import (
    EXCHANGE_HOURS,
    _parse_time,
    is_exchange_open,
    filter_df_by_open_exchange,
    _get_calendar,
    _is_holiday,
    _calendar_cache,
    _XCAL_ALIAS,
)


# ── _parse_time ────────────────────────────────────────────────────


class TestParseTime:
    def test_standard(self):
        assert _parse_time("09:30") == dtime(9, 30)
        assert _parse_time("16:00") == dtime(16, 0)

    def test_midnight(self):
        assert _parse_time("00:00") == dtime(0, 0)

    def test_end_of_day(self):
        assert _parse_time("23:59") == dtime(23, 59)


# ── EXCHANGE_HOURS table ──────────────────────────────────────────


class TestExchangeHoursTable:
    def test_all_entries_have_four_fields(self):
        for mic, entry in EXCHANGE_HOURS.items():
            assert len(entry) == 4, f"{mic} has {len(entry)} fields"

    def test_timezone_names_are_valid(self):
        for mic, (tz_name, _, _, _) in EXCHANGE_HOURS.items():
            try:
                ZoneInfo(tz_name)
            except Exception:
                pytest.fail(f"Invalid timezone '{tz_name}' for {mic}")

    def test_open_before_close(self):
        for mic, (_, open_str, close_str, _) in EXCHANGE_HOURS.items():
            open_t = _parse_time(open_str)
            close_t = _parse_time(close_str)
            assert open_t < close_t, f"{mic}: open={open_str} >= close={close_str}"

    def test_trading_days_valid(self):
        for mic, (_, _, _, days) in EXCHANGE_HOURS.items():
            assert len(days) > 0
            assert all(0 <= d <= 6 for d in days)

    def test_major_exchanges_present(self):
        for mic in ("XNYS", "XNAS", "XLON", "XTKS", "XHKG"):
            assert mic in EXCHANGE_HOURS


# ── _get_calendar ──────────────────────────────────────────────────


class TestGetCalendar:
    def setup_method(self):
        _calendar_cache.clear()

    def test_valid_exchange(self):
        cal = _get_calendar("XNYS")
        assert cal is not None

    def test_aliased_exchange(self):
        cal = _get_calendar("XNAS")
        assert cal is not None  # aliases to XNYS

    def test_invalid_exchange(self):
        cal = _get_calendar("ZZZZ")
        assert cal is None

    def test_caches_result(self):
        _get_calendar("XNYS")
        assert "XNYS" in _calendar_cache


# ── _is_holiday ────────────────────────────────────────────────────


class TestIsHoliday:
    def test_unknown_exchange_not_holiday(self):
        assert _is_holiday("ZZZZ") is False

    def test_uses_exchange_timezone_date(self):
        """Sunday in the US is Monday in Taipei — XTAI should NOT be a holiday.

        _is_holiday must use the date in the exchange's timezone, not
        the local machine date.  Without this fix, date.today() would
        return Sunday, cal.is_session(Sunday) → False, and XTAI would
        be incorrectly considered closed.
        """
        from datetime import date as _date
        taipei_monday = datetime(2026, 3, 2, 9, 0,
                                 tzinfo=ZoneInfo("Asia/Taipei"))
        mock_cal = MagicMock()
        mock_cal.is_session.return_value = True  # Monday = session

        with patch("src.exchange_hours._get_calendar", return_value=mock_cal), \
             patch("src.exchange_hours.datetime") as mock_dt:
            mock_dt.now.return_value = taipei_monday
            result = _is_holiday("XTAI")

        mock_cal.is_session.assert_called_once_with(_date(2026, 3, 2))
        assert result is False


# ── is_exchange_open ───────────────────────────────────────────────


class TestIsExchangeOpen:
    def test_open_during_trading_hours(self):
        with patch("src.exchange_hours._is_holiday", return_value=False), \
             patch("src.exchange_hours.datetime") as mock_dt:
            tz = ZoneInfo("America/New_York")
            # Wednesday at 10:00 AM ET
            mock_dt.now.return_value = datetime(2026, 3, 4, 10, 0, tzinfo=tz)
            assert is_exchange_open("XNYS") is True

    def test_closed_before_open(self):
        with patch("src.exchange_hours._is_holiday", return_value=False), \
             patch("src.exchange_hours.datetime") as mock_dt:
            tz = ZoneInfo("America/New_York")
            mock_dt.now.return_value = datetime(2026, 3, 4, 8, 0, tzinfo=tz)
            assert is_exchange_open("XNYS") is False

    def test_closed_after_close(self):
        with patch("src.exchange_hours._is_holiday", return_value=False), \
             patch("src.exchange_hours.datetime") as mock_dt:
            tz = ZoneInfo("America/New_York")
            mock_dt.now.return_value = datetime(2026, 3, 4, 17, 0, tzinfo=tz)
            assert is_exchange_open("XNYS") is False

    def test_closed_on_weekend(self):
        with patch("src.exchange_hours._is_holiday", return_value=False), \
             patch("src.exchange_hours.datetime") as mock_dt:
            tz = ZoneInfo("America/New_York")
            # Saturday
            mock_dt.now.return_value = datetime(2026, 3, 7, 12, 0, tzinfo=tz)
            assert is_exchange_open("XNYS") is False

    def test_closed_on_holiday(self):
        with patch("src.exchange_hours._is_holiday", return_value=True):
            assert is_exchange_open("XNYS") is False

    def test_unknown_exchange_assumed_open(self):
        with patch("src.exchange_hours._is_holiday", return_value=False):
            assert is_exchange_open("UNKNOWN_MIC") is True

    def test_tel_aviv_open_on_sunday(self):
        """XTAE trades Sun-Thu."""
        with patch("src.exchange_hours._is_holiday", return_value=False), \
             patch("src.exchange_hours.datetime") as mock_dt:
            tz = ZoneInfo("Asia/Jerusalem")
            # Sunday at 12:00
            mock_dt.now.return_value = datetime(2026, 3, 8, 12, 0, tzinfo=tz)
            assert is_exchange_open("XTAE") is True

    def test_tel_aviv_closed_on_friday(self):
        with patch("src.exchange_hours._is_holiday", return_value=False), \
             patch("src.exchange_hours.datetime") as mock_dt:
            tz = ZoneInfo("Asia/Jerusalem")
            # Friday
            mock_dt.now.return_value = datetime(2026, 3, 6, 12, 0, tzinfo=tz)
            assert is_exchange_open("XTAE") is False

    def test_at_exact_open_time(self):
        with patch("src.exchange_hours._is_holiday", return_value=False), \
             patch("src.exchange_hours.datetime") as mock_dt:
            tz = ZoneInfo("America/New_York")
            mock_dt.now.return_value = datetime(2026, 3, 4, 9, 30, tzinfo=tz)
            assert is_exchange_open("XNYS") is True

    def test_at_exact_close_time(self):
        with patch("src.exchange_hours._is_holiday", return_value=False), \
             patch("src.exchange_hours.datetime") as mock_dt:
            tz = ZoneInfo("America/New_York")
            mock_dt.now.return_value = datetime(2026, 3, 4, 16, 0, tzinfo=tz)
            assert is_exchange_open("XNYS") is True

    def test_whitespace_in_mic(self):
        with patch("src.exchange_hours._is_holiday", return_value=False), \
             patch("src.exchange_hours.datetime") as mock_dt:
            tz = ZoneInfo("America/New_York")
            mock_dt.now.return_value = datetime(2026, 3, 4, 12, 0, tzinfo=tz)
            assert is_exchange_open("  XNYS  ") is True


# ── filter_df_by_open_exchange ─────────────────────────────────────


class TestFilterDfByOpenExchange:
    def test_no_mic_column(self):
        df = pd.DataFrame({"Name": ["A", "B"]})
        result = filter_df_by_open_exchange(df)
        assert len(result) == 2

    def test_keeps_rows_with_missing_exchange(self):
        df = pd.DataFrame({
            "MIC Primary Exchange": [None, "XNYS", ""],
            "Name": ["A", "B", "C"],
        })
        with patch("src.exchange_hours.is_exchange_open", return_value=True):
            result = filter_df_by_open_exchange(df)
        assert len(result) == 3

    def test_filters_closed_exchanges(self):
        df = pd.DataFrame({
            "MIC Primary Exchange": ["XNYS", "XTKS"],
            "Name": ["A", "B"],
        })

        def mock_open(mic):
            return mic == "XNYS"

        with patch("src.exchange_hours.is_exchange_open", side_effect=mock_open):
            result = filter_df_by_open_exchange(df)
        assert len(result) == 1
        assert result.iloc[0]["Name"] == "A"

    def test_all_open(self):
        df = pd.DataFrame({
            "MIC Primary Exchange": ["XNYS", "XNAS"],
            "Name": ["A", "B"],
        })
        with patch("src.exchange_hours.is_exchange_open", return_value=True):
            result = filter_df_by_open_exchange(df)
        assert len(result) == 2

    def test_resets_index(self):
        df = pd.DataFrame({
            "MIC Primary Exchange": ["XNYS", "XTKS", "XLON"],
            "Name": ["A", "B", "C"],
        })

        def mock_open(mic):
            return mic != "XTKS"

        with patch("src.exchange_hours.is_exchange_open", side_effect=mock_open):
            result = filter_df_by_open_exchange(df)
        assert list(result.index) == [0, 1]

"""Tests for src/connection.py — suppress_errors and ensure_connected."""

import logging
from unittest.mock import MagicMock, patch

import pytest

from src.connection import (
    suppress_errors, ensure_connected,
    _ErrorCodeFilter, _suppressed_codes,
)


class TestSuppressErrors:
    def test_adds_and_removes_codes(self):
        assert 999 not in _suppressed_codes
        with suppress_errors(999):
            assert 999 in _suppressed_codes
        assert 999 not in _suppressed_codes

    def test_multiple_codes(self):
        with suppress_errors(100, 200, 300):
            assert {100, 200, 300} <= _suppressed_codes
        assert 100 not in _suppressed_codes
        assert 200 not in _suppressed_codes
        assert 300 not in _suppressed_codes

    def test_cleans_up_on_exception(self):
        with pytest.raises(ValueError):
            with suppress_errors(42):
                assert 42 in _suppressed_codes
                raise ValueError("test")
        assert 42 not in _suppressed_codes

    def test_nested_suppress(self):
        with suppress_errors(10):
            with suppress_errors(20):
                assert 10 in _suppressed_codes
                assert 20 in _suppressed_codes
            assert 10 in _suppressed_codes
            assert 20 not in _suppressed_codes


class TestErrorCodeFilter:
    def test_passes_when_no_suppressed_codes(self):
        f = _ErrorCodeFilter()
        record = logging.LogRecord(
            "test", logging.ERROR, "", 0, "Error 354, stuff", (), None
        )
        # Ensure no codes are suppressed.
        _suppressed_codes.clear()
        assert f.filter(record) is True

    def test_filters_matching_error_code(self):
        f = _ErrorCodeFilter()
        record = logging.LogRecord(
            "test", logging.ERROR, "", 0, "Error 354, something", (), None
        )
        _suppressed_codes.add(354)
        try:
            assert f.filter(record) is False
        finally:
            _suppressed_codes.discard(354)

    def test_filters_matching_warning_code(self):
        f = _ErrorCodeFilter()
        record = logging.LogRecord(
            "test", logging.WARNING, "", 0, "Warning 202, cancelled", (), None
        )
        _suppressed_codes.add(202)
        try:
            assert f.filter(record) is False
        finally:
            _suppressed_codes.discard(202)

    def test_passes_non_matching_code(self):
        f = _ErrorCodeFilter()
        record = logging.LogRecord(
            "test", logging.ERROR, "", 0, "Error 110, bad price", (), None
        )
        _suppressed_codes.add(999)
        try:
            assert f.filter(record) is True
        finally:
            _suppressed_codes.discard(999)


class TestEnsureConnected:
    def test_raises_when_disconnected(self):
        ib = MagicMock()
        ib.isConnected.return_value = False
        with pytest.raises(RuntimeError, match="TWS connection lost"):
            ensure_connected(ib)

    def test_passes_when_connected(self, mock_ib):
        ensure_connected(mock_ib)  # Should not raise

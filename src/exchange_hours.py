"""Exchange trading-hours lookup and open/closed filtering.

Provides a hardcoded table of exchange opening hours (keyed by ISO MIC
code) and utilities to check whether a given exchange is currently open,
filter a DataFrame to only open-exchange rows, etc.

Holidays are **not** handled — only regular weekday schedules are
considered.  Intra-day lunch breaks (Tokyo, Hong Kong, Shanghai, etc.)
are also ignored; the exchange is treated as open for the entire
open-to-close window.
"""

from __future__ import annotations

from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

import pandas as pd


# ------------------------------------------------------------------
# Exchange hours table
# ------------------------------------------------------------------
# Each value is (timezone_name, open_HH:MM, close_HH:MM, trading_weekdays)
# where trading_weekdays is a tuple of ints (Monday=0 … Sunday=6).

_MON_FRI = (0, 1, 2, 3, 4)
_SUN_THU = (6, 0, 1, 2, 3)

EXCHANGE_HOURS: dict[str, tuple[str, str, str, tuple[int, ...]]] = {
    # ---- North America ------------------------------------------------
    "XNYS": ("America/New_York",     "09:30", "16:00", _MON_FRI),
    "XNAS": ("America/New_York",     "09:30", "16:00", _MON_FRI),
    "XNGS": ("America/New_York",     "09:30", "16:00", _MON_FRI),
    "XNMS": ("America/New_York",     "09:30", "16:00", _MON_FRI),
    "XNCM": ("America/New_York",     "09:30", "16:00", _MON_FRI),
    "ARCX": ("America/New_York",     "09:30", "16:00", _MON_FRI),
    "BATS": ("America/New_York",     "09:30", "16:00", _MON_FRI),
    "XASE": ("America/New_York",     "09:30", "16:00", _MON_FRI),
    "IEXG": ("America/New_York",     "09:30", "16:00", _MON_FRI),
    "XPHL": ("America/New_York",     "09:30", "16:00", _MON_FRI),
    "OTCM": ("America/New_York",     "09:30", "16:00", _MON_FRI),
    "XTSE": ("America/Toronto",      "09:30", "16:00", _MON_FRI),
    "XTSX": ("America/Toronto",      "09:30", "16:00", _MON_FRI),
    "XMEX": ("America/Mexico_City",  "08:30", "15:00", _MON_FRI),
    # ---- South America ------------------------------------------------
    "BVMF": ("America/Sao_Paulo",    "10:00", "17:00", _MON_FRI),
    "XLIM": ("America/Lima",         "09:00", "16:00", _MON_FRI),
    "XSGO": ("America/Santiago",     "09:30", "16:00", _MON_FRI),
    # ---- Europe -------------------------------------------------------
    "XLON": ("Europe/London",        "08:00", "16:30", _MON_FRI),
    "XFRA": ("Europe/Berlin",        "08:00", "23:00", _MON_FRI),
    "XETR": ("Europe/Berlin",        "09:00", "17:30", _MON_FRI),
    "XPAR": ("Europe/Paris",         "09:00", "17:30", _MON_FRI),
    "XAMS": ("Europe/Amsterdam",     "09:00", "17:30", _MON_FRI),
    "XBRU": ("Europe/Brussels",      "09:00", "17:30", _MON_FRI),
    "XLIS": ("Europe/Lisbon",        "08:00", "16:30", _MON_FRI),
    "XMIL": ("Europe/Rome",          "09:00", "17:30", _MON_FRI),
    "MTAA": ("Europe/Rome",          "09:00", "17:30", _MON_FRI),
    "XMAD": ("Europe/Madrid",        "09:00", "17:30", _MON_FRI),
    "XSWX": ("Europe/Zurich",        "09:00", "17:30", _MON_FRI),
    "XWBO": ("Europe/Vienna",        "09:05", "17:30", _MON_FRI),
    "XSTO": ("Europe/Stockholm",     "09:00", "17:30", _MON_FRI),
    "XCSE": ("Europe/Copenhagen",    "09:00", "17:00", _MON_FRI),
    "XHEL": ("Europe/Helsinki",      "10:00", "18:30", _MON_FRI),
    "XOSL": ("Europe/Oslo",          "09:00", "16:20", _MON_FRI),
    "XWAR": ("Europe/Warsaw",        "09:00", "17:05", _MON_FRI),
    "XIST": ("Europe/Istanbul",      "10:00", "18:00", _MON_FRI),
    "XATH": ("Europe/Athens",        "10:00", "17:20", _MON_FRI),
    "XBUD": ("Europe/Budapest",      "09:00", "17:05", _MON_FRI),
    "XPRA": ("Europe/Prague",        "09:00", "17:00", _MON_FRI),
    # ---- Asia-Pacific -------------------------------------------------
    "XTKS": ("Asia/Tokyo",           "09:00", "15:00", _MON_FRI),
    "XHKG": ("Asia/Hong_Kong",       "09:30", "16:00", _MON_FRI),
    "XSES": ("Asia/Singapore",       "09:00", "17:00", _MON_FRI),
    "XASX": ("Australia/Sydney",     "10:00", "16:00", _MON_FRI),
    "XKRX": ("Asia/Seoul",           "09:00", "15:30", _MON_FRI),
    "XTAI": ("Asia/Taipei",          "09:00", "13:30", _MON_FRI),
    "ROCO": ("Asia/Taipei",          "09:00", "13:30", _MON_FRI),
    "XSHG": ("Asia/Shanghai",        "09:30", "15:00", _MON_FRI),
    "XSHE": ("Asia/Shanghai",        "09:30", "15:00", _MON_FRI),
    "XNSE": ("Asia/Kolkata",         "09:15", "15:30", _MON_FRI),
    "XBOM": ("Asia/Kolkata",         "09:15", "15:30", _MON_FRI),
    "XNZE": ("Pacific/Auckland",     "10:00", "16:45", _MON_FRI),
    # ---- Middle East / Africa -----------------------------------------
    "XTAE": ("Asia/Jerusalem",       "10:00", "17:25", _SUN_THU),
    "XJSE": ("Africa/Johannesburg",  "09:00", "17:00", _MON_FRI),
}


# ------------------------------------------------------------------
# Session-level cache for unknown exchanges
# ------------------------------------------------------------------
_unknown_exchange_cache: dict[str, bool] = {}


# ------------------------------------------------------------------
# Core helpers
# ------------------------------------------------------------------

def _parse_time(s: str) -> dtime:
    h, m = s.split(":")
    return dtime(int(h), int(m))


def is_exchange_open(mic: str) -> bool:
    """Return ``True`` if exchange *mic* is currently open.

    For MIC codes not present in :data:`EXCHANGE_HOURS`, the user is
    prompted interactively.  The response is cached for the remainder
    of the session.
    """
    mic_upper = mic.upper().strip()

    entry = EXCHANGE_HOURS.get(mic_upper)
    if entry is None:
        # Check session cache first.
        if mic_upper in _unknown_exchange_cache:
            return _unknown_exchange_cache[mic_upper]

        # Ask the user.
        while True:
            choice = input(
                f"Exchange '{mic_upper}' is not in the known hours table. "
                f"Consider it OPEN or CLOSED? [O/C] > "
            ).strip().upper()
            if choice in ("O", "OPEN"):
                _unknown_exchange_cache[mic_upper] = True
                return True
            elif choice in ("C", "CLOSED"):
                _unknown_exchange_cache[mic_upper] = False
                return False
            print("  Please enter O (open) or C (closed).")

    tz_name, open_str, close_str, trading_days = entry
    tz = ZoneInfo(tz_name)
    now = datetime.now(tz)

    if now.weekday() not in trading_days:
        return False

    open_time = _parse_time(open_str)
    close_time = _parse_time(close_str)
    return open_time <= now.time() <= close_time


# ------------------------------------------------------------------
# DataFrame filtering
# ------------------------------------------------------------------

def filter_df_by_open_exchange(df: pd.DataFrame) -> pd.DataFrame:
    """Return a filtered copy of *df* keeping only open-exchange rows.

    Rows whose ``MIC Primary Exchange`` is missing / NaN are kept
    (they cannot be filtered).

    Prints a summary of filtered-out exchanges.
    """
    if "MIC Primary Exchange" not in df.columns:
        return df

    open_flags = []
    for _, row in df.iterrows():
        mic = row.get("MIC Primary Exchange")
        if pd.isna(mic) or str(mic).strip() == "":
            open_flags.append(True)  # keep rows with no exchange info
        else:
            open_flags.append(is_exchange_open(str(mic).strip()))

    filtered = df[open_flags].copy()
    removed = len(df) - len(filtered)

    if removed:
        closed_mics = set()
        for keep, (_, row) in zip(open_flags, df.iterrows()):
            if not keep:
                closed_mics.add(str(row["MIC Primary Exchange"]).strip())
        print(f"Filtered out {removed} row(s) on closed exchanges: "
              f"{', '.join(sorted(closed_mics))}.\n")
    else:
        print("All exchanges are currently open — no rows filtered.\n")

    return filtered.reset_index(drop=True)

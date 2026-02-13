"""Generate a Project_Portfolio vs Current IBKR positions comparison.

Takes an already-reconciled DataFrame (with ``existing_qty``,
``pending_qty``, ``net_quantity`` columns from ``reconcile``),
fetches current market values via ``ib.portfolio()``, and writes
an Excel comparison to ``output/Project_VS_Current.xlsx``.
"""

from __future__ import annotations

import os

import pandas as pd
from ib_async import IB

from src.config import OUTPUT_DIR


def _resolve_fx(row: pd.Series) -> float | None:
    """Return the USD/local FX rate: 1.0 for USD, the rate for foreign, None if missing."""
    ccy = row.get("currency")
    fx_raw = row.get("fx_rate")
    if pd.isna(ccy) or str(ccy).upper() == "USD":
        return 1.0
    if pd.notna(fx_raw) and float(fx_raw) > 0:
        return float(fx_raw)
    return None


def _market_value_usd(
    conid_raw, mkt_values: dict[int, float], fx: float | None,
) -> float | None:
    """Convert a position's local-currency market value to USD.

    Returns ``0.0`` when the position has no market value (not held),
    or ``None`` when the FX rate is missing.
    """
    if fx is None:
        return None
    conid = int(conid_raw) if pd.notna(conid_raw) else None
    local = mkt_values.get(conid) if conid else None
    if local is not None:
        return round(local / fx, 2)
    return 0.0


def _safe_diff(a_raw, b: float | None) -> float | None:
    """Return ``round(a - b, 2)`` if both values are present, else ``None``."""
    if b is not None and pd.notna(a_raw):
        return round(float(a_raw) - b, 2)
    return None


def generate_project_vs_current(ib: IB, df: pd.DataFrame) -> None:
    """Build and save the Project_VS_Current Excel comparison.

    *df* must already contain the columns produced by ``reconcile``
    (``existing_qty``, ``pending_qty``, ``net_quantity``).

    Steps:
      1. Fetch current market values via ``ib.portfolio()``.
      2. Compute dollar amounts and differences.
      3. Write ``output/Project_VS_Current.xlsx``.
    """
    # --- 1. Fetch market values ---
    print("Fetching current market values ...")
    portfolio_items = ib.portfolio()
    print(f"  Found {len(portfolio_items)} portfolio item(s).\n")

    mkt_values: dict[int, float] = {}
    for item in portfolio_items:
        cid = item.contract.conId
        if cid:
            mkt_values[cid] = float(item.marketValue)

    # --- 2. Compute dollar-amount columns ---
    current_dollar_amounts: list[float | None] = []
    project_vs_current: list[float | None] = []
    actual_vs_current: list[float | None] = []

    for _, row in df.iterrows():
        fx = _resolve_fx(row)
        mkt_usd = _market_value_usd(row.get("conid"), mkt_values, fx)

        current_dollar_amounts.append(mkt_usd)
        project_vs_current.append(
            _safe_diff(row.get("Dollar Allocation"), mkt_usd))
        actual_vs_current.append(
            _safe_diff(row.get("Actual Dollar Allocation"), mkt_usd))

    # --- 3. Assemble and save ---
    out = pd.DataFrame({
        "IBKR Name": df.get("IBKR Name"),
        "IBKR Ticker": df.get("IBKR Ticker"),
        "Currency": df.get("currency"),
        "MIC Primary Exchange": df.get("MIC Primary Exchange"),
        "Last Price": df.get("last"),
        "FX Rate": df.get("fx_rate"),
        "Qty": df.get("Qty"),
        "Basket Allocation": df.get("Basket Allocation"),
        "Dollar Allocation": df.get("Dollar Allocation"),
        "Actual Dollar Allocation": df.get("Actual Dollar Allocation"),
        "Current Qty": df.get("existing_qty"),
        "Pending Qty": df.get("pending_qty"),
        "Current Dollar Allocation": current_dollar_amounts,
        "Project VS Current": project_vs_current,
        "Actual vs Current": actual_vs_current,
        "Qty Difference": df.get("net_quantity"),
    })

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "Project_VS_Current.xlsx")
    out.to_excel(out_path, index=False, engine="openpyxl")
    print(f"Comparison saved to {out_path}\n")

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
        conid_raw = row.get("conid")
        fx_raw = row.get("fx_rate")
        ccy = row.get("currency")
        dollar_alloc = row.get("Dollar Allocation")
        actual_alloc_raw = row.get("Actual Dollar Allocation")

        is_usd = pd.isna(ccy) or str(ccy).upper() == "USD"
        if is_usd:
            fx = 1.0
        elif pd.notna(fx_raw) and float(fx_raw) > 0:
            fx = float(fx_raw)
        else:
            fx = None

        conid = int(conid_raw) if pd.notna(conid_raw) else None
        mkt_value_local = mkt_values.get(conid) if conid else None

        if mkt_value_local is not None:
            if fx is not None and fx > 0:
                mkt_value_usd = round(mkt_value_local / fx, 2)
            else:
                mkt_value_usd = None
        else:
            mkt_value_usd = 0.0 if (fx is not None) else None

        current_dollar_amounts.append(mkt_value_usd)

        if mkt_value_usd is not None and pd.notna(dollar_alloc):
            project_vs_current.append(
                round(float(dollar_alloc) - mkt_value_usd, 2))
        else:
            project_vs_current.append(None)

        if mkt_value_usd is not None and pd.notna(actual_alloc_raw):
            actual_vs_current.append(
                round(float(actual_alloc_raw) - mkt_value_usd, 2))
        else:
            actual_vs_current.append(None)

    # --- 3. Assemble and save ---
    out = pd.DataFrame({
        "IBKR Name": df.get("IBKR Name"),
        "IBKR Ticker": df.get("IBKR Ticker"),
        "Currency": df.get("currency"),
        "MIC Primary Exchange": df.get("MIC Primary Exchange"),
        "Last Price": df.get("last"),
        "FX Rate": df.get("fx_rate"),
        "Qty": df.get("Qty"),
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

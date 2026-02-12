"""Generate a Project_Portfolio vs Actual IBKR positions comparison.

Loads the saved Project_Portfolio.csv, fetches current IBKR positions,
and outputs an Excel file highlighting the difference between target
and actual dollar allocations.
"""

from __future__ import annotations

import os

import pandas as pd

from src.api_client import IBKRClient
from src.config import OUTPUT_DIR
from src.orders import get_account_id


def _load_project_portfolio() -> pd.DataFrame:
    """Load the previously saved Project_Portfolio.csv."""
    csv_path = os.path.join(OUTPUT_DIR, "Project_Portfolio.csv")
    if not os.path.isfile(csv_path):
        raise FileNotFoundError(
            f"Project_Portfolio.csv not found at {csv_path}. "
            "Run a normal or noop pass first to generate it."
        )
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} rows from {csv_path}\n")
    return df


def generate_project_vs_actual(client: IBKRClient) -> None:
    """Build and save the Project_VS_Actual Excel comparison.

    Steps:
      1. Load Project_Portfolio.csv.
      2. Fetch current IBKR positions via ``get_positions``.
      3. Match positions to Project_Portfolio rows by ``conid``.
      4. Compute dollar amounts and differences.
      5. Write ``output/Project_VS_Actual.xlsx``.
    """
    # --- 1. Load target portfolio ---
    proj = _load_project_portfolio()

    # --- 2. Fetch IBKR positions ---
    account_id = get_account_id(client)
    print("Fetching current IBKR positions ...")
    raw_positions = client.get_positions(account_id)
    print(f"  Found {len(raw_positions)} position entries.\n")

    # Index positions by conid.  IBKR may return duplicate entries for
    # the same conid across pages; we keep the last entry per conid to
    # avoid double-counting.
    pos_by_conid: dict[int, dict] = {}
    for entry in raw_positions:
        cid = entry.get("conid")
        if cid is None:
            continue
        cid = int(cid)
        pos_by_conid[cid] = {
            "qty": float(entry.get("position", 0)),
            "mktValue": float(entry.get("mktValue", 0)),
            "currency": entry.get("currency", ""),
        }

    # --- 3 & 4. Build comparison rows ---
    current_qtys: list[float] = []
    current_dollar_amounts: list[float | None] = []
    project_vs_current: list[float | None] = []
    actual_vs_current: list[float | None] = []
    qty_differences: list[float | None] = []

    for _, row in proj.iterrows():
        conid_raw = row.get("conid")
        fx_raw = row.get("fx_rate")
        ccy = row.get("currency")
        dollar_alloc = row.get("Dollar Allocation")
        target_qty_raw = row.get("Qty")
        actual_alloc_raw = row.get("Actual Dollar Allocation")

        # FX rate for converting position market value to USD.
        is_usd = pd.isna(ccy) or str(ccy).upper() == "USD"
        if is_usd:
            fx = 1.0
        elif pd.notna(fx_raw) and float(fx_raw) > 0:
            fx = float(fx_raw)
        else:
            fx = None

        # Look up IBKR position.
        if pd.notna(conid_raw):
            conid = int(conid_raw)
            pos = pos_by_conid.get(conid)
        else:
            pos = None

        if pos is not None:
            qty = pos["qty"]
            mkt_value_local = pos["mktValue"]
            # Convert market value to USD.
            if fx is not None and fx > 0:
                mkt_value_usd = round(mkt_value_local / fx, 2)
            else:
                mkt_value_usd = None
        else:
            qty = 0.0
            mkt_value_local = 0.0
            mkt_value_usd = 0.0 if (fx is not None) else None

        current_qtys.append(qty)
        current_dollar_amounts.append(mkt_value_usd)

        # Project VS Current: Dollar Allocation - Current Dollar Allocation.
        if mkt_value_usd is not None and pd.notna(dollar_alloc):
            project_vs_current.append(round(float(dollar_alloc) - mkt_value_usd, 2))
        else:
            project_vs_current.append(None)

        # Actual vs Current: Actual Dollar Allocation - Current Dollar Allocation.
        if mkt_value_usd is not None and pd.notna(actual_alloc_raw):
            actual_vs_current.append(
                round(float(actual_alloc_raw) - mkt_value_usd, 2)
            )
        else:
            actual_vs_current.append(None)

        # Qty Difference: target Qty - Current Qty.
        if pd.notna(target_qty_raw):
            qty_differences.append(float(target_qty_raw) - qty)
        else:
            qty_differences.append(None)

    # --- 5. Assemble output DataFrame ---
    out = pd.DataFrame({
        "IBKR Name": proj.get("IBKR Name"),
        "IBKR Ticker": proj.get("IBKR Ticker"),
        "Currency": proj.get("currency"),
        "MIC Primary Exchange": proj.get("MIC Primary Exchange"),
        "Mark Price": proj.get("mark"),
        "FX Rate": proj.get("fx_rate"),
        "Qty": proj.get("Qty"),
        "Dollar Allocation": proj.get("Dollar Allocation"),
        "Actual Dollar Allocation": proj.get("Actual Dollar Allocation"),
        "Current Qty": current_qtys,
        "Current Dollar Allocation": current_dollar_amounts,
        "Project VS Current": project_vs_current,
        "Actual vs Current": actual_vs_current,
        "Qty Difference": qty_differences,
    })

    # Write to Excel.
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "Project_VS_Actual.xlsx")
    out.to_excel(out_path, index=False, engine="openpyxl")
    print(f"Comparison saved to {out_path}\n")

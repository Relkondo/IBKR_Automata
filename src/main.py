"""IBKR Automata – entry point (TWS API via ib_async).

Orchestrates the full portfolio-order workflow:
  1. Connect to a running Trader Workstation (TWS) instance.
  2. Read and filter the portfolio from an Excel file.
  3. Resolve contract IDs (conids) for every position.
  4. Fetch live market data and compute limit prices.
     Save the enriched portfolio to output/Project_Portfolio.csv.
  5. Reconcile against IBKR positions & pending orders (default).
  6. Interactively place limit orders with user confirmation.
  7. Print a summary of all placed orders.

CLI arguments
-------------
  noop               Run steps 1-4 only (skip order placement).
  noop-recalculate   Re-use conids from the saved Project_Portfolio.csv
                     but re-fetch live market data and re-save.
  project-portfolio  Re-use output/Project_Portfolio.csv from a previous
                     noop run: connect (step 1), then jump straight to
                     the interactive order loop (steps 5-7).
  buy-all            Skip reconciliation – order the full Project_Portfolio
                     quantities regardless of existing IBKR positions.
  cancel-all-orders  Cancel every open order on the account and exit.
  print-project-vs-current
                     Load Project_Portfolio.csv and current IBKR positions,
                     then output an Excel comparison to output/.
  -all-exchanges     Operate on all exchanges regardless of trading hours.
                     By default, only currently open exchanges are used
                     for placing and cancelling orders.
"""

import os
import sys

import pandas as pd

from src.config import OUTPUT_DIR
from src.connection import connect
from src.portfolio import load_portfolio
from src.contracts import resolve_conids
from src.market_data import (
    fetch_market_data, resolve_currencies, save_project_portfolio,
)
from src.comparison import generate_project_vs_current
from src.exchange_hours import filter_df_by_open_exchange
from src.orders import (
    cancel_all_orders, get_account_id, run_order_loop, print_order_summary,
)
from src.reconcile import reconcile


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


def main() -> None:
    args = sys.argv[1:]
    noop = "noop" in args
    noop_recalc = "noop-recalculate" in args
    use_saved = "project-portfolio" in args
    buy_all = "buy-all" in args
    cancel_all = "cancel-all-orders" in args
    print_comparison = "print-project-vs-current" in args
    all_exchanges = "-all-exchanges" in args

    # Mutual exclusivity checks.
    mode_flags = sum([noop, noop_recalc, use_saved, cancel_all,
                      print_comparison])
    if mode_flags > 1:
        print("Error: 'noop', 'noop-recalculate', 'project-portfolio', "
              "'cancel-all-orders', and 'print-project-vs-current' "
              "are mutually exclusive.")
        sys.exit(1)

    if print_comparison:
        print("Running in PRINT-PROJECT-VS-CURRENT mode.\n")
    elif cancel_all:
        print("Running in CANCEL-ALL-ORDERS mode.\n")
    elif noop:
        print("Running in NOOP mode -- orders will NOT be placed.\n")
    elif noop_recalc:
        print("Running in NOOP-RECALCULATE mode -- "
              "re-fetching market data for existing conids.\n")
    elif use_saved:
        print("Running in PROJECT_PORTFOLIO mode -- "
              "using saved CSV, skipping steps 2-4.\n")
    if buy_all:
        print("Running in BUY-ALL mode -- "
              "skipping reconciliation with existing IBKR positions.\n")
    if all_exchanges:
        print("ALL-EXCHANGES mode -- "
              "operating on all exchanges regardless of trading hours.\n")

    # ------------------------------------------------------------------
    # 1. Connect to TWS
    # ------------------------------------------------------------------
    print("Connecting to TWS ...")
    ib = connect()

    try:
        # ==============================================================
        # cancel-all-orders  (standalone mode)
        # ==============================================================
        if cancel_all:
            cancel_all_orders(ib, all_exchanges=all_exchanges)

        # ==============================================================
        # project-portfolio / print-project-vs-current
        # (load saved CSV, skip steps 2-4)
        # ==============================================================
        elif use_saved or print_comparison:
            df = _load_project_portfolio()

        # ==============================================================
        # noop-recalculate  (re-fetch market data for existing conids)
        # ==============================================================
        elif noop_recalc:
            df = _load_project_portfolio()

            # Resolve currencies & exchange rates.
            df = resolve_currencies(ib, df)

            # Fetch market data & compute limit prices.
            df = fetch_market_data(ib, df)

            # Save.
            save_project_portfolio(df)

        # ==============================================================
        # Normal run  (full pipeline: steps 2-4)
        # ==============================================================
        else:
            # 2. Read portfolio.
            df = load_portfolio()

            # 3. Resolve conids.
            print("Resolving contract IDs ...\n")
            df = resolve_conids(ib, df)

            # 3b. Resolve currencies & exchange rates.
            df = resolve_currencies(ib, df)

            # 4. Fetch market data & compute limit prices.
            df = fetch_market_data(ib, df)

            # Save Project_Portfolio.csv.
            save_project_portfolio(df)

        # ==============================================================
        # Ordering / comparison section
        # ==============================================================
        if not noop and not noop_recalc and not cancel_all:

            # 5. Reconcile (unless buy-all).
            if not buy_all:
                print("Reconciling target portfolio with IBKR state ...\n")
                df = reconcile(ib, df,
                               all_exchanges=all_exchanges,
                               dry_run=print_comparison)

            if print_comparison:
                # 6a. Write comparison Excel instead of placing orders.
                generate_project_vs_current(ib, df)
            else:
                # 5b. Filter to open exchanges.
                if not all_exchanges:
                    print("Filtering to currently open exchanges ...\n")
                    df = filter_df_by_open_exchange(df)

                # 6. Interactive order loop.
                placed = run_order_loop(ib, df)

                # 7. Summary.
                print_order_summary(placed)

        elif noop or noop_recalc:
            print("\nNOOP mode -- skipping order placement. "
                  "Review Project_Portfolio.csv in the output directory."
                  + (" (market data refreshed)" if noop_recalc else ""))

    finally:
        # 8. Disconnect.
        ib.disconnect()
        print("Disconnected from TWS.")


if __name__ == "__main__":
    main()

"""IBKR Automata – v0 entry point.

Orchestrates the full portfolio-order workflow:
  1. Launch the Client Portal Gateway and authenticate.
  2. Read and filter the portfolio from an Excel file.
  3. Resolve contract IDs (conids) for every position.
  4. Fetch live bid/ask data and compute limit prices.
  5. Save the enriched portfolio to a CSV.
  5b. Reconcile against IBKR positions & pending orders (default).
  6. Interactively place limit orders with user confirmation.
  7. Print a summary of all placed orders.

CLI arguments
-------------
  noop               Run steps 1-5 only (skip order placement).
  noop-recalculate    Re-use conids from the saved Project_Portfolio.csv
                      but re-fetch live market data (step 4) and re-save.
  project-portfolio   Re-use output/Project_Portfolio.csv from a previous
                      noop run: launch the gateway (step 1), then jump
                      straight to the interactive order loop (step 6-7).
  buy-all             Skip reconciliation – order the full Project_Portfolio
                      quantities regardless of existing IBKR positions.
  cancel-all-orders   Cancel every open order on the account and exit.
  print-project-vs-actual
                      Load Project_Portfolio.csv and current IBKR positions,
                      then output an Excel comparison to output/.
  -all-exchanges      Operate on all exchanges regardless of trading hours.
                      By default, only currently open exchanges are used
                      for placing and cancelling orders.
"""

import os
import sys

import pandas as pd

from src.api_client import IBKRClient
from src.config import OUTPUT_DIR
from src.gateway import launch_gateway, wait_for_auth, SessionKeepalive
from src.portfolio import load_portfolio
from src.contracts import resolve_conids
from src.market_data import fetch_market_data, resolve_currencies, save_project_portfolio
from src.comparison import generate_project_vs_actual
from src.exchange_hours import filter_df_by_open_exchange
from src.orders import cancel_all_orders, run_order_loop, print_order_summary
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
    print_comparison = "print-project-vs-actual" in args
    all_exchanges = "-all-exchanges" in args

    # Mutual exclusivity checks.
    mode_flags = sum([noop, noop_recalc, use_saved, cancel_all, print_comparison])
    if mode_flags > 1:
        print("Error: 'noop', 'noop-recalculate', 'project-portfolio', "
              "'cancel-all-orders', and 'print-project-vs-actual' "
              "are mutually exclusive.")
        sys.exit(1)

    if print_comparison:
        print("Running in PRINT-PROJECT-VS-ACTUAL mode.\n")
    elif cancel_all:
        print("Running in CANCEL-ALL-ORDERS mode.\n")
    elif noop:
        print("Running in NOOP mode -- orders will NOT be placed.\n")
    elif noop_recalc:
        print("Running in NOOP-RECALCULATE mode -- "
              "re-fetching market data for existing conids.\n")
    elif use_saved:
        print("Running in PROJECT_PORTFOLIO mode -- "
              "using saved CSV, skipping steps 2-5.\n")
    if buy_all:
        print("Running in BUY-ALL mode -- "
              "skipping reconciliation with existing IBKR positions.\n")
    if all_exchanges:
        print("ALL-EXCHANGES mode -- "
              "operating on all exchanges regardless of trading hours.\n")

    # ------------------------------------------------------------------
    # 1. Launch gateway & authenticate
    # ------------------------------------------------------------------
    print("Starting IBKR Client Portal Gateway ...")
    try:
        gw_process = launch_gateway()
    except FileNotFoundError as exc:
        print(f"Error: {exc}")
        sys.exit(1)

    client = IBKRClient()
    wait_for_auth(client)

    # Start session keepalive.
    keepalive = SessionKeepalive(client)
    keepalive.start()

    try:
        if print_comparison:
            generate_project_vs_actual(client)
        elif cancel_all:
            cancel_all_orders(client, all_exchanges=all_exchanges)
        elif use_saved:
            # Load the previously generated CSV directly.
            df = _load_project_portfolio()
        elif noop_recalc:
            # Load saved CSV (with conids), re-fetch market data, re-save.
            df = _load_project_portfolio()

            # ----------------------------------------------------------
            # 3b. Resolve currencies & exchange rates
            # ----------------------------------------------------------
            df = resolve_currencies(client, df)

            # ----------------------------------------------------------
            # 4. Fetch market data & compute limit prices
            # ----------------------------------------------------------
            df = fetch_market_data(client, df)

            # ----------------------------------------------------------
            # 5. Save Project_Portfolio.csv
            # ----------------------------------------------------------
            save_project_portfolio(df)
        else:
            # ----------------------------------------------------------
            # 2. Read portfolio
            # ----------------------------------------------------------
            df = load_portfolio()

            # ----------------------------------------------------------
            # 3. Resolve conids
            # ----------------------------------------------------------
            print("Resolving contract IDs ...\n")
            df = resolve_conids(client, df)

            # ----------------------------------------------------------
            # 3b. Resolve currencies & exchange rates
            # ----------------------------------------------------------
            df = resolve_currencies(client, df)

            # ----------------------------------------------------------
            # 4. Fetch market data & compute limit prices
            # ----------------------------------------------------------
            df = fetch_market_data(client, df)

            # ----------------------------------------------------------
            # 5. Save Project_Portfolio.csv
            # ----------------------------------------------------------
            save_project_portfolio(df)

        if not noop and not noop_recalc and not cancel_all and not print_comparison:
            # ----------------------------------------------------------
            # 5b. Reconcile against IBKR state (unless buy-all)
            # ----------------------------------------------------------
            if not buy_all:
                print("Reconciling target portfolio with IBKR state ...\n")
                df = reconcile(client, df, all_exchanges=all_exchanges)

            # ----------------------------------------------------------
            # 5c. Filter to open exchanges (unless -all-exchanges)
            # ----------------------------------------------------------
            if not all_exchanges:
                print("Filtering to currently open exchanges ...\n")
                df = filter_df_by_open_exchange(df)

            # ----------------------------------------------------------
            # 6. Interactive order loop
            # ----------------------------------------------------------
            placed = run_order_loop(client, df)

            # ----------------------------------------------------------
            # 7. Summary
            # ----------------------------------------------------------
            print_order_summary(placed)
        elif noop or noop_recalc:
            print("\nNOOP mode -- skipping order placement. "
                  "Review Project_Portfolio.csv in the output directory."
                  + (" (market data refreshed)" if noop_recalc else ""))

    finally:
        # --------------------------------------------------------------
        # 8. Cleanup
        # --------------------------------------------------------------
        keepalive.stop()
        print("Session keepalive stopped.")

        try:
            import signal
            if gw_process.stdout:
                gw_process.stdout.close()
            # Kill the entire process group (shell + Java child).
            os.killpg(os.getpgid(gw_process.pid), signal.SIGTERM)
            gw_process.wait(timeout=10)
            print("Gateway terminated.")
        except Exception as exc:
            print(f"Failed to terminate gateway: {exc}")
            # Last resort: force kill.
            try:
                os.killpg(os.getpgid(gw_process.pid), signal.SIGKILL)
            except Exception:
                pass


if __name__ == "__main__":
    main()

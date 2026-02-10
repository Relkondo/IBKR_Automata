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
"""

import os
import sys

import pandas as pd

from src.api_client import IBKRClient
from src.config import OUTPUT_DIR
from src.gateway import launch_gateway, wait_for_auth, SessionKeepalive
from src.portfolio import load_portfolio
from src.contracts import resolve_conids
from src.market_data import fetch_market_data, save_project_portfolio
from src.orders import run_order_loop, print_order_summary
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

    # Mutual exclusivity checks.
    mode_flags = sum([noop, noop_recalc, use_saved])
    if mode_flags > 1:
        print("Error: 'noop', 'noop-recalculate', and 'project-portfolio' "
              "are mutually exclusive.")
        sys.exit(1)

    if noop:
        print("Running in NOOP mode -- orders will NOT be placed.\n")
    if noop_recalc:
        print("Running in NOOP-RECALCULATE mode -- "
              "re-fetching market data for existing conids.\n")
    if use_saved:
        print("Running in PROJECT_PORTFOLIO mode -- "
              "using saved CSV, skipping steps 2-5.\n")
    if buy_all:
        print("Running in BUY-ALL mode -- "
              "skipping reconciliation with existing IBKR positions.\n")

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
        if use_saved:
            # Load the previously generated CSV directly.
            df = _load_project_portfolio()
        elif noop_recalc:
            # Load saved CSV (with conids), re-fetch market data, re-save.
            df = _load_project_portfolio()

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
            # 4. Fetch market data & compute limit prices
            # ----------------------------------------------------------
            df = fetch_market_data(client, df)

            # ----------------------------------------------------------
            # 5. Save Project_Portfolio.csv
            # ----------------------------------------------------------
            save_project_portfolio(df)

        if not noop and not noop_recalc:
            # ----------------------------------------------------------
            # 5b. Reconcile against IBKR state (unless buy-all)
            # ----------------------------------------------------------
            if not buy_all:
                print("Reconciling target portfolio with IBKR state ...\n")
                df = reconcile(client, df)

            # ----------------------------------------------------------
            # 6. Interactive order loop
            # ----------------------------------------------------------
            placed = run_order_loop(client, df)

            # ----------------------------------------------------------
            # 7. Summary
            # ----------------------------------------------------------
            print_order_summary(placed)
        else:
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

# IBKR Automata

A Python wrapper around the **Interactive Brokers TWS API** (via [ib_async](https://github.com/ib-api-reloaded/ib_async)) that automates portfolio-wide limit order placement from a spreadsheet.

Given an Excel file describing a target portfolio (tickers, allocations, exchanges), IBKR Automata will:

1. Connect to a running Trader Workstation (TWS) instance.
2. Resolve every position to an IBKR contract ID — stocks and options, with intelligent fallbacks (name-based search, exchange redirection for JP/HK markets).
3. Fetch live market data (bid, ask, last, close, day high/low), compute limit prices using a configurable patience parameter, and save the enriched portfolio to `output/Project_Portfolio.csv`.
4. Reconcile the target portfolio against your existing IBKR positions and pending orders, so only the *net difference* is ordered.
5. Interactively walk you through each order for confirmation before submission.

---

## Prerequisites

- **Python 3.12+** (managed via [pyenv](https://github.com/pyenv/pyenv))
- **Trader Workstation (TWS)** — download from [Interactive Brokers](https://www.interactivebrokers.com/en/trading/tws.php). TWS must be running and authenticated before launching IBKR Automata. Enable the API in TWS settings: *Edit → Global Configuration → API → Settings* — check "Enable ActiveX and Socket Clients" and note the port (default: 7497 for paper, 7496 for live).

## Setup

```bash
# Clone the repository
git clone https://github.com/<your-username>/IBKR_Automata.git
cd IBKR_Automata

# Create and activate a pyenv virtualenv
pyenv virtualenv 3.12.12 ibkr-automata
pyenv local ibkr-automata

# Install dependencies
pip install -r requirements.txt
```

### Configuration

Edit `src/config.py` to match your environment:

| Setting | Description |
|---|---|
| `TWS_HOST` | TWS API host. Default: `127.0.0.1`. |
| `TWS_PORT` | TWS API port. Default: `7497` (paper trading). Use `7496` for live. |
| `TWS_CLIENT_ID` | Client ID for the API connection. Default: `1`. |

## Input format

Place your portfolio Excel file (`.xlsx`) in the `assets/` directory. The file must contain the following columns:

| Column | Description |
|---|---|
| **Ticker** | Ticker symbol (Bloomberg-style, e.g. `NVDA US Equity`). |
| **Security Ticker** | Alternative ticker (takes priority over Ticker when present). |
| **Name** | Company or instrument name. Rows with an empty name are filtered out. |
| **Basket Allocation** | Target allocation as a percentage of net liquidation. Positive = long, negative = short. |
| **MIC Primary Exchange** | ISO MIC code for the primary exchange (used to disambiguate multi-listed securities). |

## Usage

```bash
python -m src.main [options]
```

Before launching, make sure TWS is running and authenticated. The program connects to TWS automatically — no browser authentication step is needed.

### Modes

| Argument | Description |
|---|---|
| *(none)* | **Full run** — read the Excel, resolve contracts, fetch market data, reconcile against existing IBKR positions & pending orders, then interactively place orders for the net difference. |
| `noop` | **Dry run** — execute steps 1-3 (resolve, fetch, save) but skip order placement. Useful for reviewing `output/Project_Portfolio.csv` before committing. |
| `noop-recalculate` | Re-use contract IDs from a previously saved `Project_Portfolio.csv` but re-fetch net liquidation, recompute dollar allocations, re-fetch live market data, and re-save. Skips order placement. |
| `project-portfolio` | Skip steps 2-3 entirely — load an existing `output/Project_Portfolio.csv` and jump straight to the interactive order loop. |
| `buy-all` | Skip reconciliation — order the full target quantities from `Project_Portfolio` regardless of existing positions or pending orders on IBKR. Can be combined with `project-portfolio`. |
| `cancel-all-orders` | Cancel every open order on the account and exit. |
| `print-project-vs-current` | Load `Project_Portfolio.csv` and current IBKR positions, then output an Excel comparison (`output/Project_VS_Current.xlsx`) showing target vs current allocations. |
| `-all-exchanges` | Operate on **all** exchanges regardless of trading hours. By default, only currently open exchanges are considered when placing or cancelling orders. Has no effect with `noop` or `noop-recalculate`. Compatible with all other arguments. |

`noop`, `noop-recalculate`, `project-portfolio`, `cancel-all-orders`, and `print-project-vs-current` are mutually exclusive. `buy-all` can be combined with `project-portfolio`.

### Examples

```bash
# Dry run: resolve everything, save CSV, don't place orders
python -m src.main noop

# Refresh market data only (re-use previously resolved conids)
python -m src.main noop-recalculate

# Full run with reconciliation (default)
python -m src.main

# Full run without reconciliation (place everything in Project_Portfolio)
python -m src.main buy-all

# Place orders from a previously saved Project_Portfolio.csv
python -m src.main project-portfolio

# Full run, including exchanges that are currently closed
python -m src.main -all-exchanges

# Cancel all open orders (only on currently open exchanges)
python -m src.main cancel-all-orders

# Cancel all open orders regardless of exchange hours
python -m src.main cancel-all-orders -all-exchanges

# Compare Project_Portfolio targets against current IBKR positions
python -m src.main print-project-vs-current
```

### Interactive order loop

For each planned order, you are shown the side, limit price, quantity, and dollar amount, then prompted:

- **Y** — Confirm and place this order.
- **A** — Confirm this order *and all subsequent ones* automatically.
- **E** — Confirm all orders for this exchange automatically.
- **M** — Modify the quantity, limit price, or side before confirming.
- **S** — Skip this order.
- **X** — Skip all orders for this exchange.
- **Q** — Quit the order loop (orders already placed are kept).

Orders exceeding the `MAXIMUM_AMOUNT_AUTOMATIC_ORDER` threshold (default: $10,000) are deferred during auto-confirm and presented individually for manual approval at the end.

## Output

The program writes `output/Project_Portfolio.csv` containing the enriched portfolio with:

- Original and IBKR-resolved tickers and names
- Name mismatch flag
- Bid, ask, last, close, day high, day low
- Computed limit price (tick-size snapped)
- Planned quantity and actual dollar allocation

After order placement, a summary table of all placed orders is printed to the terminal.

## Limit price formula

Limit prices are computed using a `FILL_PATIENCE` parameter (default: **20**, configurable in `src/market_data.py`). The value ranges from 0 to 100 and controls how aggressively the order crosses the bid/ask spread.

**Buying:**

```
limit = ask - (ask - bid) × FILL_PATIENCE / 100
```
- `0` → buy at ask (aggressive — fills immediately)
- `50` → buy at midpoint
- `100` → buy at bid (patient — may not fill)

**Selling:**

```
limit = bid + (ask - bid) × FILL_PATIENCE / 100
```
- `0` → sell at bid (aggressive — fills immediately)
- `50` → sell at midpoint
- `100` → sell at ask (patient — may not fill)

When bid/ask is unavailable, the limit price falls back to `last`, then `close`.

All limit prices are snapped to valid tick increments using IBKR market rules to avoid order rejections.

## Project structure

```
IBKR_Automata/
├── assets/                  # Input Excel files
├── output/                  # Generated Project_Portfolio.csv
├── src/
│   ├── main.py              # CLI entry point & workflow orchestration
│   ├── config.py            # TWS connection settings & paths
│   ├── connection.py        # ib_async IB() connection wrapper
│   ├── portfolio.py         # Excel loading & preprocessing
│   ├── contracts.py         # Contract ID resolution (stocks, options, fallbacks)
│   ├── market_data.py       # Market data snapshots & limit price computation
│   ├── exchange_hours.py    # Exchange trading hours & open/closed filtering
│   ├── comparison.py        # Project_Portfolio vs current IBKR positions
│   ├── extra_positions.py   # Handle IBKR positions not in the input file
│   ├── reconcile.py         # Reconciliation against IBKR positions & orders
│   └── orders.py            # Interactive order placement loop
├── requirements.txt
└── README.md
```

## License

This project is provided as-is for personal use. Use at your own risk — automated trading involves financial risk. Always review orders before confirming.

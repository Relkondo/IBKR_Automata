# IBKR Automata

A Python wrapper around the **Interactive Brokers Client Portal API** that automates portfolio-wide limit order placement from a spreadsheet.

Given an Excel file describing a target portfolio (tickers, allocations, exchanges), IBKR Automata will:

1. Launch the IBKR Client Portal Gateway and authenticate your session.
2. Resolve every position to an IBKR contract ID — stocks and options, with intelligent fallbacks (company name search, LLM-assisted ticker resolution via OpenAI).
3. Fetch live market data (bid, ask, mark price, day high/low), compute limit prices using a configurable parameter, and save the enriched portfolio to `output/Project_Portfolio.csv`.
4. Reconcile the target portfolio against your existing IBKR positions and pending orders, so only the *net difference* is ordered.
5. Interactively walk you through each order for confirmation before submission.

---

## Prerequisites

- **Python 3.12+** (managed via [pyenv](https://github.com/pyenv/pyenv))
- **IBKR Client Portal Gateway** — download from [Interactive Brokers](https://www.interactivebrokers.com/en/trading/ib-api.php). Note the installation path; you will configure it in `src/config.py`.
- **OpenAI API key** (optional) — used as a last-resort fallback to resolve ambiguous ticker symbols. Store the key in a plain text file and point `OPENAI_API_KEY_FILE` in `src/config.py` to it.

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
| `GATEWAY_DIR` | Absolute path to your Client Portal Gateway installation (the directory containing `bin/run.sh`). |
| `BASE_URL` | Gateway URL. Default: `https://localhost:5001/v1/api`. Adjust the port if you changed it in the gateway config. |
| `OPENAI_API_KEY_FILE` | Path to a plain-text file containing your OpenAI API key (used for LLM ticker fallback). |

## Input format

Place your portfolio Excel file (`.xlsx`) in the `assets/` directory. The file must contain the following columns:

| Column | Description |
|---|---|
| **Ticker** | Ticker symbol. |
| **Security Ticker** | Alternative ticker (takes priority over Ticker when present). |
| **Name** | Company or instrument name. Rows with an empty name are filtered out. |
| **Dollar Allocation** | Target dollar amount. Positive = buy, negative = short sell. |
| **MIC Primary Exchange** | ISO MIC code for the primary exchange (used to disambiguate multi-listed securities). |

## Usage

```bash
python -m src.main [options]
```

On launch, the program starts the Client Portal Gateway and asks you to authenticate via your browser at `https://localhost:5001`. Press Enter once authenticated.

### Modes

| Argument | Description |
|---|---|
| *(none)* | **Full run** — read the Excel, resolve contracts, fetch market data, reconcile against existing IBKR positions & pending orders, then interactively place orders for the net difference. |
| `noop` | **Dry run** — execute steps 1-3 (resolve, fetch, save) but skip order placement. Useful for reviewing `output/Project_Portfolio.csv` before committing. |
| `noop-recalculate` | Re-use contract IDs from a previously saved `Project_Portfolio.csv` but re-fetch live market data and recompute limit prices. Skips order placement. |
| `project-portfolio` | Skip steps 2-3 entirely — load an existing `output/Project_Portfolio.csv` and jump straight to the interactive order loop. |
| `buy-all` | Skip reconciliation — order the full target quantities from `Project_Portfolio` regardless of existing positions or pending orders on IBKR. |

`noop`, `noop-recalculate`, and `project-portfolio` are mutually exclusive. `buy-all` can be combined with `project-portfolio`.

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
```

### Interactive order loop

For each planned order, you are shown the side, limit price, quantity, and dollar amount, then prompted:

- **Y** — Confirm and place this order.
- **A** — Confirm this order *and all subsequent ones* automatically.
- **M** — Modify the quantity, limit price, or side before confirming.
- **S** — Skip this order.
- **Q** — Quit the order loop (orders already placed are kept).

## Output

The program writes `output/Project_Portfolio.csv` containing the enriched portfolio with:

- Original and IBKR-resolved tickers and names
- Name mismatch flag
- Bid, ask, mark price, day high, day low
- Computed limit price
- Planned quantity and actual dollar allocation

After order placement, a summary table of all placed orders is printed to the terminal.

## Limit price formula

Prices are computed using a `SPEED_VS_GREED` parameter (default: **10**, configurable in `src/market_data.py`). A higher value produces prices closer to the reference (more patient / "greedier"); a lower value produces more aggressive prices.

**Buying:**
1. If bid available: `bid - (mark - bid) / SPEED_VS_GREED`
2. Else if day low available: `mark - (mark - day_low) / SPEED_VS_GREED`
3. Else: `mark`

**Selling (short):**
1. If ask available: `ask - (mark - ask) / SPEED_VS_GREED`
2. Else if day high available: `mark - (mark - day_high) / SPEED_VS_GREED`
3. Else: `mark`

## Project structure

```
IBKR_Automata/
├── assets/                  # Input Excel files
├── output/                  # Generated Project_Portfolio.csv
├── src/
│   ├── main.py              # CLI entry point & workflow orchestration
│   ├── config.py            # Paths, URLs, and settings
│   ├── gateway.py           # Gateway subprocess & session keepalive
│   ├── api_client.py        # IBKR Client Portal API wrapper
│   ├── portfolio.py         # Excel loading & preprocessing
│   ├── contracts.py         # Contract ID resolution (stocks, options, fallbacks)
│   ├── market_data.py       # Market data polling & limit price computation
│   ├── reconcile.py         # Reconciliation against IBKR positions & orders
│   └── orders.py            # Interactive order placement loop
├── requirements.txt
└── README.md
```

## License

This project is provided as-is for personal use. Use at your own risk — automated trading involves financial risk. Always review orders before confirming.

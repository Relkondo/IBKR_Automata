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
- **TWS or IB Gateway** — at least one must be running and authenticated before launching IBKR Automata (in `-auto` mode the program can start IB Gateway itself; see [Automated / Cron Setup](#automated--cron-setup) below).
  - *Interactive use*: [Trader Workstation (TWS)](https://www.interactivebrokers.com/en/trading/tws.php) — port 7497 (paper) / 7496 (live).
  - *Unattended / cron use*: [IB Gateway](https://www.interactivebrokers.com/en/trading/ibgateway-stable.php) — lighter (~40 % fewer resources), no inactivity lock-out, same API — port 4002 (paper) / 4001 (live).

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

Connection settings are read from environment variables (`.env` file or shell). Edit `.env` or export them directly. Tuning parameters live in `src/config.py`.

| Env var | Default | Description |
|---|---|---|
| `IBKR_HOST` | `127.0.0.1` | IBKR API host. |
| `IBKR_TWS_PORT` | `7497` (paper) / `7496` (live) | TWS port for interactive runs. Derived from `TRADING_MODE`. |
| `IBKR_GATEWAY_PORT` | `4002` (paper) / `4001` (live) | IB Gateway port for `-auto` / cron runs. Derived from `TRADING_MODE`. |

Trading thresholds (in `src/config.py`):

| Setting | Default | Description |
|---|---|---|
| `MINIMUM_TRADING_AMOUNT` | `100` | USD — net orders below this value are skipped. |
| `MAXIMUM_AMOUNT_AUTOMATIC_ORDER` | `1,500` | USD — auto-confirmed orders above this are deferred for manual approval. |
| `STALE_ORDER_TOL_PCT` | `0.005` | Fraction — stale-order price tolerance (0.5 %). |
| `STALE_ORDER_TOL_PCT_ILLIQUID` | `0.05` | Fraction — wider tolerance for illiquid exchanges (5 %). |

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

Before launching, make sure TWS or IB Gateway is running and authenticated (or use `-auto` mode with IBC to start it automatically — see [Automated / Cron Setup](#automated--cron-setup)).

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
| `-auto` | Fully autonomous mode (no user prompts). Auto-confirms orders, rejects large orders, sends Telegram notifications on errors. **Automatically starts IB Gateway via IBC if it is not already running** — designed for cron-job execution. |

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

Limit prices are computed using the `FILL_PATIENCE` parameter (default: **20**, configurable in `src/config.py`). The value ranges from 0 to 100 and controls how aggressively the order crosses the bid/ask spread.

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

## Automated / Cron Setup

For fully unattended execution (e.g. via `cron`), IBKR Automata can start **IB Gateway** automatically through [IBC](https://github.com/IbcAlpha/IBC). IB Gateway is a lightweight headless alternative to TWS that exposes the same API.

### 1. TWS installation (includes Gateway)

You can install either TWS or IB Gateway. TWS has a UI, IB Gateway is lightweight.

A TWS installation already contains the IB Gateway code -- no separate download is needed. IBC can launch Gateway mode directly from the TWS install at `~/Applications/Trader Workstation/`.

If you haven't installed TWS yet, download the **offline** (stable) installer from [Interactive Brokers](https://www.interactivebrokers.com/en/trading/tws.php). On macOS it installs to `~/Applications`.

Make sure the following API settings are configured in TWS (*Edit → Global Configuration → API → Settings*):

- Check **Enable ActiveX and Socket Clients**
- Check **Download open orders on connection**
- Increase Memory Allocation to at least 4096 MB

When IBC launches Gateway mode from the TWS installation, it listens on port 4001 (live) or 4002 (paper) by default.

### 2. Install IBC

[IBC](https://github.com/IbcAlpha/IBC) automates IB Gateway login, dialog handling, and daily restarts.

```bash
# Download the latest macOS release
curl -L -o /tmp/ibc.zip \
  https://github.com/IbcAlpha/IBC/releases/latest/download/IBCMacos-3.23.0.zip

# Install to /opt/ibc
sudo mkdir -p /opt/ibc
sudo unzip /tmp/ibc.zip -d /opt/ibc
sudo chmod +x /opt/ibc/scripts/*.sh

# Create ~/ibc/ (for config.ini) and ~/ibc/logs/ (for IBC diagnostics)
mkdir -p ~/ibc/logs
```

### 3. Configure IBC

Create `~/ibc/config.ini` with your credentials. A minimal config:

```ini
IbLoginId=your_username
IbPassword=your_password
TradingMode=live

# Auto-accept API connections from localhost
AcceptIncomingConnectionAction=accept

# Accept paper-trading dialog automatically
AcceptNonBrokerageAccountWarning=yes

# Handle the "US stocks market data in shares" dialog
AcceptBidAskLastSizeDisplayUpdateNotification=accept

# Allow blind trading (orders without market data subscription)
AllowBlindTrading=yes

# 2FA: retry automatically if you miss the alert
ReloginAfterSecondFactorAuthenticationTimeout=yes

# Daily auto-restart (no re-auth needed Mon–Sat)
AutoRestartTime=11:55 PM
```

> **Security:** Restrict permissions on the config file: `chmod 600 ~/ibc/config.ini`.

### 4. Configure environment variables

Add IBC settings to your `.env` file (see `.env.example`):

```bash
TRADING_MODE=live       # or paper
TWS_MAJOR_VRSN=10.44   # check via Help > About in TWS, or tail ~/Jts/launcher.log
# Ports are derived from TRADING_MODE automatically.
# Override only if your setup uses non-standard ports:
IBKR_GATEWAY_PORT=4001
IBKR_TWS_PORT=7496
```

### 5. Set up the cron job

```bash
crontab -e
```

Example — run every weekday at 9:30 AM Eastern:

```cron
SHELL=/bin/zsh
30 9 * * 1-5 cd /path/to/IBKR_Automata && /path/to/.pyenv/versions/ibkr-automata/bin/python -m src.main -auto -all-exchanges >> ~/ibc/logs/automata.log 2>&1
```

In `-auto` mode the program will:
1. Check if IB Gateway is already listening on the configured port.
2. If not, start it via IBC and wait for it to accept connections (up to `GATEWAY_STARTUP_TIMEOUT` seconds, default 120).
3. Connect, run the full pipeline, and disconnect.

### Two-Factor Authentication notes

- **Daily auto-restarts** (Mon–Sat) do *not* require 2FA.
- **Weekly cold restart** (Sundays, after IBKR's Saturday-night server reset) requires acknowledging one 2FA prompt on the IBKR Mobile app.
- If IB Gateway crashes mid-week and needs a fresh start, 2FA is required again. IBC's `ReloginAfterSecondFactorAuthenticationTimeout=yes` will keep retrying, giving you multiple chances to acknowledge.

---

## Project structure

```
IBKR_Automata/
├── assets/                  # Input Excel files
├── output/                  # Generated Project_Portfolio.csv
├── src/
│   ├── main.py              # CLI entry point & workflow orchestration
│   ├── config.py            # Centralized settings (connection, thresholds, tuning)
│   ├── connection.py        # ib_async IB() connection wrapper
│   ├── gateway.py           # IB Gateway lifecycle management via IBC
│   ├── portfolio.py         # Excel loading & preprocessing
│   ├── contracts.py         # Contract ID resolution (stocks, options, fallbacks)
│   ├── market_data.py       # Market data, limit prices, FX & tick-size helpers
│   ├── exchange_hours.py    # Exchange trading hours & open/closed filtering
│   ├── cancel.py            # Shared order-cancellation logic & interactive prompt
│   ├── comparison.py        # Project_Portfolio vs current IBKR positions
│   ├── extra_positions.py   # Handle IBKR positions not in the input file
│   ├── reconcile.py         # Reconciliation against IBKR positions & orders
│   └── orders.py            # Interactive order placement loop
├── requirements.txt
└── README.md
```

## License

This project is provided as-is for personal use. Use at your own risk — automated trading involves financial risk. Always review orders before confirming.

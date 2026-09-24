# IBKR Automata

Python tool that rebalances an Interactive Brokers account toward a target
portfolio (the Citrindex plan) exported as an `.xlsx` in `assets/`. It resolves
contracts, fetches market data, reconciles against current positions and open
orders, and places only the net difference. Talks to TWS / IB Gateway through
`ib_async` (TWS API, not the old Client Portal REST API).

README.md is the user guide (modes, IBC/cron setup). Parts of it are stale:
orders are now Relative (REL) orders tuned by `PRICE_OFFSET` and
`LIMIT_PRICE_OFFSET`, not the `FILL_PATIENCE` limit formula it describes.
`src/config.py` is the source of truth for tunables.

## Environment

- Python via pyenv virtualenv `ibkr-automata`:
  `/Users/samuelcoron/.pyenv/versions/ibkr-automata/bin/python`
- Tests: `python -m pytest -q` (pytest.ini sets `testpaths = tests`). Tests mock
  `ib_async`; they never need TWS or the Gateway.
- Run: `python -m src.main [mode] [-all-exchanges] [-auto]`. Modes: none (full
  run), `noop`, `noop-recalculate`, `project-portfolio`, `buy-all`,
  `cancel-all-orders`, `print-project-vs-current`.
- `-auto` connects to IB Gateway (4001 live / 4002 paper) and starts it through
  IBC if needed; interactive runs connect to TWS (7496 live / 7497 paper).
  Ports come from `TRADING_MODE` in `.env`. All connections share one client id.

## Layout

- `src/main.py` CLI entry and workflow
- `src/config.py` settings: thresholds, order tuning, ticker redirects, ignore lists
- `src/portfolio.py` loads the input sheet, applies redirects and `IGNORE_NAMES`
- `src/contracts.py` conid resolution (stocks, options, JP/HK exchange redirects,
  dot-suffix tickers like `META.US`)
- `src/market_data.py`, `src/exchange_hours.py` (uses `exchange_calendars`)
- `src/reconcile.py` target vs current positions and orders
- `src/extra_positions.py` IBKR positions absent from the input (sold unless ignored)
- `src/orders.py`, `src/cancel.py` order placement and cancellation loops
- `src/gateway.py` IBC / Gateway lifecycle; `src/telegram.py` alerts in `-auto`
- `assets/` input sheets and `output/` results are gitignored

## Working rules

- This trades a live account. Never run the program in a mode that places or
  cancels orders (full run, `buy-all`, `project-portfolio`, `cancel-all-orders`,
  `-auto`) unless Samuel asks for that exact run. `noop` and
  `print-project-vs-current` are read-only.
- Show the diff and ask before committing, pushing, or opening a PR.
- Add or update tests in `tests/test_<module>.py` for every behaviour change and
  keep the full suite green.
- Match the existing style: module docstrings, typed signatures, section
  comments like `# --- Name ---` in config, `# ── name ──` in tests.
- For larger changes, write a short plan first (Samuel used Cursor plan mode
  for every feature so far).

## Ignore lists

Positions the program must never buy or sell (hedges, legacy holdings, manual
trades) go in `IGNORE_NAMES` in `src/config.py`, matched by full name, not
ticker. See the `ignore-position` skill.

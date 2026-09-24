---
name: ignore-position
description: Add or remove a security from IBKR Automata's ignore list so the rebalancer never buys or sells it (hedges, futures, legacy holdings, manual trades).
---

# Ignore a position

`IGNORE_NAMES` in `src/config.py` hides securities from the rebalancer. An
entry matches the full security name exactly (case-insensitive, surrounding
whitespace ignored), not the ticker:

- rows of the input sheet whose `Name` matches are dropped (never bought);
- held IBKR positions whose `longName` matches are not sold, even when absent
  from the input sheet.

Entries ending in ` (OPTION)` only match options on that underlying name
(e.g. `"INVESCO QQQ TRUST SERIES 1 (OPTION)"`).

## Finding the exact name

- For a held position, use IBKR's `longName` from `reqContractDetails`. It is
  printed in the run log (`Ignoring N extra position(s): ...` and the extra
  positions table) and in the `IBKR Name` column of
  `output/Project_Portfolio.csv`.
- Futures: the long name is shared by every contract month, so one entry
  survives rolls. Never ignore by conid. Example: SOFR3 is
  `"Secured Overnight Financing Rate 3-month Average of Rates"`.
- Confirm the security with Samuel before adding it.

## Steps

1. Add the name to `IGNORE_NAMES` in `src/config.py`, one entry per line, with
   a short comment naming the ticker when the name is not obvious.
2. Run `python -m pytest -q` with the `ibkr-automata` interpreter.
3. Verify read-only with `python -m src.main noop` and check that the
   position shows in the `Ignoring ...` line rather than as a sell.
4. Show Samuel the diff and ask before committing.

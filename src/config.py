"""Centralized configuration for IBKR Automata."""

import os

# --- TWS connection ---
TWS_HOST = "127.0.0.1"
TWS_PORT = 7497
TWS_CLIENT_ID = 1

# --- Paths ---
# Resolve relative to the project root (parent of src/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ASSETS_DIR = os.path.join(PROJECT_ROOT, "assets")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")

# --- Trading thresholds ---
MINIMUM_TRADING_AMOUNT = 100  # USD – net orders below this value are skipped

# --- API Keys ---
OPENAI_API_KEY_FILE = "/Users/samuelcoron/Keys/OpenAI_API"

"""Centralized configuration for IBKR Automata."""

import os

# --- Paths ---
GATEWAY_DIR = "/Users/samuelcoron/IBKR/clientportal.gw"
BASE_URL = "https://localhost:5001/v1/api"

# Resolve relative to the project root (parent of src/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ASSETS_DIR = os.path.join(PROJECT_ROOT, "assets")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")

# --- API Keys ---
OPENAI_API_KEY_FILE = "/Users/samuelcoron/Keys/OpenAI_API"

# --- Session ---
KEEPALIVE_INTERVAL_SECONDS = 55

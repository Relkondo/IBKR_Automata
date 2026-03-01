"""IB Gateway lifecycle management via IBC.

Provides helpers to detect whether IB Gateway is listening on the
configured port, start it through IBC if needed, and wait until
it is ready to accept API connections.
"""

from __future__ import annotations

import os
import socket
import subprocess
import time

from src.config import (
    IBC_PATH,
    IBC_INI,
    GATEWAY_TWS_PATH,
    TWS_MAJOR_VRSN,
    TWS_HOST,
    TWS_PORT,
    TRADING_MODE,
    GATEWAY_STARTUP_TIMEOUT,
)


def is_gateway_running(host: str = TWS_HOST, port: int = TWS_PORT) -> bool:
    """Return *True* if something is accepting TCP connections on *host*:*port*."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(2)
        try:
            sock.connect((host, port))
            return True
        except (ConnectionRefusedError, OSError):
            return False


def start_gateway(timeout: int = GATEWAY_STARTUP_TIMEOUT) -> subprocess.Popen:
    """Start IB Gateway via IBC and block until it accepts connections.

    Uses IBC's ``ibcstart.sh`` script which is the low-level launcher
    that does **not** open a new Terminal.app window (unlike
    ``gatewaystartmacos.sh``).

    Raises
    ------
    FileNotFoundError
        If the IBC start script cannot be found.
    RuntimeError
        If the gateway does not begin accepting connections within
        *timeout* seconds.
    """
    script = os.path.join(IBC_PATH, "scripts", "ibcstart.sh")
    if not os.path.isfile(script):
        raise FileNotFoundError(
            f"IBC start script not found at {script}.  "
            "Please install IBC (https://github.com/IbcAlpha/IBC) "
            f"and set IBC_PATH (currently '{IBC_PATH}')."
        )

    env = {
        **os.environ,
        "TWS_MAJOR_VRSN": TWS_MAJOR_VRSN,
        "IBC_INI": IBC_INI,
        "IBC_PATH": IBC_PATH,
        "TWS_PATH": GATEWAY_TWS_PATH,
        "TRADING_MODE": TRADING_MODE,
        "TWOFA_TIMEOUT_ACTION": "exit",
        "LOG_PATH": os.path.expanduser("~/ibc/logs"),
        "APP": "GATEWAY",
    }

    print(f"Starting IB Gateway via IBC ({script}) ...")
    proc = subprocess.Popen(
        [script],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    _wait_for_gateway(timeout)
    return proc


def _wait_for_gateway(timeout: int) -> None:
    """Poll the gateway port until it accepts connections or *timeout* expires."""
    poll_interval = 3
    elapsed = 0
    while elapsed < timeout:
        if is_gateway_running():
            print(f"IB Gateway is ready (took ~{elapsed}s).")
            return
        time.sleep(poll_interval)
        elapsed += poll_interval
        if elapsed % 15 == 0:
            print(f"  … still waiting for IB Gateway ({elapsed}s / {timeout}s)")

    raise RuntimeError(
        f"IB Gateway did not start within {timeout}s.  "
        "Check IBC logs at ~/ibc/logs/ and ensure 2FA was acknowledged "
        "if required."
    )


def ensure_gateway() -> None:
    """Make sure IB Gateway is accepting connections, starting it if needed.

    Safe to call when the gateway is already running — it simply returns
    immediately.

    Raises
    ------
    FileNotFoundError
        If IBC is not installed at the configured path.
    RuntimeError
        If the gateway cannot be started within the configured timeout.
    """
    if is_gateway_running():
        print("IB Gateway is already running.")
        return
    start_gateway()

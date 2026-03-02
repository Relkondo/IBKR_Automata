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
    GATEWAY_PORT,
    TRADING_MODE,
    GATEWAY_STARTUP_TIMEOUT,
)


def is_gateway_running(host: str = TWS_HOST, port: int = GATEWAY_PORT) -> bool:
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

    cmd = [
        script,
        TWS_MAJOR_VRSN,
        "--gateway",
        f"--tws-path={GATEWAY_TWS_PATH}",
        f"--ibc-path={IBC_PATH}",
        f"--ibc-ini={IBC_INI}",
        f"--mode={TRADING_MODE}",
        "--on2fatimeout=exit",
    ]

    log_dir = os.path.expanduser("~/ibc/logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "ibc_stdout.log")

    print(f"Starting IB Gateway via IBC ({script}) ...")
    print(f"  cmd: {' '.join(cmd)}")
    with open(log_file, "w") as fh:
        proc = subprocess.Popen(
            cmd,
            stdout=fh,
            stderr=subprocess.STDOUT,
        )

    _wait_for_gateway(timeout, proc, log_file)
    return proc


def _wait_for_gateway(
    timeout: int,
    proc: subprocess.Popen | None = None,
    log_file: str | None = None,
) -> None:
    """Poll the gateway port until it accepts connections or *timeout* expires."""
    poll_interval = 3
    elapsed = 0
    while elapsed < timeout:
        if is_gateway_running():
            print(f"IB Gateway is ready (took ~{elapsed}s).")
            return

        if proc is not None and proc.poll() is not None:
            tail = ""
            if log_file and os.path.isfile(log_file):
                with open(log_file) as fh:
                    tail = fh.read()[-2000:]
            raise RuntimeError(
                f"IBC process exited with code {proc.returncode} "
                f"before Gateway became ready.\n\n"
                f"--- IBC output (last 2000 chars) ---\n{tail}"
            )

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

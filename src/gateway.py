"""Launch and manage the IBKR Client Portal Gateway.

Responsibilities:
  - Start the CP Gateway Java process as a subprocess.
  - Prompt the user to authenticate in their browser.
  - Run a daemon keepalive thread (POST /tickle) while the main workflow runs.
  - Validate that the session is authenticated before handing control back.
"""

import os
import subprocess
import threading
import time

from src.api_client import IBKRClient
from src.config import GATEWAY_DIR, KEEPALIVE_INTERVAL_SECONDS


def launch_gateway() -> subprocess.Popen:
    """Start the Client Portal Gateway as a background subprocess.

    Returns the Popen handle so the caller can terminate it later.
    """
    run_script = os.path.join(GATEWAY_DIR, "bin", "run.sh")

    if not os.path.isfile(run_script):
        raise FileNotFoundError(
            f"Gateway run script not found at {run_script}. "
            "Please verify GATEWAY_DIR in config.py."
        )

    # run.sh expects a *relative* config path because it builds the
    # classpath and --conf flag using dirname/basename on the argument.
    # CWD is set to GATEWAY_DIR so "root/conf.yaml" resolves correctly.
    proc = subprocess.Popen(
        ["bin/run.sh", "root/conf.yaml"],
        cwd=GATEWAY_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,  # Own process group so we can kill the Java child too.
    )

    # Give the JVM a moment to start listening, then check it's alive.
    time.sleep(5)

    if proc.poll() is not None:
        # Process already exited – dump its output for debugging.
        output = proc.stdout.read() if proc.stdout else ""
        raise RuntimeError(
            f"Gateway exited immediately (code {proc.returncode}).\n"
            f"Output:\n{output}"
        )

    # Print the first few lines of gateway output for visibility.
    print("Gateway process started (pid={}).".format(proc.pid))
    return proc


def wait_for_auth(client: IBKRClient, auth_url: str = "https://localhost:5001") -> None:
    """Block until the user has authenticated through the browser.

    Prints instructions and polls ``/iserver/auth/status`` after the user
    signals they have completed the login flow.
    """
    print(
        "\n====================================================\n"
        "  IBKR Client Portal Gateway is starting.\n"
        f"  Please open {auth_url} in your browser\n"
        "  and complete the authentication.\n"
        "====================================================\n"
    )

    while True:
        input("Press ENTER once you have authenticated in the browser...")
        try:
            status = client.auth_status()
            if status.get("authenticated"):
                print("Session authenticated successfully.\n")
                return
            print(
                f"Not yet authenticated (status={status}). "
                "Please try again."
            )
        except Exception as exc:
            print(f"Could not reach gateway ({exc}). Is it running?")


# ------------------------------------------------------------------
# Keepalive
# ------------------------------------------------------------------

class SessionKeepalive:
    """Daemon thread that periodically tickles the CP Gateway session."""

    def __init__(self, client: IBKRClient):
        self._client = client
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._client.tickle()
            except Exception:
                pass  # Swallow – next tick will retry.
            self._stop_event.wait(timeout=KEEPALIVE_INTERVAL_SECONDS)

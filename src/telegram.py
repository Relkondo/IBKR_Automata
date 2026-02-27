"""Send Telegram notifications for flagged orders."""

from __future__ import annotations

import urllib.parse
import urllib.request

from src.config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

_API_URL = "https://api.telegram.org/bot{token}/sendMessage"


def _is_configured() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def send_message(text: str) -> bool:
    """Send *text* to the configured Telegram chat.

    Returns ``True`` on success, ``False`` (with a printed warning) on
    failure or if credentials are missing.
    """
    if not _is_configured():
        print("  [!] Telegram not configured — skipping notification.")
        return False

    url = _API_URL.format(token=TELEGRAM_BOT_TOKEN)
    payload = urllib.parse.urlencode({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
    }).encode()

    try:
        req = urllib.request.Request(url, data=payload)
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception as exc:
        print(f"  [!] Telegram send failed: {exc}")
        return False


def notify_flagged_orders(
    rejected: list[dict],
    large: list[dict],
) -> None:
    """Build and send a Telegram summary for rejected/cancelled and
    large orders.

    Each dict should have at minimum: ``ticker``, ``name``, ``side``,
    ``quantity``, ``usd_amount``.  Rejected dicts also have ``reason``.
    """
    if not rejected and not large:
        return
    if not _is_configured():
        return

    lines: list[str] = ["*IBKR Automata — Flagged Orders*\n"]

    if rejected:
        lines.append(f"*Rejected / Cancelled ({len(rejected)}):*")
        for o in rejected:
            lines.append(
                f"  • {o['side']} {o['quantity']} {o['ticker']} "
                f"({o['name']}) [{o.get('exchange', '?')}]"
                f" — ${o['usd_amount']:,.0f}")
            reason = o.get("reason", "")
            if reason:
                lines.append(f"    _{reason}_")
        lines.append("")

    if large:
        lines.append(f"*Large Orders ({len(large)}):*")
        for o in large:
            status = o.get("status", "")
            lines.append(
                f"  • {o['side']} {o['quantity']} {o['ticker']} "
                f"({o['name']}) [{o.get('exchange', '?')}]"
                f" — ${o['usd_amount']:,.0f}"
                + (f"  [{status}]" if status else ""))
        lines.append("")

    message = "\n".join(lines)
    if send_message(message):
        print("  Telegram notification sent.")
    else:
        print("  [!] Failed to send Telegram notification.")

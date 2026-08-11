"""Low-noise Telegram setup reporter.

Credentials are read only from environment variables and are never returned by
the API or written to the event store.  The reporter sends paper/read-only
setups and confirmed entries; it never sends execution commands.
"""

from __future__ import annotations

from typing import Any

import httpx


class TelegramReporter:
    def __init__(self, token: str | None, chat_id: str | None, enabled: bool = True) -> None:
        self.token = token
        self.chat_id = chat_id
        self.enabled = bool(enabled and token and chat_id)
        self.last_error: str | None = None
        self.last_sent_at: int | None = None
        self.last_signature: str | None = None

    def status(self) -> dict[str, Any]:
        return {"enabled": self.enabled, "configured": bool(self.token and self.chat_id), "last_sent_at": self.last_sent_at, "last_error": self.last_error, "mode": "READ_ONLY_SETUP_REPORT"}

    async def consider(self, decision: dict[str, Any]) -> bool:
        final = str(decision.get("final_decision") or "NO TRADE")
        if final not in {"WAIT LONG", "WAIT SHORT", "LONG SETUP", "SHORT SETUP", "LONG ENTRY CONFIRMED", "SHORT ENTRY CONFIRMED"}:
            return False
        signature = "|".join(str(decision.get(key)) for key in ("final_decision", "entry", "stop_loss", "take_profit", "target_type"))
        if signature == self.last_signature:
            return False
        if not self.enabled:
            self.last_signature = signature
            self.last_error = "Telegram is not configured; set CODEXIN_TELEGRAM_BOT_TOKEN and CODEXIN_TELEGRAM_CHAT_ID"
            return False
        target = decision.get("primary_target") or {}
        monitor = decision.get("position_monitor") or {}
        lines = [
            "Codexin Order Flow — READ ONLY SETUP",
            f"Decision: {final}",
            f"Regime / 5M / 1M: {decision.get('market_regime', 'N/A')} / {decision.get('five_minute_direction', 'N/A')} / {(decision.get('one_minute_structure') or {}).get('label', 'N/A')}",
            f"Scores: LONG {decision.get('long_score', 'N/A')} · SHORT {decision.get('short_score', 'N/A')}",
            f"1S: {(decision.get('one_second_confirmation') or {}).get('status', 'N/A')}",
            f"Entry / SL / TP: {decision.get('entry', 'N/A')} / {decision.get('stop_loss', 'N/A')} / {decision.get('take_profit', 'N/A')}",
            f"Primary target: {target.get('price', 'N/A')} · {target.get('type', decision.get('target_type', 'N/A'))}",
            f"RR: {decision.get('risk_reward', 'N/A')} · path: {(decision.get('order_book_path') or {}).get('label', 'N/A')}",
            f"Reason: {decision.get('final_reason', 'N/A')}",
            "Liquidation levels are ESTIMATED; order-book liquidity is OBSERVED aggregated L2.",
        ]
        if monitor.get("status") not in (None, "NO ACTIVE POSITION"):
            lines.append(f"Position monitor: {monitor.get('status')}")
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            async with httpx.AsyncClient(timeout=8) as client:
                response = await client.post(url, data={"chat_id": self.chat_id, "text": "\n".join(lines), "disable_web_page_preview": "true"})
                response.raise_for_status()
            self.last_signature = signature
            self.last_sent_at = int(decision.get("generated_at") or 0)
            self.last_error = None
            return True
        except Exception as error:
            self.last_error = str(error)
            return False

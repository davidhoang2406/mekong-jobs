import json
import logging
import os
import urllib.error
import urllib.request

log = logging.getLogger(__name__)


def telegram_alert_sink():
    """Returns a PyFlink MapFunction that sends string messages to Telegram as a side effect.

    RichSinkFunction was removed in PyFlink 2.0; MapFunction.open() provides the
    same per-task initialisation. Use as: .map(telegram_alert_sink()).print()

    Reads TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID from the environment.
    Logs a warning and suppresses silently when either var is missing.
    """
    from pyflink.datastream.functions import MapFunction

    class _TelegramAlertSink(MapFunction):
        def open(self, runtime_context):
            self._token = os.getenv("TELEGRAM_BOT_TOKEN")
            self._chat_id = os.getenv("TELEGRAM_CHAT_ID")
            if not self._token or not self._chat_id:
                log.warning("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set — alerts suppressed")

        def map(self, text: str):
            if self._token and self._chat_id:
                payload = json.dumps({
                    "chat_id": self._chat_id,
                    "text": text,
                    "parse_mode": "Markdown",
                }).encode()
                req = urllib.request.Request(
                    f"https://api.telegram.org/bot{self._token}/sendMessage",
                    data=payload,
                    headers={"Content-Type": "application/json"},
                )
                try:
                    urllib.request.urlopen(req, timeout=10)
                except urllib.error.HTTPError as e:
                    log.error("Telegram API error %s: %s", e.code, e.read().decode())
                except Exception as e:
                    log.error("Failed to send Telegram alert: %s", e)
            return text

    return _TelegramAlertSink()

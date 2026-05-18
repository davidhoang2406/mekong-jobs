import json
import logging
import os
from pathlib import Path

COOLDOWN_MS = int(os.getenv("ALERT_COOLDOWN_MS", str(5 * 60 * 1000)))  # 5 min default

from dotenv import load_dotenv

_ROOT = Path(__file__).parent.parent.parent
load_dotenv(_ROOT / ".env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

from market_data_models.topics import CRYPTO_PRICE_REALTIME, STOCK_PRICE_REALTIME
from jobs.sinks import telegram_alert_sink
from jobs.utils import evaluate_rules, load_json_config, validate_rules

TOPICS = [STOCK_PRICE_REALTIME, CRYPTO_PRICE_REALTIME]
ALERTS_CONFIG = _ROOT / "config" / "alerts.json"

# Used in local mode only. In Docker the JAR is already in $FLINK_HOME/lib/.
_JAR = _ROOT / "jars" / "flink-sql-connector-kafka-4.0.1-2.0.jar"


def run() -> None:
    try:
        from pyflink.common import WatermarkStrategy
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.common.typeinfo import Types
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.datastream.connectors.kafka import (
            KafkaOffsetsInitializer,
            KafkaSource,
        )
        from pyflink.datastream.functions import KeyedProcessFunction
        from pyflink.datastream.state import MapStateDescriptor
    except ImportError:
        raise SystemExit(
            "apache-flink is not installed.\n"
            "Run via Docker instead:  make flink-build && make run-flink-alert"
        )

    rules = validate_rules(load_json_config(ALERTS_CONFIG))

    class PriceAlertFunction(KeyedProcessFunction):
        """Per-symbol alert evaluation with per-rule cooldown via MapState."""

        def open(self, ctx):
            self._last_fired = self.runtime_context.get_map_state(
                MapStateDescriptor("last_fired", Types.STRING(), Types.LONG())
            )

        def process_element(self, msg: dict, ctx: "KeyedProcessFunction.Context"):
            symbol  = msg.get("symbol", "")
            payload = msg.get("payload", {})
            source  = msg.get("source", "")
            price   = payload.get("price", 0.0)
            pct     = payload.get("pct_change", 0.0)
            ts      = msg.get("timestamp", "")[:19]
            now     = ctx.timer_service().current_processing_time()

            log.info("tick  %-12s  price=%.4f  pct=%+.2f%%  source=%s", symbol, price, pct, source)

            for hit in evaluate_rules(rules, symbol, payload, source):
                rule_key = f"{hit['field']}:{hit['operator']}:{hit['threshold']}"
                last = self._last_fired.get(rule_key)
                if last is not None and (now - last) < COOLDOWN_MS:
                    continue
                self._last_fired.put(rule_key, now)
                yield (
                    f"🚨 *Price Alert*\n"
                    f"*Symbol:* `{symbol}` | *Time:* `{ts}`\n"
                    f"*Signal:* {hit['message']}\n"
                    f"*Price:* `{price:.2f}` | *Change:* `{pct:+.2f}%`"
                )

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)

    if _JAR.exists():
        env.add_jars(f"file://{_JAR.resolve()}")

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap)
        .set_topics(*TOPICS)
        .set_group_id("flink-alerts")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka · price.realtime (stock + crypto)",
    )

    (
        stream
        .map(json.loads)
        .filter(lambda m: m.get("event_type") == "price.snapshot")
        .key_by(lambda m: m["symbol"])
        .process(PriceAlertFunction())
        .map(telegram_alert_sink())
        .print()
    )

    log.info("Starting PriceAlertJob | topics=%s | rules=%d", TOPICS, len(rules))
    env.execute("PriceAlertJob")


if __name__ == "__main__":
    run()

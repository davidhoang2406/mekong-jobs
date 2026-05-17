import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

_ROOT = Path(__file__).parent.parent.parent
load_dotenv(_ROOT / ".env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

from market_data_models.topics import CRYPTO_PRICE_REALTIME, STOCK_PRICE_REALTIME
from jobs.utils import evaluate_rules, load_json_config, validate_rules

TOPICS = [STOCK_PRICE_REALTIME, CRYPTO_PRICE_REALTIME]
ALERTS_CONFIG = _ROOT / "config" / "alerts.json"

# Used in local mode only. In Docker the JAR is already in $FLINK_HOME/lib/.
_JAR = _ROOT / "jars" / "flink-sql-connector-kafka-4.0.1-2.0.jar"


def run() -> None:
    try:
        from pyflink.common import WatermarkStrategy
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.datastream.connectors.kafka import (
            KafkaOffsetsInitializer,
            KafkaSource,
        )
        from pyflink.datastream.functions import KeyedProcessFunction
    except ImportError:
        raise SystemExit(
            "apache-flink is not installed.\n"
            "Run via Docker instead:  make flink-build && make run-flink-alert"
        )

    rules = validate_rules(load_json_config(ALERTS_CONFIG))

    class PriceAlertFunction(KeyedProcessFunction):
        """
        Stateless per-element check against threshold rules.
        Keyed by symbol — prerequisite for adding ValueState/timers in future phases.
        """

        def process_element(self, msg: dict, ctx: "KeyedProcessFunction.Context"):
            symbol  = msg.get("symbol", "")
            payload = msg.get("payload", {})
            source  = msg.get("source", "")
            price   = payload.get("price", 0.0)
            pct     = payload.get("pct_change", 0.0)
            ts      = msg.get("timestamp", "")[:19]

            log.info("tick  %-12s  price=%.4f  pct=%+.2f%%  source=%s", symbol, price, pct, source)

            for hit in evaluate_rules(rules, symbol, payload, source):
                alert = (
                    f"[ALERT {ts}] {symbol:10s} | {hit['message']}"
                    f" | price={price:.2f}  pct={pct:+.2f}%"
                    f"  {hit['matched_field']}={hit['matched_value']}"
                )
                log.warning(alert)
                yield alert

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
        .print()
    )

    log.info("Starting PriceAlertJob | topics=%s | rules=%d", TOPICS, len(rules))
    env.execute("PriceAlertJob")


if __name__ == "__main__":
    run()

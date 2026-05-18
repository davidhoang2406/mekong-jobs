import io
import json
import logging
import math
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

_ROOT = Path(__file__).parent.parent.parent
load_dotenv(_ROOT / ".env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

from market_data_models.topics import CRYPTO_PRICE_REALTIME, STOCK_PRICE_REALTIME
from jobs.utils import load_json_config

TOPICS = [STOCK_PRICE_REALTIME, CRYPTO_PRICE_REALTIME]
VOLATILITY_CONFIG = _ROOT / "config" / "volatility.json"

_JAR = _ROOT / "jars" / "flink-sql-connector-kafka-4.0.1-2.0.jar"


def _std_dev(values: list[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    mean = sum(values) / n
    return math.sqrt(sum((v - mean) ** 2 for v in values) / n)


def run() -> None:
    try:
        from pyflink.common import WatermarkStrategy
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.common.time import Time
        from pyflink.common.typeinfo import Types
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.datastream.connectors.kafka import (
            KafkaOffsetsInitializer,
            KafkaSource,
        )
        from pyflink.datastream.functions import (
            KeyedProcessFunction,
            ProcessWindowFunction,
            RichSinkFunction,
        )
        from pyflink.datastream.state import MapStateDescriptor
        from pyflink.datastream.window import SlidingProcessingTimeWindows
    except ImportError:
        raise SystemExit(
            "apache-flink is not installed.\n"
            "Run via Docker instead:  make flink-build && make run-volatility-burst"
        )

    cfg = load_json_config(VOLATILITY_CONFIG)
    window_size_s: int = cfg.get("window_size_s", 300)
    slide_size_s: int = cfg.get("slide_size_s", 60)
    std_dev_threshold: float = cfg.get("std_dev_threshold", 0.5)
    min_ticks: int = cfg.get("min_ticks", 5)
    cooldown_ms: int = cfg.get("cooldown_ms", 300_000)
    minio_prefix: str = cfg.get("minio_prefix", "volatility.burst")
    flush_interval_s: int = cfg.get("flush_interval_s", 60)
    flush_batch_size: int = cfg.get("flush_batch_size", 50)

    class VolatilityWindowFunction(ProcessWindowFunction):
        """Computes price std dev over the sliding window; emits an alert dict when threshold is exceeded."""

        def process(self, symbol: str, context, elements):
            prices = [e["price"] for e in elements if e.get("price") is not None]
            if len(prices) < min_ticks:
                return
            sd = _std_dev(prices)
            if sd < std_dev_threshold:
                return
            yield {
                "symbol": symbol,
                "std_dev": round(sd, 6),
                "price_count": len(prices),
                "mean_price": round(sum(prices) / len(prices), 6),
                "window_start_ms": context.window().start,
                "window_end_ms": context.window().end,
                "alert_ts_ms": int(time.time() * 1000),
            }

    class CooldownFunction(KeyedProcessFunction):
        """Suppresses repeated alerts for the same symbol within the cooldown window."""

        def open(self, ctx):
            self._last_fired = self.runtime_context.get_map_state(
                MapStateDescriptor("volatility_cooldown", Types.STRING(), Types.LONG())
            )

        def process_element(self, value, ctx):
            symbol = value["symbol"]
            now = ctx.timer_service().current_processing_time()
            last = self._last_fired.get(symbol)
            if last is not None and (now - last) < cooldown_ms:
                return
            self._last_fired.put(symbol, now)
            log.warning(
                "[VOLATILITY BURST] %-12s  std_dev=%.4f  ticks=%d  mean_price=%.4f",
                symbol, value["std_dev"], value["price_count"], value["mean_price"],
            )
            yield value

    class MinioVolatilitySink(RichSinkFunction):
        """Buffers volatility alerts and flushes them as NDJSON files to the market-analysis bucket."""

        def open(self, runtime_context):
            from minio import Minio
            raw_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
            secure = raw_endpoint.startswith("https://")
            endpoint = raw_endpoint.replace("https://", "").replace("http://", "")
            self._client = Minio(
                endpoint,
                access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
                secure=secure,
            )
            self._bucket = os.getenv("MINIO_ANALYSIS_BUCKET", "market-analysis")
            self._prefix = minio_prefix
            self._buffer: list[dict] = []
            self._last_flush = time.time()

        def invoke(self, value, context):
            self._buffer.append(value)
            elapsed = time.time() - self._last_flush
            if len(self._buffer) >= flush_batch_size or elapsed >= flush_interval_s:
                self._flush()

        def close(self):
            self._flush()

        def _flush(self):
            if not self._buffer:
                return
            now = datetime.now(timezone.utc)
            object_name = (
                f"{self._prefix}/year={now.year}/month={now.month:02d}/"
                f"day={now.day:02d}/hour={now.hour:02d}/{uuid.uuid4().hex}.json"
            )
            payload = "\n".join(json.dumps(r) for r in self._buffer).encode()
            self._client.put_object(
                self._bucket,
                object_name,
                io.BytesIO(payload),
                length=len(payload),
                content_type="application/x-ndjson",
            )
            log.info(
                "Flushed %d volatility alerts → %s/%s",
                len(self._buffer), self._bucket, object_name,
            )
            self._buffer = []
            self._last_flush = time.time()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)

    if _JAR.exists():
        env.add_jars(f"file://{_JAR.resolve()}")

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap)
        .set_topics(*TOPICS)
        .set_group_id("flink-volatility")
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
        .map(lambda m: {**m.get("payload", {}), "symbol": m["symbol"], "source": m.get("source", "")})
        .key_by(lambda m: m["symbol"])
        .window(SlidingProcessingTimeWindows.of(Time.seconds(window_size_s), Time.seconds(slide_size_s)))
        .process(VolatilityWindowFunction())
        .key_by(lambda v: v["symbol"])
        .process(CooldownFunction())
        .add_sink(MinioVolatilitySink())
    )

    log.info(
        "Starting VolatilityBurstJob | window=%ds  slide=%ds  threshold=%.2f  cooldown=%dms",
        window_size_s, slide_size_s, std_dev_threshold, cooldown_ms,
    )
    env.execute("VolatilityBurstJob")


if __name__ == "__main__":
    run()

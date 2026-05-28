# mekong-jobs

Flink stream jobs and Spark batch jobs for the Mekong platform.

- **Stream (Flink)** — real-time alerts and burst detectors over Kafka topics.
- **Batch (Spark)** — derive OHLCV, technical indicators, daily digests,
  screener results from raw snapshots in MinIO. Submitted as
  `SparkApplication` CRDs by Dagster (`mekong-dagster`).

## Layout

```
jobs/
  stream/
    price_alert_job.py        # Flink KeyedProcessFunction — threshold alerts
    volatility_burst_job.py   # Flink sliding window + ValueState
  batch/
    ohlcv_daily_ingest.py     # Spark — derive daily OHLCV bars (Parquet)
    technical_job.py          # Spark — SMA/RSI/MACD/Bollinger Bands
    digest_job.py             # Spark — daily gainers/losers/volume digest
    screener_job.py           # Spark — weekly fundamental screener
model/
  spark.py                    # SparkFactory (S3A pre-wired, eventLog → MinIO)
  minio_store.py              # MinIO Parquet/Avro read+write
config/                       # alerts.json, screener thresholds, ingest config
main.py                       # CLI dispatch for every job
Dockerfile.flink              # PyFlink image
Dockerfile.spark              # PySpark image (used by SparkApplication CRD)
```

## Quick start

```bash
make install        # creates .venv, installs requirements.txt (for tests)
make test
```

The actual jobs run inside Kubernetes — locally for development you can run
`main.py` directly with `SPARK_MASTER_URL=local[*]`.

## CLI

```
python main.py <command> [options]
```

| Command | Type | Notes |
|---|---|---|
| `ohlcv-daily-ingest` | Spark | `--date YYYY-MM-DD` (default today) |
| `technical` | Spark | `--full-recompute` to ignore checkpoint |
| `digest` | Spark | `--date YYYY-MM-DD` |
| `screener` | Spark | `--date YYYY-MM-DD` (any day in the target ISO week) |
| `flink-alert` | Flink | submit via `flink run --python main.py -- flink-alert` |
| `volatility-burst` | Flink | submit via `flink run --python main.py -- volatility-burst` |

## How jobs are submitted in production

- **Spark batch jobs** are submitted by `mekong-dagster` as `SparkApplication`
  CRDs to the spark-operator running in `mekong-processing`. Driver and
  executor pods are scheduled by the operator; logs land in
  `s3a://market-data/spark-events` and are picked up by Spark History.
- **Flink stream jobs** run as long-lived `FlinkDeployment` CRDs in
  `mekong-processing`, managed by the Flink Kubernetes operator.

## Environment

| Var | Purpose |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka brokers |
| `MINIO_ENDPOINT` | MinIO S3 endpoint |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | MinIO credentials |
| `MINIO_BUCKET` | Raw bucket (default `market-data`) |
| `MINIO_ANALYSIS_BUCKET` | Derived bucket (default `market-analysis`) |
| `SPARK_MASTER_URL` | Override Spark master for local runs (default `local[*]`) |

## MinIO layout written

```
market-analysis/
  ohlcv.bar/asset_class=*/year=/month=/day=/part-*.parquet
  technical.indicators/year=/month=/day=/part-*.parquet
  digest/year=/month=/day=/part-*.parquet
  screener/year=/week=/part-*.parquet
```

## Docker images

Built and pushed by CI:

```
ghcr.io/davidhoang2406/mekong-spark:latest
ghcr.io/davidhoang2406/mekong-flink:latest
```

Both images embed this repo at `/opt/project`. The Spark image is referenced
by the `SparkApplication` template in `mekong-dagster/resources.py`.

## Depends on

- [`mekong-data-models`](https://github.com/davidhoang2406/mekong-data-models) — Avro schemas, topic constants
- `mekong-infra` — Kafka, MinIO, Spark/Flink operators
- `mekong-dagster` — schedules and submits the batch jobs

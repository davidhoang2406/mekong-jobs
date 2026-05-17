import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import functions as F

from model.minio_store import MinioStore
from model.spark import SparkFactory

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

RAW_BUCKET      = os.getenv("MINIO_BUCKET", "market-data")
ANALYSIS_BUCKET = os.getenv("MINIO_ANALYSIS_BUCKET", "market-analysis")
CONFIG          = Path(__file__).parent.parent.parent / "config" / "ohlcv_ingest.json"


def run(target_date: str | None = None) -> None:
    if target_date:
        target = date.fromisoformat(target_date)
    else:
        config   = json.loads(CONFIG.read_text())
        lookback = config.get("lookback_days", 0)
        target   = date.today() - timedelta(days=lookback)

    year  = target.strftime("%Y")
    month = target.strftime("%m")
    day   = target.strftime("%d")

    log.info("OHLCV daily ingest | date=%s | src=%s → dst=%s",
             target.isoformat(), RAW_BUCKET, ANALYSIS_BUCKET)

    raw_store     = MinioStore(RAW_BUCKET)
    date_fragment = f"/year={year}/month={month}/day={day}/"
    has_data = any(
        date_fragment in obj.object_name
        for obj in raw_store.list_objects(prefix="price.snapshot/")
    )
    if not has_data:
        log.warning("No price snapshots found for %s — nothing to ingest", target.isoformat())
        return

    target_start = datetime(target.year, target.month, target.day, tzinfo=timezone.utc)
    target_end   = target_start + timedelta(days=1)
    dst = f"s3a://{ANALYSIS_BUCKET}/ohlcv.bar"

    with SparkFactory("ohlcv_daily_ingest") as spark:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        df = (spark.read.format("avro")
              .option("recursiveFileLookup", "true")
              .load(f"s3a://{RAW_BUCKET}/price.snapshot")
              .filter((F.col("time") >= F.lit(target_start)) &
                      (F.col("time") <  F.lit(target_end))))

        df_ohlcv = (
            df.groupBy("symbol", "exchange")
            .agg(
                F.min(F.struct("time", "price")).getField("price").alias("open"),
                F.max("price").alias("high"),
                F.min("price").alias("low"),
                F.max(F.struct("time", "price")).getField("price").alias("close"),
                F.sum("volume").alias("volume"),
                F.min("time").alias("_min_time"),
            )
            .withColumn("time", F.date_trunc("day", F.col("_min_time")))
            .withColumn("asset_class",
                F.when(F.col("symbol").contains("/"), F.lit("crypto")).otherwise(F.lit("stock")))
            .withColumn("year",  F.lit(year))
            .withColumn("month", F.lit(month))
            .withColumn("day",   F.lit(day))
            .drop("_min_time")
        )

        df_valid = df_ohlcv.filter(
            (F.col("high") >= F.col("low")) &
            (F.col("high") >= F.greatest("open", "close")) &
            (F.col("low")  <= F.least("open", "close")) &
            (F.col("volume") >= 0)
        )
        dropped = df_ohlcv.count() - df_valid.count()
        if dropped:
            log.warning("OHLCV quality check: dropped %d bars with invalid high/low/volume", dropped)

        (df_valid
         .write
         .mode("overwrite")
         .partitionBy("asset_class", "year", "month", "day")
         .parquet(dst))

        log.info("Done | bars written for %s → %s", target.isoformat(), dst)

import json
import logging
import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from model.spark import SparkFactory

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ANALYSIS_BUCKET = os.getenv("MINIO_ANALYSIS_BUCKET", "market-analysis")
CONFIG = Path(__file__).parent.parent.parent / "config" / "digest.json"


def run(target_date: str | None = None) -> None:
    config = json.loads(CONFIG.read_text())
    top_n  = config.get("top_n", 10)

    target = date.fromisoformat(target_date) if target_date else date.today()
    year, month, day = target.strftime("%Y"), target.strftime("%m"), target.strftime("%d")

    src = f"s3a://{ANALYSIS_BUCKET}/ohlcv.bar"
    dst = f"s3a://{ANALYSIS_BUCKET}/digest"

    log.info("DigestJob | date=%s | top_n=%d | %s → %s", target.isoformat(), top_n, src, dst)

    with SparkFactory("DigestJob") as spark:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        df = (spark.read.parquet(src)
              .filter((F.col("year") == year) & (F.col("month") == month) & (F.col("day") == day))
              .withColumn("pct_change",
                          ((F.col("close") - F.col("open")) / F.col("open") * 100)))

        if df.rdd.isEmpty():
            log.warning("No OHLCV bars found for %s — skipping digest", target.isoformat())
            return

        w_gain = Window.orderBy(F.col("pct_change").desc())
        w_loss = Window.orderBy(F.col("pct_change").asc())
        w_vol  = Window.orderBy(F.col("volume").desc())

        cols = ["category", "rank", "symbol", "exchange", "asset_class",
                "open", "close", "volume", "pct_change"]

        gainers = (df.filter(F.col("pct_change") > 0)
                   .withColumn("rank", F.row_number().over(w_gain))
                   .filter(F.col("rank") <= top_n)
                   .withColumn("category", F.lit("gainer"))
                   .select(cols))

        losers = (df.filter(F.col("pct_change") < 0)
                  .withColumn("rank", F.row_number().over(w_loss))
                  .filter(F.col("rank") <= top_n)
                  .withColumn("category", F.lit("loser"))
                  .select(cols))

        by_volume = (df.withColumn("rank", F.row_number().over(w_vol))
                     .filter(F.col("rank") <= top_n)
                     .withColumn("category", F.lit("volume"))
                     .select(cols))

        (gainers.unionAll(losers).unionAll(by_volume)
         .withColumn("year",  F.lit(year))
         .withColumn("month", F.lit(month))
         .withColumn("day",   F.lit(day))
         .write.mode("overwrite")
         .partitionBy("year", "month", "day")
         .parquet(dst))

    log.info("DigestJob done | date=%s → %s", target.isoformat(), dst)

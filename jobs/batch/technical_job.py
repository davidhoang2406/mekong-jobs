import logging
import os
from datetime import date

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

from model.spark import SparkFactory

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ANALYSIS_BUCKET = os.getenv("MINIO_ANALYSIS_BUCKET", "market-analysis")

_MIN = {"sma20": 20, "sma50": 50, "sma200": 200, "rsi": 14, "bb": 20, "macd": 26}
_LOOKBACK = 200  # rows per symbol to keep in the checkpoint

_MACD_SCHEMA = StructType([
    StructField("symbol",      StringType()),
    StructField("time",        TimestampType()),
    StructField("macd",        DoubleType()),
    StructField("macd_signal", DoubleType()),
    StructField("macd_hist",   DoubleType()),
])


def _macd_per_symbol(pdf):
    """applyInPandas function: computes MACD per symbol using pandas EWM (EMA)."""
    import pandas as pd
    pdf = pdf.sort_values("time").copy()
    close  = pdf["close"].astype(float)
    ema12  = close.ewm(span=12, adjust=False).mean()
    ema26  = close.ewm(span=26, adjust=False).mean()
    macd   = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    return pd.DataFrame({
        "symbol":      pdf["symbol"].values,
        "time":        pdf["time"].values,
        "macd":        macd.values,
        "macd_signal": signal.values,
        "macd_hist":   (macd - signal).values,
    })


def _add_indicators(df: DataFrame) -> DataFrame:
    w_ord  = Window.partitionBy("symbol").orderBy("time")
    w20    = w_ord.rowsBetween(-19,  0)
    w50    = w_ord.rowsBetween(-49,  0)
    w200   = w_ord.rowsBetween(-199, 0)
    w14    = w_ord.rowsBetween(-13,  0)

    df = df.withColumn("_n", F.row_number().over(w_ord))

    df = (df
        .withColumn("sma20",  F.when(F.col("_n") >= _MIN["sma20"],  F.avg("close").over(w20)))
        .withColumn("sma50",  F.when(F.col("_n") >= _MIN["sma50"],  F.avg("close").over(w50)))
        .withColumn("sma200", F.when(F.col("_n") >= _MIN["sma200"], F.avg("close").over(w200)))
    )

    bb_mid = F.avg("close").over(w20)
    bb_std = F.stddev_pop("close").over(w20)
    df = (df
        .withColumn("bb_mid",   F.when(F.col("_n") >= _MIN["bb"], bb_mid))
        .withColumn("bb_upper", F.when(F.col("_n") >= _MIN["bb"], bb_mid + 2 * bb_std))
        .withColumn("bb_lower", F.when(F.col("_n") >= _MIN["bb"], bb_mid - 2 * bb_std))
    )

    prev = F.lag("close", 1).over(w_ord)
    df = (df
        .withColumn("_prev", prev)
        .withColumn("_gain", F.when(F.col("close") > F.col("_prev"), F.col("close") - F.col("_prev")).otherwise(F.lit(0.0)))
        .withColumn("_loss", F.when(F.col("close") < F.col("_prev"), F.col("_prev") - F.col("close")).otherwise(F.lit(0.0)))
    )
    avg_gain = F.avg("_gain").over(w14)
    avg_loss = F.avg("_loss").over(w14)
    df = df.withColumn(
        "rsi14",
        F.when(F.col("_n") >= _MIN["rsi"],
            F.when(avg_loss == 0, F.lit(100.0))
             .otherwise(100.0 - 100.0 / (1.0 + avg_gain / avg_loss))
        )
    ).drop("_prev", "_gain", "_loss")

    macd_df = (
        df.select("symbol", "time", "close")
          .groupBy("symbol")
          .applyInPandas(_macd_per_symbol, schema=_MACD_SCHEMA)
    )
    df = df.join(macd_df, ["symbol", "time"])
    df = df.withColumn("macd",        F.when(F.col("_n") >= _MIN["macd"], F.col("macd")))
    df = df.withColumn("macd_signal", F.when(F.col("_n") >= _MIN["macd"], F.col("macd_signal")))
    df = df.withColumn("macd_hist",   F.when(F.col("_n") >= _MIN["macd"], F.col("macd_hist")))

    return df.drop("_n")


def _read_checkpoint(spark: SparkSession, path: str) -> DataFrame | None:
    try:
        df = spark.read.parquet(path)
        count = df.count()
        if count > 0:
            log.info("Loaded checkpoint: %d rows from %s", count, path)
            return df
    except Exception:
        pass
    log.info("No checkpoint found at %s — will do a full scan", path)
    return None


def _write_checkpoint(df: DataFrame, path: str) -> None:
    """Keep only the last _LOOKBACK rows per symbol, ordered by time."""
    w = Window.partitionBy("symbol").orderBy(F.col("time").desc())
    trimmed = (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") <= _LOOKBACK)
          .drop("_rn")
    )
    trimmed.write.mode("overwrite").parquet(path)
    log.info("Checkpoint written: %d rows → %s", trimmed.count(), path)


def run(full_recompute: bool = False, target_date: str | None = None) -> None:
    ohlcv_path      = f"s3a://{ANALYSIS_BUCKET}/ohlcv.bar"
    dst             = f"s3a://{ANALYSIS_BUCKET}/technical.indicators"
    checkpoint_path = f"s3a://{ANALYSIS_BUCKET}/technical.checkpoint"
    target = date.fromisoformat(target_date) if target_date else date.today()
    year  = target.strftime("%Y")
    month = target.strftime("%m")
    day   = target.strftime("%d")

    with SparkFactory("TechnicalJob") as spark:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        checkpoint = None if full_recompute else _read_checkpoint(spark, checkpoint_path)

        if checkpoint is not None:
            # Incremental: find the latest timestamp in the checkpoint,
            # load only OHLCV bars strictly after that, then combine.
            max_time = checkpoint.agg(F.max("time")).collect()[0][0]
            log.info("Incremental mode | checkpoint max time: %s", max_time)

            new_bars = (
                spark.read.parquet(ohlcv_path)
                     .filter(F.col("time") > F.lit(max_time))
            )
            new_count = new_bars.count()

            if new_count == 0:
                log.info("No new OHLCV bars since checkpoint — recomputing from checkpoint only")

            log.info("New bars since checkpoint: %d", new_count)

            # Union checkpoint history with new bars (checkpoint columns match OHLCV columns)
            ohlcv_cols = ["time", "symbol", "exchange", "open", "high", "low", "close", "volume"]
            df = checkpoint.select(ohlcv_cols).unionByName(new_bars.select(ohlcv_cols))
        else:
            log.info("Full scan mode | reading all OHLCV bars from %s", ohlcv_path)
            df = spark.read.parquet(ohlcv_path)

        df = _add_indicators(df)

        latest = Window.partitionBy("symbol").orderBy(F.col("time").desc())
        df_out = (
            df
            .withColumn("_rn", F.row_number().over(latest))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .withColumn("year",  F.lit(year))
            .withColumn("month", F.lit(month))
            .withColumn("day",   F.lit(day))
        )

        df_out.write.mode("overwrite").partitionBy("year", "month", "day").parquet(dst)
        log.info("Indicators written | date=%s → %s", target.isoformat(), dst)

        # Update checkpoint with the last _LOOKBACK bars per symbol from the
        # full working set (checkpoint + new bars), so the next run can be incremental.
        _write_checkpoint(df, checkpoint_path)

    log.info("TechnicalJob done | date=%s", target.isoformat())

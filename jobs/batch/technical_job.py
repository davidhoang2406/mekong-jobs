import logging
import os
from datetime import date

from dotenv import load_dotenv
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

from model.spark import SparkFactory

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ANALYSIS_BUCKET = os.getenv("MINIO_ANALYSIS_BUCKET", "market-analysis")

_MIN = {"sma20": 20, "sma50": 50, "sma200": 200, "rsi": 14, "bb": 20, "macd": 26}

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


def run() -> None:
    src   = f"s3a://{ANALYSIS_BUCKET}/ohlcv.bar"
    dst   = f"s3a://{ANALYSIS_BUCKET}/technical.indicators"
    today = date.today()
    year  = today.strftime("%Y")
    month = today.strftime("%m")
    day   = today.strftime("%d")

    log.info("TechnicalJob | src=%s | computing indicators...", src)

    with SparkFactory("TechnicalJob") as spark:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        df = spark.read.parquet(src)
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

    log.info("TechnicalJob done | date=%s → %s", today.isoformat(), dst)

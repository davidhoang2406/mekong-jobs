import json
import logging
import os
from datetime import date
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import functions as F

from model.spark import SparkFactory

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ANALYSIS_BUCKET = os.getenv("MINIO_ANALYSIS_BUCKET", "market-analysis")
CONFIG = Path(__file__).parent.parent.parent / "config" / "screener.json"

_FIELD_MAP = {
    "priceToEarning": "pe_ratio",
    "priceToBook":    "pb_ratio",
    "roe":            "roe",
    "eps":            "eps",
    "debtOnEquity":   "de_ratio",
    "currentRatio":   "current_ratio",
}


def _fetch_fundamentals(symbols: list[str], source: str) -> list[dict]:
    from vnstock import Finance
    records = []
    for symbol in symbols:
        try:
            df = Finance(symbol=symbol, period="year", source=source).ratio()
            if df is None or df.empty:
                log.warning("No fundamental data for %s", symbol)
                continue

            # Log actual columns to diagnose mapping mismatches.
            log.info("Finance.ratio() columns for %s: %s", symbol, df.columns.tolist())

            # Drop rows where all mapped fields are NaN, then take most recent.
            mapped_cols = [c for c in _FIELD_MAP if c in df.columns]
            if not mapped_cols:
                log.warning("No _FIELD_MAP keys found in df.columns for %s — column mismatch", symbol)
                continue
            df = df.dropna(subset=mapped_cols, how="all")
            if df.empty:
                log.warning("All rows NaN for %s after dropna", symbol)
                continue

            row = df.iloc[-1].to_dict()
            record = {"symbol": symbol}
            for src_col, dst_col in _FIELD_MAP.items():
                if src_col in row:
                    val = row[src_col]
                    record[dst_col] = float(val) if pd.notna(val) else None
            records.append(record)
        except Exception as exc:
            log.warning("Failed to fetch fundamentals for %s: %s", symbol, exc)
    return records


def run(target_date: str | None = None) -> None:
    config     = json.loads(CONFIG.read_text())
    symbols    = config["symbols"]
    thresholds = config.get("thresholds", {})
    source     = config.get("vnstock_source", "VCI")

    target = date.fromisoformat(target_date) if target_date else date.today()
    year   = target.strftime("%Y")
    week   = str(target.isocalendar().week).zfill(2)

    dst = f"s3a://{ANALYSIS_BUCKET}/screener"

    log.info("ScreenerJob | week=%s-%s | symbols=%d | %s", year, week, len(symbols), dst)

    records = _fetch_fundamentals(symbols, source)
    if not records:
        log.warning("No fundamental data fetched — skipping screener")
        return

    log.info("Fetched fundamentals for %d/%d symbols", len(records), len(symbols))

    with SparkFactory("ScreenerJob") as spark:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        df = spark.createDataFrame(records)

        for field, rule in thresholds.items():
            if field not in df.columns:
                log.warning("Threshold field '%s' not in data — skipping", field)
                continue
            op  = rule.get("op", "<=")
            val = rule.get("value")
            if val is None:
                continue
            col = F.col(field).cast("double")
            if   op == "<=": df = df.filter(col <= val)
            elif op == ">=": df = df.filter(col >= val)
            elif op == "<":  df = df.filter(col <  val)
            elif op == ">":  df = df.filter(col >  val)
            elif op == "==": df = df.filter(col == val)

        count = df.count()
        log.info("Screener matched %d symbols after applying %d threshold(s)",
                 count, len(thresholds))

        (df.withColumn("year", F.lit(year))
           .withColumn("week", F.lit(week))
           .write.mode("overwrite")
           .partitionBy("year", "week")
           .parquet(dst))

    log.info("ScreenerJob done | week=%s-%s → %s", year, week, dst)

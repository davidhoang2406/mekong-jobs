import json
import logging
import os
from datetime import date
from pathlib import Path

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

_OPS = {"<=": "__le__", ">=": "__ge__", "<": "__lt__", ">": "__gt__", "==": "__eq__"}


def _fetch_fundamentals(symbols: list[str], source: str) -> list[dict]:
    from vnstock import Finance
    records = []
    for symbol in symbols:
        try:
            df = Finance(symbol=symbol, period="yearly", source=source).ratio(dropna=True)
            if df is None or df.empty:
                log.warning("No fundamental data for %s", symbol)
                continue
            row = df.iloc[-1].to_dict()
            record = {"symbol": symbol}
            for src_col, dst_col in _FIELD_MAP.items():
                if src_col in row:
                    record[dst_col] = float(row[src_col]) if row[src_col] is not None else None
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
            op, val = rule.get("op", "<="), rule.get("value")
            if val is None:
                continue
            col = F.col(field).cast("double")
            if   op == "<=": df = df.filter(col <= val)
            elif op == ">=": df = df.filter(col >= val)
            elif op == "<":  df = df.filter(col < val)
            elif op == ">":  df = df.filter(col > val)
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

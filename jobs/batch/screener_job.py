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

# Maps item_en row labels (from pivoted Finance.ratio() DataFrame) to destination columns.
# ROE is stored as a percentage value (e.g. 22.5 = 22.5%) — thresholds must match.
_FIELD_MAP = {
    "P/E":           "pe_ratio",
    "P/B":           "pb_ratio",
    "ROE (%)":       "roe",
    "Debt/Equity":   "de_ratio",
    "Current Ratio": "current_ratio",
}


def _latest_year_col_idx(df: pd.DataFrame) -> int:
    """Return the positional index of the most recent year column.

    Finance.ratio() returns a pivoted DataFrame:
      col 0: item   col 1: item_en   col 2: item_id   col 3..: year values
    Year column names may be duplicate Period strings — use positional access.
    The 'Ratio Year Id' row contains the actual year numbers for each column.
    """
    year_id_row = df[df.iloc[:, 1] == "Ratio Year Id"]
    if not year_id_row.empty:
        year_vals = year_id_row.iloc[0, 3:].values

        def _to_year(val) -> int:
            try:
                return int(float(str(val))) if pd.notna(val) else 0
            except (ValueError, TypeError):
                return 0

        year_ints = [_to_year(v) for v in year_vals]
        if any(y > 0 for y in year_ints):
            max_offset = max(range(len(year_ints)), key=lambda i: year_ints[i])
            return 3 + max_offset
    return len(df.columns) - 1


def _fetch_fundamentals(symbols: list[str], source: str) -> list[dict]:
    from vnstock import Finance
    records = []
    for symbol in symbols:
        try:
            df = Finance(symbol=symbol, period="year", source=source).ratio()
            if df is None or df.empty:
                log.warning("No fundamental data for %s", symbol)
                continue

            if len(df.columns) <= 3:
                log.warning("Unexpected DataFrame shape for %s: %s", symbol, df.shape)
                continue

            latest_idx = _latest_year_col_idx(df)

            # Build lookup: item_en label → latest year value (positional to avoid dup col names)
            lookup: dict[str, float | None] = {}
            for _, row in df.iterrows():
                key = row.iloc[1]  # item_en column
                if pd.notna(key) and key and key != 0:
                    val = row.iloc[latest_idx]
                    lookup[str(key)] = float(val) if pd.notna(val) else None

            record = {"symbol": symbol}
            for src_key, dst_col in _FIELD_MAP.items():
                record[dst_col] = lookup.get(src_key)

            records.append(record)
            log.info("Fetched %s: %s", symbol, {k: v for k, v in record.items() if k != "symbol"})
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

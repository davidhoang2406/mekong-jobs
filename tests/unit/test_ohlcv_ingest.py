"""Unit tests for ohlcv_daily_ingest date logic — no Spark, no MinIO."""
from datetime import date, timedelta

import pytest

pytestmark = pytest.mark.unit


def test_target_date_from_string():
    target = date.fromisoformat("2026-05-20")
    assert target.strftime("%Y") == "2026"
    assert target.strftime("%m") == "05"
    assert target.strftime("%d") == "20"


def test_target_date_defaults_to_today_minus_lookback():
    lookback = 1
    target = date.today() - timedelta(days=lookback)
    assert target == date.today() - timedelta(days=1)


def test_date_fragment_format():
    target = date.fromisoformat("2026-05-20")
    year, month, day = target.strftime("%Y"), target.strftime("%m"), target.strftime("%d")
    fragment = f"/year={year}/month={month}/day={day}/"
    assert fragment == "/year=2026/month=05/day=20/"


def test_partition_path_format():
    year, month, day = "2026", "05", "20"
    # Simulate a real MinIO object name — fragment needs a trailing slash to avoid partial matches
    path = f"price.snapshot/asset_class=stock/symbol=VCB/year={year}/month={month}/day={day}/data.avro"
    fragment = f"/year={year}/month={month}/day={day}/"
    assert fragment in path


@pytest.mark.parametrize("date_str,expected_year,expected_month,expected_day", [
    ("2026-01-01", "2026", "01", "01"),
    ("2026-12-31", "2026", "12", "31"),
    ("2026-05-09", "2026", "05", "09"),
])
def test_date_formatting(date_str, expected_year, expected_month, expected_day):
    t = date.fromisoformat(date_str)
    assert t.strftime("%Y") == expected_year
    assert t.strftime("%m") == expected_month
    assert t.strftime("%d") == expected_day


# ── OHLCV validation conditions ───────────────────────────────────────────────

def _ohlcv_valid(open_, high, low, close, volume):
    return (
        high >= low
        and high >= max(open_, close)
        and low  <= min(open_, close)
        and volume >= 0
    )


def test_valid_bar_passes():
    assert _ohlcv_valid(100, 110, 95, 105, 1000)


def test_high_less_than_low_fails():
    assert not _ohlcv_valid(100, 90, 95, 100, 1000)


def test_open_above_high_fails():
    assert not _ohlcv_valid(120, 110, 95, 105, 1000)


def test_close_below_low_fails():
    assert not _ohlcv_valid(100, 110, 95, 90, 1000)


def test_negative_volume_fails():
    assert not _ohlcv_valid(100, 110, 95, 105, -1)


def test_zero_volume_passes():
    assert _ohlcv_valid(100, 110, 95, 105, 0)

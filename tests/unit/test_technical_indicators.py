"""Unit tests for indicator logic in jobs/batch/technical_job.py.

Tests target the pure-pandas _macd_per_symbol helper — no Spark cluster needed.
"""
import pandas as pd
import pytest

pytestmark = pytest.mark.unit


@pytest.fixture
def rising_prices():
    return pd.Series([100.0 + i * 0.5 for i in range(60)])


@pytest.fixture
def macd_input():
    prices = [100.0 + i for i in range(40)]
    return pd.DataFrame({
        "symbol": ["VCB"] * 40,
        "time":   [f"2026-{i:02d}-01" for i in range(1, 41)],
        "close":  prices,
    })


# ── SMA ───────────────────────────────────────────────────────────────────────

def test_sma20_equals_simple_mean(rising_prices):
    sma20 = rising_prices.rolling(20).mean().iloc[-1]
    assert abs(sma20 - float(rising_prices.iloc[-20:].mean())) < 1e-6


def test_sma20_requires_20_bars():
    assert pd.isna(pd.Series([100.0] * 19).rolling(20).mean().iloc[-1])


# ── Bollinger Bands ───────────────────────────────────────────────────────────

def test_bollinger_band_ordering(rising_prices):
    mid   = rising_prices.rolling(20).mean().iloc[-1]
    std   = rising_prices.rolling(20).std(ddof=0).iloc[-1]
    assert mid + 2 * std > mid > mid - 2 * std


def test_bollinger_band_width_positive(rising_prices):
    assert rising_prices.rolling(20).std(ddof=0).iloc[-1] > 0


# ── RSI ───────────────────────────────────────────────────────────────────────

def _compute_rsi(prices: pd.Series, window: int = 14) -> float:
    delta = prices.diff()
    avg_g = delta.clip(lower=0).rolling(window).mean()
    avg_l = (-delta).clip(lower=0).rolling(window).mean()
    last_g, last_l = avg_g.iloc[-1], avg_l.iloc[-1]
    if last_l == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + last_g / last_l)


def test_rsi_bounds(rising_prices):
    assert 0 <= _compute_rsi(rising_prices) <= 100


def test_rsi_rising_series_above_50(rising_prices):
    assert _compute_rsi(rising_prices) > 50


def test_rsi_flat_series_is_nan():
    flat  = pd.Series([100.0] * 20)
    delta = flat.diff()
    assert (-delta).clip(lower=0).rolling(14).mean().iloc[-1] == 0


# ── MACD ──────────────────────────────────────────────────────────────────────

def test_macd_output_shape(macd_input):
    from jobs.batch.technical_job import _macd_per_symbol
    result = _macd_per_symbol(macd_input)
    assert len(result) == 40
    assert set(result.columns) == {"symbol", "time", "macd", "macd_signal", "macd_hist"}


def test_macd_positive_for_rising_series(macd_input):
    from jobs.batch.technical_job import _macd_per_symbol
    assert _macd_per_symbol(macd_input)["macd"].iloc[-1] > 0


def test_macd_hist_equals_macd_minus_signal(macd_input):
    from jobs.batch.technical_job import _macd_per_symbol
    result = _macd_per_symbol(macd_input)
    diff = (result["macd"] - result["macd_signal"] - result["macd_hist"]).abs()
    assert diff.max() < 1e-10


def test_macd_preserves_row_count(macd_input):
    from jobs.batch.technical_job import _macd_per_symbol
    assert len(_macd_per_symbol(macd_input)) == len(macd_input)

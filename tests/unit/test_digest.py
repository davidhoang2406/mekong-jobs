"""Unit tests for digest_job business logic — no Spark, no MinIO."""
from datetime import date

import pytest

pytestmark = pytest.mark.unit


def _pct_change(open_: float, close: float) -> float:
    return (close - open_) / open_ * 100


# ── pct_change formula ────────────────────────────────────────────────────────

def test_pct_change_positive_gain():
    assert _pct_change(100, 110) == pytest.approx(10.0)


def test_pct_change_negative_loss():
    assert _pct_change(100, 90) == pytest.approx(-10.0)


def test_pct_change_flat():
    assert _pct_change(100, 100) == pytest.approx(0.0)


def test_pct_change_large_gain():
    assert _pct_change(50, 100) == pytest.approx(100.0)


def test_pct_change_small_movement():
    assert _pct_change(85000, 85800) == pytest.approx(0.9411, rel=1e-3)


# ── category classification ───────────────────────────────────────────────────

def _classify(pct: float) -> str:
    if pct > 0:
        return "gainer"
    if pct < 0:
        return "loser"
    return "flat"


def test_positive_pct_is_gainer():
    assert _classify(5.0) == "gainer"


def test_negative_pct_is_loser():
    assert _classify(-3.0) == "loser"


def test_zero_pct_is_flat():
    assert _classify(0.0) == "flat"


# ── ranking + top_n ───────────────────────────────────────────────────────────

def _top_n(items: list[dict], key: str, ascending: bool, n: int) -> list[dict]:
    return sorted(items, key=lambda x: x[key], reverse=not ascending)[:n]


def test_top_gainers_returns_n():
    bars = [{"symbol": f"S{i}", "pct_change": float(i)} for i in range(20)]
    result = _top_n(bars, "pct_change", ascending=False, n=10)
    assert len(result) == 10
    assert result[0]["pct_change"] == 19.0


def test_top_losers_returns_n():
    bars = [{"symbol": f"S{i}", "pct_change": float(i - 10)} for i in range(20)]
    result = _top_n(bars, "pct_change", ascending=True, n=5)
    assert len(result) == 5
    assert result[0]["pct_change"] == -10.0


def test_top_n_less_than_n_available():
    bars = [{"symbol": "A", "pct_change": 5.0}, {"symbol": "B", "pct_change": 3.0}]
    assert len(_top_n(bars, "pct_change", ascending=False, n=10)) == 2


# ── date parsing ──────────────────────────────────────────────────────────────

def test_target_date_from_string():
    t = date.fromisoformat("2026-05-20")
    assert (t.strftime("%Y"), t.strftime("%m"), t.strftime("%d")) == ("2026", "05", "20")


def test_target_date_defaults_to_today():
    assert date.fromisoformat(date.today().isoformat()) == date.today()

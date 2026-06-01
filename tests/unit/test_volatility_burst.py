"""Unit tests for volatility_burst_job pure functions — no Flink, no Kafka."""
import math
import pytest

pytestmark = pytest.mark.unit


# ── _std_dev ──────────────────────────────────────────────────────────────────

class TestStdDev:
    def test_empty_list_returns_zero(self):
        from jobs.stream.volatility_burst_job import _std_dev
        assert _std_dev([]) == 0.0

    def test_single_value_returns_zero(self):
        from jobs.stream.volatility_burst_job import _std_dev
        assert _std_dev([42.0]) == 0.0

    def test_constant_series_returns_zero(self):
        from jobs.stream.volatility_burst_job import _std_dev
        assert _std_dev([100.0, 100.0, 100.0, 100.0]) == 0.0

    def test_known_values(self):
        from jobs.stream.volatility_burst_job import _std_dev
        # population std dev of [2, 4, 4, 4, 5, 5, 7, 9] = 2.0
        assert _std_dev([2, 4, 4, 4, 5, 5, 7, 9]) == pytest.approx(2.0)

    def test_two_values(self):
        from jobs.stream.volatility_burst_job import _std_dev
        # population std dev of [0, 10] = 5.0
        assert _std_dev([0.0, 10.0]) == pytest.approx(5.0)

    def test_returns_positive(self):
        from jobs.stream.volatility_burst_job import _std_dev
        assert _std_dev([1.0, 2.0, 3.0, 4.0, 5.0]) > 0

    def test_symmetric_around_mean(self):
        from jobs.stream.volatility_burst_job import _std_dev
        assert _std_dev([90.0, 110.0]) == _std_dev([0.0, 20.0])


# ── _fmt_alert ────────────────────────────────────────────────────────────────

class TestFmtAlert:
    def _sample(self, **overrides):
        base = {
            "symbol":      "BTC-USDT",
            "std_dev":     1250.5,
            "price_count": 30,
            "mean_price":  68500.0,
        }
        return {**base, **overrides}

    def test_contains_symbol(self):
        from jobs.stream.volatility_burst_job import _fmt_alert
        assert "BTC-USDT" in _fmt_alert(self._sample())

    def test_contains_std_dev(self):
        from jobs.stream.volatility_burst_job import _fmt_alert
        result = _fmt_alert(self._sample(std_dev=1250.5))
        assert "1250.5" in result

    def test_contains_price_count(self):
        from jobs.stream.volatility_burst_job import _fmt_alert
        assert "30" in _fmt_alert(self._sample())

    def test_contains_mean_price(self):
        from jobs.stream.volatility_burst_job import _fmt_alert
        assert "68500" in _fmt_alert(self._sample())

    def test_returns_string(self):
        from jobs.stream.volatility_burst_job import _fmt_alert
        assert isinstance(_fmt_alert(self._sample()), str)

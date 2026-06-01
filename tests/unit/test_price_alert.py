"""Unit tests for price alert rule evaluation — no Flink, no Kafka."""
import pytest

pytestmark = pytest.mark.unit


SAMPLE_RULES = [
    {"symbol": "VCB",  "field": "price",      "operator": ">=", "threshold": 90000, "source": "*"},
    {"symbol": "VCB",  "field": "pct_change",  "operator": ">=", "threshold": 5.0,  "source": "*"},
    {"symbol": "*",    "field": "price",       "operator": ">=", "threshold": 70000, "source": "stock"},
    {"symbol": "BTC-USDT", "field": "price",   "operator": ">=", "threshold": 60000, "source": "crypto"},
]


# ── validate_rules ────────────────────────────────────────────────────────────

class TestValidateRules:
    def test_keeps_valid_operators(self):
        from jobs.utils import validate_rules
        rules = [{"symbol": "*", "field": "price", "operator": op, "threshold": 100, "source": "*"}
                 for op in ["<", ">", "<=", ">=", "=="]]
        assert len(validate_rules(rules)) == 5

    def test_drops_unknown_operator(self):
        from jobs.utils import validate_rules
        rules = [
            {"symbol": "*", "field": "price", "operator": "!=", "threshold": 100, "source": "*"},
            {"symbol": "*", "field": "price", "operator": ">=", "threshold": 100, "source": "*"},
        ]
        valid = validate_rules(rules)
        assert len(valid) == 1
        assert valid[0]["operator"] == ">="

    def test_empty_rules_returns_empty(self):
        from jobs.utils import validate_rules
        assert validate_rules([]) == []


# ── evaluate_rules ────────────────────────────────────────────────────────────

class TestEvaluateRules:
    def test_fires_when_price_above_threshold(self):
        from jobs.utils import evaluate_rules
        rules = [{"symbol": "VCB", "field": "price", "operator": ">=", "threshold": 90000, "source": "*"}]
        triggered = evaluate_rules(rules, "VCB", {"price": 95000}, source="vnstock")
        assert len(triggered) == 1
        assert triggered[0]["matched_value"] == 95000

    def test_no_fire_when_below_threshold(self):
        from jobs.utils import evaluate_rules
        rules = [{"symbol": "VCB", "field": "price", "operator": ">=", "threshold": 90000, "source": "*"}]
        assert evaluate_rules(rules, "VCB", {"price": 80000}, source="vnstock") == []

    def test_symbol_filter_skips_non_matching(self):
        from jobs.utils import evaluate_rules
        rules = [{"symbol": "VCB", "field": "price", "operator": ">=", "threshold": 1, "source": "*"}]
        assert evaluate_rules(rules, "ACB", {"price": 99999}, source="vnstock") == []

    def test_wildcard_symbol_matches_all(self):
        from jobs.utils import evaluate_rules
        rules = [{"symbol": "*", "field": "price", "operator": ">=", "threshold": 1, "source": "*"}]
        assert len(evaluate_rules(rules, "ANYTHING", {"price": 100}, source="vnstock")) == 1

    def test_source_stock_filter(self):
        from jobs.utils import evaluate_rules
        rules = [{"symbol": "*", "field": "price", "operator": ">=", "threshold": 1, "source": "stock"}]
        # vnstock → stock
        assert len(evaluate_rules(rules, "VCB", {"price": 100}, source="vnstock.VCI")) == 1
        # ccxt → crypto, should not fire for "stock" rule
        assert evaluate_rules(rules, "BTC", {"price": 100}, source="ccxt.binance") == []

    def test_source_crypto_filter(self):
        from jobs.utils import evaluate_rules
        rules = [{"symbol": "*", "field": "price", "operator": ">=", "threshold": 1, "source": "crypto"}]
        assert len(evaluate_rules(rules, "BTC", {"price": 100}, source="ccxt.binance")) == 1
        assert evaluate_rules(rules, "VCB", {"price": 100}, source="vnstock.VCI") == []

    def test_missing_field_skips_rule(self):
        from jobs.utils import evaluate_rules
        rules = [{"symbol": "*", "field": "volume", "operator": ">=", "threshold": 1000, "source": "*"}]
        assert evaluate_rules(rules, "VCB", {"price": 100}, source="vnstock") == []

    def test_multiple_rules_fire(self):
        from jobs.utils import evaluate_rules
        rules = [
            {"symbol": "VCB", "field": "price",     "operator": ">=", "threshold": 90000, "source": "*"},
            {"symbol": "VCB", "field": "pct_change", "operator": ">=", "threshold": 5.0,  "source": "*"},
        ]
        triggered = evaluate_rules(rules, "VCB", {"price": 95000, "pct_change": 6.0}, source="vnstock")
        assert len(triggered) == 2

    def test_triggered_result_has_matched_fields(self):
        from jobs.utils import evaluate_rules
        rules = [{"symbol": "VCB", "field": "price", "operator": ">=", "threshold": 1, "source": "*"}]
        result = evaluate_rules(rules, "VCB", {"price": 100}, source="vnstock")
        assert result[0]["matched_field"] == "price"
        assert result[0]["matched_value"] == 100

    @pytest.mark.parametrize("op,val,threshold,expect_fire", [
        ("<",  50, 100, True),
        ("<",  100, 100, False),
        (">",  150, 100, True),
        (">",  100, 100, False),
        ("<=", 100, 100, True),
        (">=", 100, 100, True),
        ("==", 100, 100, True),
        ("==", 99,  100, False),
    ])
    def test_operator_semantics(self, op, val, threshold, expect_fire):
        from jobs.utils import evaluate_rules
        rules = [{"symbol": "*", "field": "price", "operator": op, "threshold": threshold, "source": "*"}]
        triggered = evaluate_rules(rules, "X", {"price": val}, source="vnstock")
        assert bool(triggered) == expect_fire

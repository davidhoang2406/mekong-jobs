import json
import logging
import operator
from pathlib import Path

log = logging.getLogger(__name__)

ALERT_OPS: dict = {
    "<":  operator.lt,
    ">":  operator.gt,
    "<=": operator.le,
    ">=": operator.ge,
    "==": operator.eq,
}


def load_json_config(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def validate_rules(rules: list[dict]) -> list[dict]:
    """Drop rules with unknown operators at load time so evaluate_rules never silently no-ops."""
    valid = []
    for rule in rules:
        op = rule.get("operator")
        if op not in ALERT_OPS:
            log.warning("Skipping alert rule with unknown operator %r: %s", op, rule)
            continue
        valid.append(rule)
    return valid


def evaluate_rules(rules: list[dict], symbol: str, payload: dict, source: str = "") -> list[dict]:
    """Return every rule that fires for this tick.

    Each returned dict is the original rule entry plus two extra keys:
      matched_field  — the payload field that was tested
      matched_value  — the value that crossed the threshold
    """
    asset_cls = "stock" if source.startswith("vnstock") else "crypto" if source.startswith("ccxt") else "unknown"
    triggered = []
    for rule in rules:
        rule_source = rule.get("source", "*")
        if rule_source != "*" and rule_source != asset_cls:
            continue
        if rule["symbol"] != "*" and rule["symbol"] != symbol:
            continue
        field = rule["field"]
        value = payload.get(field)
        if value is None:
            continue
        op_fn = ALERT_OPS.get(rule["operator"])
        if op_fn and op_fn(value, rule["threshold"]):
            triggered.append({**rule, "matched_field": field, "matched_value": value})
    return triggered

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_KEY_PATTERN = re.compile(r"^[a-z0-9_]+$")


@dataclass(frozen=True, slots=True)
class MetricDefinition:
    key: str
    name: str
    description: str


DEFAULT_METRICS: tuple[MetricDefinition, ...] = (
    MetricDefinition("revenue", "Выручка", "Revenue from IFRS statement of profit or loss."),
    MetricDefinition(
        "interest_expense_loans",
        "Финансовые расходы (проценты по кредитам)",
        "Priority: bank loan interest expense; else total interest expense; else total finance costs.",
    ),
    MetricDefinition(
        "depreciation",
        "Амортизация",
        "Depreciation and amortization expense for the period.",
    ),
    MetricDefinition(
        "cash_and_cash_equivalents",
        "Денежные средства и эквиваленты",
        "Cash and cash equivalents from statement of financial position.",
    ),
    MetricDefinition(
        "property_plant_and_equipment",
        "Основные средства",
        "Property, plant and equipment (PPE) carrying amount.",
    ),
    MetricDefinition(
        "operating_profit",
        "Операционная прибыль",
        "Operating profit or operating income.",
    ),
    MetricDefinition(
        "long_term_debt_and_lease",
        "Долгосрочные обязательства (кредиты+лизинг)",
        "Sum of non-current borrowings and non-current lease liabilities; if lease is not disclosed, use non-current 'other' liabilities.",
    ),
    MetricDefinition(
        "short_term_debt_and_lease",
        "Краткосрочные обязательства (кредиты+лизинг)",
        "Sum of current borrowings and current lease liabilities; if lease is not disclosed, use current 'other' liabilities.",
    ),
)


def load_metrics(path: str | Path | None = None) -> list[MetricDefinition]:
    if path is None:
        return list(DEFAULT_METRICS)

    metrics_path = Path(path)
    payload = json.loads(metrics_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Metrics config must be a JSON list.")

    metrics: list[MetricDefinition] = []
    for item in payload:
        metric = _parse_metric(item)
        metrics.append(metric)

    _validate_metrics(metrics)
    return metrics


def _parse_metric(item: Any) -> MetricDefinition:
    if not isinstance(item, dict):
        raise ValueError("Each metric entry must be a JSON object.")

    key = _as_non_empty_string(item.get("key"), "Metric key")
    name = _as_non_empty_string(item.get("name"), f"Metric '{key}' name")
    description = _as_non_empty_string(item.get("description"), f"Metric '{key}' description")

    return MetricDefinition(key=key, name=name, description=description)


def _validate_metrics(metrics: list[MetricDefinition]) -> None:
    if not metrics:
        raise ValueError("Metrics list must not be empty.")

    seen: set[str] = set()
    for metric in metrics:
        if not _KEY_PATTERN.match(metric.key):
            raise ValueError(
                f"Invalid metric key '{metric.key}'. Use lowercase letters, numbers, and underscore only."
            )
        if metric.key in seen:
            raise ValueError(f"Duplicate metric key '{metric.key}' in metrics config.")
        seen.add(metric.key)


def _as_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string.")
    return value.strip()

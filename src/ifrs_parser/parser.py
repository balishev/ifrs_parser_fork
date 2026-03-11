from __future__ import annotations

import json
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Sequence

from .metrics import MetricDefinition

DEFAULT_MODEL = "gemini-2.5-flash"
DEFAULT_LOCATION = "us-central1"
MAX_INLINE_PDF_BYTES = 19 * 1024 * 1024
DEPRECIATION_KEY = "depreciation"
PPE_KEY = "property_plant_and_equipment"
REVENUE_KEY = "revenue"
INTEREST_EXPENSE_KEY = "interest_expense_loans"
OPERATING_PROFIT_KEY = "operating_profit"
LONG_TERM_DEBT_KEY = "long_term_debt_and_lease"
SHORT_TERM_DEBT_KEY = "short_term_debt_and_lease"
CASH_KEY = "cash_and_cash_equivalents"

CALC_EBITDA_KEY = "ebitda"
CALC_EBITDA_MARGIN_KEY = "ebitda_margin_pct"
CALC_TOTAL_DEBT_KEY = "total_debt"
CALC_NET_DEBT_KEY = "net_debt"
CALC_EBITDA_TO_INTEREST_KEY = "ebitda_to_interest_expense"
CALC_NET_DEBT_TO_EBITDA_LTM_KEY = "net_debt_to_ebitda_ltm"
TARGET_SCALE_BN = 1_000_000_000.0
TARGET_RUB_BN_UNIT = "RUB bn"
_ISO_DATE_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
_DOT_DATE_RE = re.compile(r"\b(\d{1,2})[./](\d{1,2})[./](\d{4})\b")
_TEXT_DATE_RE = re.compile(r"\b(\d{1,2})\s+([A-Za-zА-Яа-я]+)\s+(\d{4})\b")
_QUARTER_RE = re.compile(r"\bQ([1-4])\s*(\d{4})\b", re.IGNORECASE)
_YEAR_RE = re.compile(r"\b(20\d{2})\b")
_MONTH_MAP = {
    "january": 1,
    "jan": 1,
    "february": 2,
    "feb": 2,
    "march": 3,
    "mar": 3,
    "april": 4,
    "apr": 4,
    "may": 5,
    "june": 6,
    "jun": 6,
    "july": 7,
    "jul": 7,
    "august": 8,
    "aug": 8,
    "september": 9,
    "sep": 9,
    "october": 10,
    "oct": 10,
    "november": 11,
    "nov": 11,
    "december": 12,
    "dec": 12,
    "января": 1,
    "январь": 1,
    "февраля": 2,
    "февраль": 2,
    "марта": 3,
    "март": 3,
    "апреля": 4,
    "апрель": 4,
    "мая": 5,
    "май": 5,
    "июня": 6,
    "июнь": 6,
    "июля": 7,
    "июль": 7,
    "августа": 8,
    "август": 8,
    "сентября": 9,
    "сентябрь": 9,
    "октября": 10,
    "октябрь": 10,
    "ноября": 11,
    "ноябрь": 11,
    "декабря": 12,
    "декабрь": 12,
}


@dataclass(slots=True)
class IFRSParserConfig:
    model: str = DEFAULT_MODEL
    location: str = DEFAULT_LOCATION
    timeout_sec: int = 300
    poll_interval_sec: float = 2.0
    keep_uploaded_file: bool = False
    max_retries_on_resource_exhausted: int = 5
    max_retries_on_transient_error: int = 4
    retry_base_delay_sec: float = 2.0
    retry_max_delay_sec: float = 30.0


class GoogleIFRSPdfParser:
    def __init__(
        self,
        api_key: str | None = None,
        credentials_json: str | Path | None = None,
        project: str | None = None,
        config: IFRSParserConfig | None = None,
    ) -> None:
        try:
            from google import genai
            from google.genai import types
        except ImportError as exc:
            raise RuntimeError(
                "google-genai is not installed. Run: pip install google-genai"
            ) from exc

        self._config = config or IFRSParserConfig()
        self._types = types

        if credentials_json:
            credentials_path = Path(credentials_json)
            if not credentials_path.exists():
                raise FileNotFoundError(f"Credentials JSON not found: {credentials_path}")
            try:
                from google.oauth2 import service_account
            except ImportError as exc:
                raise RuntimeError(
                    "google-auth is not installed. Run: pip install google-auth"
                ) from exc

            credentials = service_account.Credentials.from_service_account_file(
                str(credentials_path),
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            resolved_project = project or credentials.project_id
            if not resolved_project:
                raise ValueError(
                    "Project ID is missing. Provide --project or include project_id in credentials JSON."
                )
            self._client = genai.Client(
                vertexai=True,
                project=resolved_project,
                location=self._config.location,
                credentials=credentials,
            )
            self._use_files_api = False
            return

        resolved_key = api_key or os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
        if not resolved_key:
            raise ValueError(
                "Google API key is missing. Set GOOGLE_API_KEY (or GEMINI_API_KEY), "
                "or pass credentials_json for Vertex AI."
            )

        self._client = genai.Client(api_key=resolved_key)
        self._use_files_api = True

    def extract_metrics(
        self,
        pdf_path: str | Path,
        metrics: Sequence[MetricDefinition],
        period_hint: str | None = None,
    ) -> dict[str, Any]:
        source_path = Path(pdf_path)
        if not source_path.exists():
            raise FileNotFoundError(f"PDF file not found: {source_path}")
        if source_path.suffix.lower() != ".pdf":
            raise ValueError(f"Expected a .pdf file, got: {source_path.name}")
        if not metrics:
            raise ValueError("Metrics list must not be empty.")

        prompt = _build_prompt(metrics, period_hint)
        if self._use_files_api:
            uploaded_file = self._upload_pdf_with_retry(source_path)
            try:
                ready_file = self._wait_for_file(uploaded_file)
                response = self._generate_response(prompt=prompt, document=ready_file, metrics=metrics)
            finally:
                if not self._config.keep_uploaded_file:
                    self._try_delete_uploaded_file(uploaded_file)
        else:
            document_part = self._build_inline_pdf_part(source_path)
            response = self._generate_response(prompt=prompt, document=document_part, metrics=metrics)

        raw_payload = _parse_json_payload(_extract_response_text(response))
        return _normalize_result(
            payload=raw_payload,
            source_document=source_path.name,
            model=self._config.model,
            metrics=metrics,
        )

    def extract_bank_debt_notes_from_images_text(
        self,
        images_text: str,
        rep_year: int | str | None = None,
    ) -> dict[str, Any]:
        text = images_text.strip() if isinstance(images_text, str) else ""
        if not text:
            raise ValueError("images_text must be a non-empty string.")
        year_text = _normalize_optional_rep_year(rep_year)

        prompt = _build_bank_debt_prompt(images_text=text, rep_year=year_text)
        response = self._generate_response_with_schema(
            contents=[prompt],
            response_schema=_build_bank_debt_response_schema(),
        )
        payload = _parse_json_payload(_extract_response_text(response))
        return _normalize_bank_debt_result(
            payload=payload,
            model=self._config.model,
            rep_year=year_text,
            source_document="images_text_input",
        )

    def extract_bank_debt_notes_from_pdf(
        self,
        pdf_path: str | Path,
        rep_year: int | str | None = None,
        period_hint: str | None = None,
    ) -> dict[str, Any]:
        source_path = Path(pdf_path)
        if not source_path.exists():
            raise FileNotFoundError(f"PDF file not found: {source_path}")
        if source_path.suffix.lower() != ".pdf":
            raise ValueError(f"Expected a .pdf file, got: {source_path.name}")

        year_text = _normalize_optional_rep_year(rep_year)

        prompt = _build_bank_debt_pdf_prompt(rep_year=year_text, period_hint=period_hint)
        if self._use_files_api:
            uploaded_file = self._upload_pdf_with_retry(source_path)
            try:
                ready_file = self._wait_for_file(uploaded_file)
                response = self._generate_response_with_schema(
                    contents=[prompt, ready_file],
                    response_schema=_build_bank_debt_response_schema(),
                )
            finally:
                if not self._config.keep_uploaded_file:
                    self._try_delete_uploaded_file(uploaded_file)
        else:
            document_part = self._build_inline_pdf_part(source_path)
            response = self._generate_response_with_schema(
                contents=[prompt, document_part],
                response_schema=_build_bank_debt_response_schema(),
            )

        payload = _parse_json_payload(_extract_response_text(response))
        return _normalize_bank_debt_result(
            payload=payload,
            model=self._config.model,
            rep_year=year_text,
            source_document=source_path.name,
        )

    def _generate_response(
        self,
        prompt: str,
        document: Any,
        metrics: Sequence[MetricDefinition],
    ) -> Any:
        schema = _build_response_schema([metric.key for metric in metrics])
        return self._generate_response_with_schema(
            contents=[prompt, document],
            response_schema=schema,
        )

    def _generate_response_with_schema(
        self,
        contents: list[Any],
        response_schema: dict[str, Any],
    ) -> Any:
        attempt = 0
        while True:
            try:
                return self._client.models.generate_content(
                    model=self._config.model,
                    contents=contents,
                    config={
                        "temperature": 0,
                        "response_mime_type": "application/json",
                        "response_json_schema": response_schema,
                    },
                )
            except Exception as exc:
                retry_resource_exhausted = _is_resource_exhausted_error(exc)
                retry_transient = _is_transient_network_error(exc)
                if not retry_resource_exhausted and not retry_transient:
                    raise
                retry_limit = (
                    self._config.max_retries_on_resource_exhausted
                    if retry_resource_exhausted
                    else self._config.max_retries_on_transient_error
                )
                if attempt >= retry_limit:
                    raise
                delay = _retry_delay_seconds(
                    attempt=attempt,
                    base=self._config.retry_base_delay_sec,
                    max_delay=self._config.retry_max_delay_sec,
                )
                time.sleep(delay)
                attempt += 1

    def _wait_for_file(self, file_ref: Any) -> Any:
        file_name = getattr(file_ref, "name", None)
        if not file_name:
            return file_ref

        deadline = time.monotonic() + self._config.timeout_sec
        current = file_ref
        while True:
            state = _file_state_name(current)
            if state in {"ACTIVE", "READY", "SUCCEEDED", "SUCCESS", "UNSPECIFIED"}:
                return current
            if state in {"FAILED", "ERROR"}:
                raise RuntimeError(f"Uploaded file failed processing in Google API. State: {state}")
            if time.monotonic() > deadline:
                raise TimeoutError("Timed out while waiting for PDF processing in Google API.")
            time.sleep(self._config.poll_interval_sec)
            current = self._client.files.get(name=file_name)

    def _try_delete_uploaded_file(self, file_ref: Any) -> None:
        file_name = getattr(file_ref, "name", None)
        if not file_name:
            return
        try:
            self._client.files.delete(name=file_name)
        except Exception:
            return

    def _build_inline_pdf_part(self, source_path: Path) -> Any:
        pdf_bytes = source_path.read_bytes()
        if len(pdf_bytes) > MAX_INLINE_PDF_BYTES:
            raise ValueError(
                f"PDF is too large for inline upload in Vertex mode: {len(pdf_bytes)} bytes. "
                f"Limit is {MAX_INLINE_PDF_BYTES} bytes."
            )
        return self._types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf")

    def _upload_pdf_with_retry(self, source_path: Path) -> Any:
        attempt = 0
        while True:
            try:
                return self._client.files.upload(
                    file=str(source_path),
                    config={"mime_type": "application/pdf"},
                )
            except Exception as exc:
                retry_resource_exhausted = _is_resource_exhausted_error(exc)
                retry_transient = _is_transient_network_error(exc)
                if not retry_resource_exhausted and not retry_transient:
                    raise
                retry_limit = (
                    self._config.max_retries_on_resource_exhausted
                    if retry_resource_exhausted
                    else self._config.max_retries_on_transient_error
                )
                if attempt >= retry_limit:
                    raise
                delay = _retry_delay_seconds(
                    attempt=attempt,
                    base=self._config.retry_base_delay_sec,
                    max_delay=self._config.retry_max_delay_sec,
                )
                time.sleep(delay)
                attempt += 1


def _build_prompt(metrics: Sequence[MetricDefinition], period_hint: str | None) -> str:
    metric_lines = [
        f"- {metric.key}: {metric.name}. {metric.description}"
        for metric in metrics
    ]
    period_text = period_hint or "latest period presented in the report"
    return "\n".join(
        [
            "You are an IFRS financial analyst.",
            "Extract the target metrics from the attached IFRS report PDF.",
            f"Focus period: {period_text}.",
            "Rules:",
            "1) Prefer consolidated IFRS statements when both consolidated and standalone data are present.",
            "2) Determine latest reporting period end date in the document and return it as reporting_period_end_date (ISO YYYY-MM-DD).",
            "3) Return exactly one item per requested metric_key in metrics (latest period) and one item per requested metric_key in comparative_metrics (previous period for the same metric). Do not add extra keys.",
            "4) For comparative_metrics choose the nearest earlier disclosed period for each metric_key.",
            "   For P&L/flow metrics use prior comparable duration (for example H1 2024 for H1 2025).",
            "   For balance-sheet metrics use prior statement-of-financial-position date (often 31 Dec prior year).",
            "5) Do not estimate values. For missing metrics set found=false and include a short note in notes.",
            "6) Put numeric amount into value and indicate scaling in scale_multiplier:",
            "   1 for units, 1000 for thousands, 1000000 for millions, 1000000000 for billions.",
            "7) page must be the PDF page where the value is visible.",
            "8) confidence must be between 0 and 1.",
            "9) For debt+lease metrics, return the sum of loans/borrowings and lease liabilities for the same horizon.",
            "   If lease liabilities are not separately disclosed for that horizon, use the corresponding 'other' liabilities for that horizon.",
            "10) For each metric in both arrays provide period_end_date in ISO YYYY-MM-DD.",
            "11) For interest_expense_loans apply strict priority:",
            "    a) 'Процентные расходы - кредиты банков' (or direct bank loan interest expense).",
            "    b) If unavailable, use total 'Процентные расходы' (interest expense).",
            "    c) If unavailable, use total 'Финансовые расходы' (finance costs).",
            "    Set selection_level as one of: bank_loan_interest, interest_expense, finance_costs.",
            "12) Also identify ultimate beneficial owner surname (UBO) only if explicitly disclosed in report notes",
            "    (examples: 'ultimate controlling party', 'конечный бенефициар').",
            "    Return surname only in ubo_surname. If unknown, return empty string.",
            "Requested metrics:",
            *metric_lines,
        ]
    )


def _build_bank_debt_prompt(images_text: str, rep_year: str | None) -> str:
    period_rule = (
        f"3. Только в найденных разделах извлекай точные показатели только за {rep_year} год,"
        if rep_year
        else "3. Определи последний отчетный период в тексте (квартал/полугодие/9 месяцев/год) "
        "и извлекай показатели только за этот период."
    )
    tail_rule = (
        f"В rows оставляй только строки за {rep_year} год."
        if rep_year
        else "В rows оставляй только строки за найденный последний отчетный период."
    )
    return "\n".join(
        [
            "Ты финансовый аналитик данных. Перед тобой тексты, извлеченные из изображений финансовых отчетов МСФО.",
            "Внимательно изучи все изображения. Извлеки весь текст и числовые данные.",
            "",
            "ИЗВЛЕЧЕННЫЕ ТЕКСТЫ:",
            images_text,
            "",
            "ИНСТРУКЦИИ ДЛЯ АНАЛИЗА:",
            "1. Проанализируй ВСЕ предоставленные тексты.",
            "2. Найди разделы из Примечаний (или Приложений), связанных с кредитами банков:",
            "   долгосрочные и краткосрочные обязательства, кредиты и займы, долговые обязательства, заемные средства, кредиты.",
            period_rule,
            "   где встречаются слова или части слов: 'банк', 'заем', 'займ', 'облигаци'.",
            "   Если показатель не относится к кредитам, не извлекай.",
            "4. Назначай приоритет строго так:",
            "   - если есть 'займ' -> 1",
            "   - если есть 'банк' -> 2",
            "   - если есть 'заем' -> 3",
            "   - если есть 'облигаци' -> 4",
            "   Если приоритет не назначен, показатель не включай.",
            "5. Если число указано в скобках, например (100), это отрицательное число.",
            "6. Название компании и единицы измерения пиши единообразно.",
            "7. Верни JSON со списком rows.",
            "Формат rows: company_name, section_name, indicator, priority, period, amount, unit.",
            "amount должен быть числом.",
            tail_rule,
        ]
    )


def _build_bank_debt_pdf_prompt(rep_year: str | None, period_hint: str | None) -> str:
    period_focus = period_hint.strip() if isinstance(period_hint, str) and period_hint.strip() else "все периоды в PDF"
    period_rule = (
        f"3. Только в найденных разделах извлекай показатели только за {rep_year} год,"
        if rep_year
        else "3. Определи последний отчетный период в PDF (квартал/полугодие/9 месяцев/год) "
        "и извлекай показатели только за этот период,"
    )
    tail_rule = (
        f"В rows оставляй только строки за {rep_year} год."
        if rep_year
        else "В rows оставляй только строки за найденный последний отчетный период."
    )
    return "\n".join(
        [
            "Ты финансовый аналитик данных. Перед тобой PDF финансовой отчетности МСФО.",
            "Внимательно изучи ВЕСЬ PDF (все страницы, включая Примечания/Приложения).",
            f"Фокус периода: {period_focus}.",
            "",
            "ИНСТРУКЦИИ ДЛЯ АНАЛИЗА:",
            "1. Проанализируй все разделы PDF.",
            "2. Найди разделы из Примечаний (или Приложений), связанные с кредитами банков:",
            "   долгосрочные/краткосрочные обязательства, кредиты и займы, долговые обязательства, заемные средства, кредиты.",
            period_rule,
            "   где встречаются слова или части слов: 'банк', 'заем', 'займ', 'облигаци'.",
            "   Если показатель не относится к кредитам, не извлекай его.",
            "4. Назначай приоритет строго так:",
            "   - если есть 'займ' -> 1",
            "   - если есть 'банк' -> 2",
            "   - если есть 'заем' -> 3",
            "   - если есть 'облигаци' -> 4",
            "5. Если приоритет не назначен, показатель не включай.",
            "6. Если число в скобках, например (100), считай его отрицательным.",
            "7. Название компании и единицы измерения пиши единообразно.",
            "8. Верни JSON со списком rows.",
            "Формат rows: company_name, section_name, indicator, priority, period, amount, unit.",
            "amount должен быть числом.",
            tail_rule,
        ]
    )


def _build_bank_debt_response_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "company_name_style": {"type": "string"},
            "unit_style": {"type": "string"},
            "reporting_period": {"type": "string"},
            "reporting_period_end_date": {"type": "string"},
            "rows": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "company_name": {"type": "string"},
                        "section_name": {"type": "string"},
                        "indicator": {"type": "string"},
                        "priority": {"type": "integer", "enum": [1, 2, 3, 4]},
                        "period": {"type": "string"},
                        "period_end_date": {"type": "string"},
                        "amount": {"type": "number"},
                        "unit": {"type": "string"},
                    },
                    "required": [
                        "company_name",
                        "section_name",
                        "indicator",
                        "priority",
                        "period",
                        "amount",
                        "unit",
                    ],
                },
            },
            "notes": {"type": "string"},
        },
        "required": ["rows"],
    }


def _metric_item_schema(metric_keys: Sequence[str]) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "metric_key": {"type": "string", "enum": list(metric_keys)},
            "metric_name": {"type": "string"},
            "found": {"type": "boolean"},
            "value": {"type": "number"},
            "unit": {"type": "string"},
            "scale_multiplier": {"type": "number"},
            "period_label": {"type": "string"},
            "period_end_date": {"type": "string"},
            "selection_level": {"type": "string"},
            "statement": {"type": "string"},
            "page": {"type": "integer"},
            "evidence": {"type": "string"},
            "confidence": {"type": "number"},
            "notes": {"type": "string"},
        },
        "required": ["metric_key", "found"],
    }


def _build_response_schema(metric_keys: Sequence[str]) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "company_name": {"type": "string"},
            "ubo_surname": {"type": "string"},
            "reporting_period": {"type": "string"},
            "reporting_period_end_date": {"type": "string"},
            "reporting_currency": {"type": "string"},
            "notes": {"type": "string"},
            "metrics": {
                "type": "array",
                "items": _metric_item_schema(metric_keys),
            },
            "comparative_metrics": {
                "type": "array",
                "items": _metric_item_schema(metric_keys),
            },
        },
        "required": ["metrics", "comparative_metrics"],
    }


def _extract_response_text(response: Any) -> str:
    text = getattr(response, "text", None)
    if isinstance(text, str) and text.strip():
        return text
    raise RuntimeError("Google API returned empty response text.")


def _parse_json_payload(raw_text: str) -> dict[str, Any]:
    stripped = raw_text.strip()
    if stripped.startswith("```"):
        stripped = stripped.removeprefix("```json").removeprefix("```").strip()
        if stripped.endswith("```"):
            stripped = stripped[:-3].strip()

    data = json.loads(stripped)
    if not isinstance(data, dict):
        raise ValueError("Expected JSON object from model response.")
    return data


def _normalize_result(
    payload: dict[str, Any],
    source_document: str,
    model: str,
    metrics: Sequence[MetricDefinition],
) -> dict[str, Any]:
    reporting_period = _as_string(payload.get("reporting_period"))
    reporting_period_end_date = _extract_iso_date(payload.get("reporting_period_end_date")) or _extract_iso_date(
        reporting_period
    )
    reporting_currency = _as_string(payload.get("reporting_currency"))
    metric_map = {metric.key: metric for metric in metrics}

    normalized = _normalize_metric_block(
        raw_metrics=payload.get("metrics"),
        metrics=metrics,
        metric_map=metric_map,
        missing_note="No value returned by model for latest period.",
    )
    if not reporting_period_end_date:
        reporting_period_end_date = _max_found_period_end_date(normalized)
    _enforce_latest_period_only(normalized, reporting_period_end_date)
    _apply_business_rules(normalized)

    comparative_normalized = _normalize_metric_block(
        raw_metrics=payload.get("comparative_metrics"),
        metrics=metrics,
        metric_map=metric_map,
        missing_note="No comparative value returned by model.",
    )
    _enforce_comparative_period_only(comparative_normalized, reporting_period_end_date)
    _apply_business_rules(comparative_normalized)

    _convert_metrics_to_billion_rub(normalized, reporting_currency)
    _convert_metrics_to_billion_rub(comparative_normalized, reporting_currency)

    missing_metrics = [item["metric_key"] for item in normalized if not item["found"]]
    calculated_metrics = _build_calculated_metrics(
        metrics=normalized,
        reporting_period=reporting_period,
        reporting_period_end_date=reporting_period_end_date,
    )
    all_metrics = [*normalized, *calculated_metrics]

    return {
        "source_document": source_document,
        "model": model,
        "company_name": _as_string(payload.get("company_name")),
        "ubo_surname": _as_string(payload.get("ubo_surname")),
        "reporting_period": reporting_period,
        "reporting_period_end_date": reporting_period_end_date,
        "reporting_currency": reporting_currency,
        "output_value_unit": TARGET_RUB_BN_UNIT,
        "notes": _as_string(payload.get("notes")),
        "metrics": all_metrics,
        "primary_metrics": normalized,
        "comparative_metrics": comparative_normalized,
        "comparative_period_end_dates": _collect_period_end_dates(comparative_normalized),
        "calculated_metrics": calculated_metrics,
        "missing_metrics": missing_metrics,
    }


def _normalize_bank_debt_result(
    payload: dict[str, Any],
    model: str,
    rep_year: str | None,
    source_document: str | None = None,
) -> dict[str, Any]:
    raw_rows = payload.get("rows")
    if not isinstance(raw_rows, list):
        raw_rows = []

    company_style = _as_string(payload.get("company_name_style"))
    unit_style = _as_string(payload.get("unit_style"))
    detected_reporting_period = _as_string(payload.get("reporting_period"))
    detected_reporting_period_end_date = _extract_reporting_period_end_date(
        payload.get("reporting_period_end_date")
    ) or _extract_reporting_period_end_date(detected_reporting_period)
    if not detected_reporting_period_end_date:
        detected_reporting_period_end_date = _infer_latest_reporting_period_end_date(raw_rows)
    detected_year = (
        detected_reporting_period_end_date[:4]
        if isinstance(detected_reporting_period_end_date, str) and len(detected_reporting_period_end_date) >= 4
        else None
    )

    normalized_rows: list[dict[str, Any]] = []
    for item in raw_rows:
        if not isinstance(item, dict):
            continue

        priority = _as_int(item.get("priority"))
        if priority not in {1, 2, 3, 4}:
            continue

        period = _as_string(item.get("period"))
        row_period_end_date = _extract_reporting_period_end_date(item.get("period_end_date")) or _extract_reporting_period_end_date(period)
        if rep_year:
            if not _period_matches_year(period or row_period_end_date, rep_year):
                continue
        elif detected_reporting_period_end_date:
            if row_period_end_date:
                if row_period_end_date != detected_reporting_period_end_date:
                    continue
            else:
                detected_year_fallback = detected_reporting_period_end_date[:4]
                if not _period_matches_year(period, detected_year_fallback):
                    continue

        amount = _as_number_with_parentheses(item.get("amount"))
        if amount is None:
            continue

        company_name = _as_string(item.get("company_name")) or company_style or "Не указано"
        section_name = _as_string(item.get("section_name")) or "Не указано"
        indicator = _as_string(item.get("indicator")) or "Не указано"
        unit = _as_string(item.get("unit")) or unit_style or "Не указано"

        normalized_rows.append(
            {
                "company_name": company_name,
                "section_name": section_name,
                "indicator": indicator,
                "priority": priority,
                "period": period or detected_reporting_period or rep_year,
                "period_end_date": row_period_end_date,
                "amount": amount,
                "unit": unit,
            }
        )

    normalized_rows.sort(
        key=lambda row: (
            row["company_name"].lower(),
            row["section_name"].lower(),
            int(row["priority"]),
            row["indicator"].lower(),
            row["period"].lower(),
        )
    )

    return {
        "mode": "bank_debt_notes",
        "model": model,
        "source_document": source_document,
        "rep_year": rep_year,
        "effective_rep_year": rep_year or detected_year,
        "detected_reporting_period": detected_reporting_period,
        "detected_reporting_period_end_date": detected_reporting_period_end_date,
        "rows": normalized_rows,
        "row_count": len(normalized_rows),
        "markdown_table": _bank_debt_rows_to_markdown(normalized_rows),
        "notes": _as_string(payload.get("notes")),
    }


def _normalize_metric_block(
    raw_metrics: Any,
    metrics: Sequence[MetricDefinition],
    metric_map: dict[str, MetricDefinition],
    missing_note: str,
) -> list[dict[str, Any]]:
    if not isinstance(raw_metrics, list):
        raw_metrics = []

    normalized_by_key: dict[str, dict[str, Any]] = {}
    for item in raw_metrics:
        if not isinstance(item, dict):
            continue
        key = _as_string(item.get("metric_key"))
        if not key or key not in metric_map or key in normalized_by_key:
            continue
        normalized_by_key[key] = _normalize_metric(item, metric_map[key])

    normalized: list[dict[str, Any]] = []
    for metric in metrics:
        existing = normalized_by_key.get(metric.key)
        if existing is not None:
            normalized.append(existing)
            continue
        normalized.append(_build_missing_metric(metric, missing_note))
    return normalized


def _build_missing_metric(definition: MetricDefinition, note: str) -> dict[str, Any]:
    return {
        "metric_key": definition.key,
        "metric_name": definition.name,
        "found": False,
        "value": None,
        "unit": None,
        "scale_multiplier": None,
        "period_label": None,
        "period_end_date": None,
        "selection_level": None,
        "statement": None,
        "page": None,
        "evidence": None,
        "confidence": None,
        "notes": note,
    }


def _max_found_period_end_date(metrics: list[dict[str, Any]]) -> str | None:
    period_dates: list[str] = []
    for item in metrics:
        if not item.get("found"):
            continue
        period_end = _extract_iso_date(item.get("period_end_date")) or _extract_iso_date(item.get("period_label"))
        if period_end:
            period_dates.append(period_end)
    if not period_dates:
        return None
    return max(period_dates)


def _collect_period_end_dates(metrics: list[dict[str, Any]]) -> list[str]:
    periods = sorted(
        {
            period_end
            for item in metrics
            for period_end in [
                _extract_iso_date(item.get("period_end_date")) or _extract_iso_date(item.get("period_label"))
            ]
            if period_end
        }
    )
    return periods


def _normalize_metric(item: dict[str, Any], definition: MetricDefinition) -> dict[str, Any]:
    found = bool(item.get("found"))
    value = _as_number(item.get("value"))
    scale_multiplier = _as_number(item.get("scale_multiplier"))
    page = _as_int(item.get("page"))
    confidence = _as_number(item.get("confidence"))
    if confidence is not None:
        confidence = max(0.0, min(1.0, confidence))

    if not found:
        value = None
        scale_multiplier = None
        page = None
        confidence = None

    return {
        "metric_key": definition.key,
        "metric_name": _as_string(item.get("metric_name")) or definition.name,
        "found": found,
        "value": value,
        "unit": _as_string(item.get("unit")),
        "scale_multiplier": scale_multiplier,
        "period_label": _as_string(item.get("period_label")),
        "period_end_date": _extract_iso_date(item.get("period_end_date")) or _extract_iso_date(item.get("period_label")),
        "selection_level": _as_string(item.get("selection_level")),
        "statement": _as_string(item.get("statement")),
        "page": page,
        "evidence": _as_string(item.get("evidence")),
        "confidence": confidence,
        "notes": _as_string(item.get("notes")),
    }


def _enforce_latest_period_only(
    metrics: list[dict[str, Any]],
    reporting_period_end_date: str | None,
) -> None:
    if not reporting_period_end_date:
        return

    for item in metrics:
        if not item.get("found"):
            continue
        metric_period_end = _extract_iso_date(item.get("period_end_date")) or _extract_iso_date(
            item.get("period_label")
        )
        if metric_period_end != reporting_period_end_date:
            _mark_metric_not_found(
                item,
                f"Excluded because metric period ({metric_period_end or 'unknown'}) does not match "
                f"latest period ({reporting_period_end_date}).",
            )


def _enforce_comparative_period_only(
    metrics: list[dict[str, Any]],
    reporting_period_end_date: str | None,
) -> None:
    if not reporting_period_end_date:
        return
    for item in metrics:
        if not item.get("found"):
            continue
        metric_period_end = _extract_iso_date(item.get("period_end_date")) or _extract_iso_date(
            item.get("period_label")
        )
        if not metric_period_end:
            _mark_metric_not_found(
                item,
                "Comparative period is missing period_end_date.",
            )
            continue
        if metric_period_end == reporting_period_end_date:
            _mark_metric_not_found(
                item,
                f"Comparative period ({metric_period_end}) matches latest period ({reporting_period_end_date}).",
            )


def _apply_business_rules(metrics: list[dict[str, Any]]) -> None:
    metrics_by_key = {item["metric_key"]: item for item in metrics}

    depreciation = metrics_by_key.get(DEPRECIATION_KEY)
    ppe = metrics_by_key.get(PPE_KEY)
    if not depreciation or not ppe:
        return
    if depreciation["found"]:
        return
    ppe_value = _as_number(ppe.get("value"))
    if not ppe.get("found") or ppe_value is None:
        return

    estimated_value = round(ppe_value * 0.1, 6)
    depreciation["found"] = True
    depreciation["value"] = estimated_value
    depreciation["unit"] = depreciation.get("unit") or ppe.get("unit")
    depreciation["scale_multiplier"] = depreciation.get("scale_multiplier") or ppe.get("scale_multiplier")
    depreciation["period_label"] = depreciation.get("period_label") or ppe.get("period_label")
    depreciation["period_end_date"] = depreciation.get("period_end_date") or ppe.get("period_end_date")
    depreciation["statement"] = depreciation.get("statement") or "Estimated from PPE"
    depreciation["page"] = depreciation.get("page") or ppe.get("page")
    depreciation["evidence"] = depreciation.get("evidence") or f"Estimated as 10% of {PPE_KEY}."
    depreciation["confidence"] = 0.35
    depreciation["notes"] = _append_note(
        depreciation.get("notes"),
        "Estimated as 10% of PPE because depreciation was not explicitly disclosed.",
    )


def _append_note(existing: Any, suffix: str) -> str:
    base = _as_string(existing)
    if not base:
        return suffix
    return f"{base} {suffix}"


def _mark_metric_not_found(metric: dict[str, Any], reason: str) -> None:
    metric["found"] = False
    metric["value"] = None
    metric["unit"] = None
    metric["scale_multiplier"] = None
    metric["selection_level"] = None
    metric["page"] = None
    metric["evidence"] = None
    metric["confidence"] = None
    metric["notes"] = _append_note(metric.get("notes"), reason)


def _convert_metrics_to_billion_rub(
    metrics: list[dict[str, Any]],
    reporting_currency: str | None,
) -> None:
    is_rub = _is_rub_currency(reporting_currency)
    for item in metrics:
        if not item.get("found"):
            continue
        value = _as_number(item.get("value"))
        if value is None:
            continue
        scale_multiplier = _as_number(item.get("scale_multiplier")) or 1.0
        absolute_value = value * scale_multiplier
        item["value"] = round(absolute_value / TARGET_SCALE_BN, 6)
        item["scale_multiplier"] = 1.0
        item["unit"] = TARGET_RUB_BN_UNIT
        if not is_rub:
            item["notes"] = _append_note(
                item.get("notes"),
                "Converted to RUB bn format, but reporting_currency is not RUB.",
            )


def _build_calculated_metrics(
    metrics: list[dict[str, Any]],
    reporting_period: str | None,
    reporting_period_end_date: str | None,
) -> list[dict[str, Any]]:
    metrics_by_key = {item.get("metric_key"): item for item in metrics if isinstance(item, dict)}
    period_label = reporting_period
    period_end_date = reporting_period_end_date
    is_ltm = _is_ltm_like_period(reporting_period)

    def get_value(metric_key: str) -> float | None:
        metric = metrics_by_key.get(metric_key)
        if not isinstance(metric, dict) or not metric.get("found"):
            return None
        return _as_number(metric.get("value"))

    def build_metric(
        metric_key: str,
        metric_name: str,
        unit: str,
        value: float | None,
        notes: str | None = None,
        found: bool | None = None,
    ) -> dict[str, Any]:
        is_found = found if found is not None else value is not None
        normalized_value = round(value, 6) if value is not None else None
        return {
            "metric_key": metric_key,
            "metric_name": metric_name,
            "found": bool(is_found),
            "value": normalized_value if is_found else None,
            "unit": unit if is_found else None,
            "scale_multiplier": 1.0 if is_found else None,
            "period_label": period_label,
            "period_end_date": period_end_date,
            "selection_level": "calculated",
            "statement": "Calculated from parsed metrics",
            "page": None,
            "evidence": None,
            "confidence": 1.0 if is_found else None,
            "notes": notes,
        }

    operating_profit = get_value(OPERATING_PROFIT_KEY)
    depreciation = get_value(DEPRECIATION_KEY)
    revenue = get_value(REVENUE_KEY)
    interest_expense = get_value(INTEREST_EXPENSE_KEY)
    long_term_debt = get_value(LONG_TERM_DEBT_KEY)
    short_term_debt = get_value(SHORT_TERM_DEBT_KEY)
    cash = get_value(CASH_KEY)

    ebitda_value: float | None = None
    total_debt_value: float | None = None
    net_debt_value: float | None = None

    calculated: list[dict[str, Any]] = []

    if operating_profit is None or depreciation is None:
        missing_parts = []
        if operating_profit is None:
            missing_parts.append("operating_profit")
        if depreciation is None:
            missing_parts.append("depreciation")
        calculated.append(
            build_metric(
                metric_key=CALC_EBITDA_KEY,
                metric_name="EBITDA",
                unit=TARGET_RUB_BN_UNIT,
                value=None,
                found=False,
                notes=f"Cannot calculate EBITDA: missing {', '.join(missing_parts)}.",
            )
        )
    else:
        ebitda_value = operating_profit + depreciation
        calculated.append(
            build_metric(
                metric_key=CALC_EBITDA_KEY,
                metric_name="EBITDA",
                unit=TARGET_RUB_BN_UNIT,
                value=ebitda_value,
                notes="Calculated as operating_profit + depreciation.",
            )
        )

    if ebitda_value is None or revenue is None:
        missing_parts = []
        if ebitda_value is None:
            missing_parts.append("ebitda")
        if revenue is None:
            missing_parts.append("revenue")
        calculated.append(
            build_metric(
                metric_key=CALC_EBITDA_MARGIN_KEY,
                metric_name="Рентабельность EBITDA",
                unit="%",
                value=None,
                found=False,
                notes=f"Cannot calculate EBITDA margin: missing {', '.join(missing_parts)}.",
            )
        )
    elif revenue == 0:
        calculated.append(
            build_metric(
                metric_key=CALC_EBITDA_MARGIN_KEY,
                metric_name="Рентабельность EBITDA",
                unit="%",
                value=None,
                found=False,
                notes="Cannot calculate EBITDA margin: revenue is zero.",
            )
        )
    else:
        calculated.append(
            build_metric(
                metric_key=CALC_EBITDA_MARGIN_KEY,
                metric_name="Рентабельность EBITDA",
                unit="%",
                value=(ebitda_value / revenue) * 100.0,
                notes="Calculated as EBITDA / revenue * 100.",
            )
        )

    if long_term_debt is None or short_term_debt is None:
        missing_parts = []
        if short_term_debt is None:
            missing_parts.append("short_term_debt_and_lease")
        if long_term_debt is None:
            missing_parts.append("long_term_debt_and_lease")
        calculated.append(
            build_metric(
                metric_key=CALC_TOTAL_DEBT_KEY,
                metric_name="Долг всего",
                unit=TARGET_RUB_BN_UNIT,
                value=None,
                found=False,
                notes=f"Cannot calculate total debt: missing {', '.join(missing_parts)}.",
            )
        )
    else:
        total_debt_value = short_term_debt + long_term_debt
        calculated.append(
            build_metric(
                metric_key=CALC_TOTAL_DEBT_KEY,
                metric_name="Долг всего",
                unit=TARGET_RUB_BN_UNIT,
                value=total_debt_value,
                notes="Calculated as short_term_debt_and_lease + long_term_debt_and_lease.",
            )
        )

    if total_debt_value is None or cash is None:
        missing_parts = []
        if total_debt_value is None:
            missing_parts.append("total_debt")
        if cash is None:
            missing_parts.append("cash_and_cash_equivalents")
        calculated.append(
            build_metric(
                metric_key=CALC_NET_DEBT_KEY,
                metric_name="Чистый долг",
                unit=TARGET_RUB_BN_UNIT,
                value=None,
                found=False,
                notes=f"Cannot calculate net debt: missing {', '.join(missing_parts)}.",
            )
        )
    else:
        net_debt_value = total_debt_value - cash
        calculated.append(
            build_metric(
                metric_key=CALC_NET_DEBT_KEY,
                metric_name="Чистый долг",
                unit=TARGET_RUB_BN_UNIT,
                value=net_debt_value,
                notes="Calculated as total_debt - cash_and_cash_equivalents.",
            )
        )

    if ebitda_value is None or interest_expense is None:
        missing_parts = []
        if ebitda_value is None:
            missing_parts.append("ebitda")
        if interest_expense is None:
            missing_parts.append("interest_expense_loans")
        calculated.append(
            build_metric(
                metric_key=CALC_EBITDA_TO_INTEREST_KEY,
                metric_name="EBITDA / % расходы",
                unit="x",
                value=None,
                found=False,
                notes=f"Cannot calculate EBITDA / interest expense: missing {', '.join(missing_parts)}.",
            )
        )
    else:
        denominator = abs(interest_expense)
        if denominator == 0:
            calculated.append(
                build_metric(
                    metric_key=CALC_EBITDA_TO_INTEREST_KEY,
                    metric_name="EBITDA / % расходы",
                    unit="x",
                    value=None,
                    found=False,
                    notes="Cannot calculate EBITDA / interest expense: denominator is zero.",
                )
            )
        else:
            calculated.append(
                build_metric(
                    metric_key=CALC_EBITDA_TO_INTEREST_KEY,
                    metric_name="EBITDA / % расходы",
                    unit="x",
                    value=ebitda_value / denominator,
                    notes="Calculated as EBITDA / abs(interest_expense_loans).",
                )
            )

    if net_debt_value is None or ebitda_value is None:
        missing_parts = []
        if net_debt_value is None:
            missing_parts.append("net_debt")
        if ebitda_value is None:
            missing_parts.append("ebitda")
        calculated.append(
            build_metric(
                metric_key=CALC_NET_DEBT_TO_EBITDA_LTM_KEY,
                metric_name="Чистый долг / EBITDA LTM",
                unit="x",
                value=None,
                found=False,
                notes=f"Cannot calculate net debt / EBITDA LTM: missing {', '.join(missing_parts)}.",
            )
        )
    elif ebitda_value <= 0:
        calculated.append(
            build_metric(
                metric_key=CALC_NET_DEBT_TO_EBITDA_LTM_KEY,
                metric_name="Чистый долг / EBITDA LTM",
                unit="x",
                value=None,
                found=False,
                notes="Cannot calculate net debt / EBITDA LTM: EBITDA is zero or negative.",
            )
        )
    else:
        notes = "Calculated as net_debt / EBITDA."
        if not is_ltm:
            notes = f"{notes} EBITDA is not explicitly LTM in reporting_period."
        calculated.append(
            build_metric(
                metric_key=CALC_NET_DEBT_TO_EBITDA_LTM_KEY,
                metric_name="Чистый долг / EBITDA LTM",
                unit="x",
                value=net_debt_value / ebitda_value,
                notes=notes,
            )
        )

    return calculated


def _is_ltm_like_period(reporting_period: str | None) -> bool:
    if not reporting_period:
        return False
    text = reporting_period.strip().lower()
    if not text:
        return False
    if re.search(r"\b(ltm|12m|fy|full\s*year|annual)\b", text):
        return True
    if re.search(r"\b(1q|q1|q2|q3|h1|6m|9m)\b", text):
        return False
    if re.search(r"^\s*20\d{2}\s*$", text):
        return True
    return False


def _is_rub_currency(currency: str | None) -> bool:
    if not currency:
        return False
    normalized = currency.strip().upper()
    return normalized in {"RUB", "RUR", "РУБ", "РУБ.", "RUSSIAN RUBLE", "RUSSIAN ROUBLE"}


def _extract_iso_date(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None

    iso_match = _ISO_DATE_RE.search(text)
    if iso_match:
        parsed = _safe_iso_date(iso_match.group(1))
        if parsed:
            return parsed

    dot_match = _DOT_DATE_RE.search(text)
    if dot_match:
        day, month, year = int(dot_match.group(1)), int(dot_match.group(2)), int(dot_match.group(3))
        parsed = _safe_date(year, month, day)
        if parsed:
            return parsed

    text_match = _TEXT_DATE_RE.search(text)
    if text_match:
        day = int(text_match.group(1))
        month_name = text_match.group(2).lower()
        year = int(text_match.group(3))
        month = _MONTH_MAP.get(month_name)
        if month is not None:
            parsed = _safe_date(year, month, day)
            if parsed:
                return parsed

    quarter_match = _QUARTER_RE.search(text)
    if quarter_match:
        quarter = int(quarter_match.group(1))
        year = int(quarter_match.group(2))
        if quarter == 1:
            return f"{year:04d}-03-31"
        if quarter == 2:
            return f"{year:04d}-06-30"
        if quarter == 3:
            return f"{year:04d}-09-30"
        if quarter == 4:
            return f"{year:04d}-12-31"

    years = [int(year_text) for year_text in _YEAR_RE.findall(text)]
    if years:
        year = max(years)
        return f"{year:04d}-12-31"

    return None


def _safe_iso_date(value: str) -> str | None:
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return None
    return parsed.isoformat()


def _safe_date(year: int, month: int, day: int) -> str | None:
    try:
        parsed = date(year, month, day)
    except ValueError:
        return None
    return parsed.isoformat()


def _file_state_name(file_ref: Any) -> str:
    state = getattr(file_ref, "state", None)
    if state is None:
        return "UNSPECIFIED"
    if isinstance(state, str):
        return state.upper()
    state_name = getattr(state, "name", None)
    if isinstance(state_name, str):
        return state_name.upper()
    return str(state).upper()


def _is_resource_exhausted_error(exc: Exception) -> bool:
    text = str(exc).upper()
    if "RESOURCE_EXHAUSTED" in text:
        return True
    if " 429" in text:
        return True
    if "CODE: 429" in text:
        return True
    return False


def _is_transient_network_error(exc: Exception) -> bool:
    text = str(exc).upper()
    class_name = exc.__class__.__name__.upper()

    if class_name in {
        "REMOTEPROTOCOLERROR",
        "CONNECTERROR",
        "CONNECTTIMEOUT",
        "READTIMEOUT",
        "WRITEERROR",
        "READERROR",
        "TIMEOUTEXCEPTION",
        "NETWORKERROR",
        "PROTOCOLERROR",
    }:
        return True

    transient_markers = (
        "SERVER DISCONNECTED WITHOUT SENDING A RESPONSE",
        "CONNECTION RESET",
        "BROKEN PIPE",
        "TIMED OUT",
        "TIMEOUT",
        "TEMPORARY FAILURE",
        "TRY AGAIN LATER",
        "SERVICE UNAVAILABLE",
        "INTERNAL SERVER ERROR",
        "BAD GATEWAY",
        "GATEWAY TIMEOUT",
        "UNAVAILABLE",
        "HTTP 500",
        "HTTP 502",
        "HTTP 503",
        "HTTP 504",
        " CODE: 500",
        " CODE: 502",
        " CODE: 503",
        " CODE: 504",
    )
    return any(marker in text for marker in transient_markers)


def _retry_delay_seconds(attempt: int, base: float, max_delay: float) -> float:
    exp_delay = min(max_delay, base * (2**attempt))
    jitter = random.uniform(0.0, min(1.0, exp_delay * 0.2))
    return exp_delay + jitter


def _as_string(value: Any) -> str | None:
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return None


def _period_matches_year(period: str | None, rep_year: str) -> bool:
    if not period:
        return False
    return bool(re.search(rf"\b{re.escape(rep_year)}\b", period))


def _normalize_optional_rep_year(rep_year: int | str | None) -> str | None:
    if rep_year is None:
        return None
    text = str(rep_year).strip()
    if not text:
        return None
    if not re.fullmatch(r"20\d{2}", text):
        raise ValueError("rep_year must be a 4-digit year like 2024.")
    return text


def _extract_reporting_period_end_date(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None

    quarter_match = re.search(r"\b([1-4])\s*(?:q|кв)\s*([12]\d{3})\b", text, re.IGNORECASE)
    if quarter_match:
        quarter = int(quarter_match.group(1))
        year = int(quarter_match.group(2))
        quarter_map = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}
        return f"{year:04d}-{quarter_map[quarter]}"

    half_match = re.search(r"\b([12])\s*h\s*([12]\d{3})\b", text, re.IGNORECASE)
    if half_match:
        half = int(half_match.group(1))
        year = int(half_match.group(2))
        return f"{year:04d}-06-30" if half == 1 else f"{year:04d}-12-31"

    half_ru_match = re.search(r"\b([12])\s*(?:пг|полугод(?:ие|ия)?)\s*([12]\d{3})\b", text, re.IGNORECASE)
    if half_ru_match:
        half = int(half_ru_match.group(1))
        year = int(half_ru_match.group(2))
        return f"{year:04d}-06-30" if half == 1 else f"{year:04d}-12-31"

    months_match = re.search(r"\b(3|6|9|12)\s*(?:m|м|мес(?:яц(?:ев|а)?)?)\s*([12]\d{3})\b", text, re.IGNORECASE)
    if months_match:
        months = int(months_match.group(1))
        year = int(months_match.group(2))
        month_map = {3: "03-31", 6: "06-30", 9: "09-30", 12: "12-31"}
        return f"{year:04d}-{month_map[months]}"

    return _extract_iso_date(text)


def _infer_latest_reporting_period_end_date(raw_rows: list[Any]) -> str | None:
    dates: list[str] = []
    for item in raw_rows:
        if not isinstance(item, dict):
            continue
        period_end_date = _extract_reporting_period_end_date(item.get("period_end_date")) or _extract_reporting_period_end_date(
            item.get("period")
        )
        if period_end_date:
            dates.append(period_end_date)
    if not dates:
        return None
    return max(dates)


def _as_number_with_parentheses(value: Any) -> float | None:
    numeric = _as_number(value)
    if numeric is not None:
        return numeric
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    is_negative = text.startswith("(") and text.endswith(")")
    cleaned = text.strip("()").replace(" ", "").replace(",", ".")
    if not cleaned:
        return None
    try:
        parsed = float(cleaned)
    except ValueError:
        return None
    return -abs(parsed) if is_negative else parsed


def _format_amount_for_markdown(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    text = f"{value:.6f}".rstrip("0").rstrip(".")
    return text


def _escape_markdown_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    return text.replace("|", "\\|").strip()


def _bank_debt_rows_to_markdown(rows: list[dict[str, Any]]) -> str:
    header = (
        "|Название компании | Номер и название раздела (Примечания, Приложения)| "
        "Показатель | Приоритет| Период | Сумма | Единица измерения |"
    )
    separator = "|---|---|---|---|---|---|---|"
    lines = [header, separator]
    for row in rows:
        amount = _format_amount_for_markdown(float(row["amount"]))
        lines.append(
            "|"
            + _escape_markdown_cell(row["company_name"])
            + " | "
            + _escape_markdown_cell(row["section_name"])
            + "| "
            + _escape_markdown_cell(row["indicator"])
            + " | "
            + _escape_markdown_cell(row["priority"])
            + "| "
            + _escape_markdown_cell(row["period"])
            + " | "
            + _escape_markdown_cell(amount)
            + " | "
            + _escape_markdown_cell(row["unit"])
            + " |"
        )
    return "\n".join(lines)


def _as_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip().replace(" ", "").replace(",", ".")
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, float) and value.is_integer():
        converted = int(value)
        return converted if converted > 0 else None
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit():
            converted = int(text)
            return converted if converted > 0 else None
    return None

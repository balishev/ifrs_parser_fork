from __future__ import annotations

import json
import os
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

DEFAULT_CONFIG_PATH = Path("config/sheets_export.json")
DEFAULT_HEADERS = [
    "Отрасль",
    "UBO",
    "Компания",
    "Type",
    "Показатель",
    "Сегмент",
    "Сегмент",
    "Источник",
    "Ед.изм.",
    "LTM",
    "",
    "2025",
    "2024",
    "2023",
    "2022",
    "",
    "2H25",
    "1H25",
    "2H24",
    "1H24",
    "2H23",
    "1H23",
    "",
    "3Q25",
    "2Q25",
    "1Q25",
    "4Q24",
    "3Q24",
    "2Q24",
    "1Q24",
    "4Q23",
    "3Q23",
    "2Q23",
    "1Q23",
]
DEFAULT_LTM_COL = 10
DEFAULT_YEAR_COLS = {2025: 12, 2024: 13, 2023: 14, 2022: 15}
DEFAULT_HALF_COLS = {
    (2025, 2): 17,
    (2025, 1): 18,
    (2024, 2): 19,
    (2024, 1): 20,
    (2023, 2): 21,
    (2023, 1): 22,
}
DEFAULT_QUARTER_COLS = {
    (2025, 3): 24,
    (2025, 2): 25,
    (2025, 1): 26,
    (2024, 4): 27,
    (2024, 3): 28,
    (2024, 2): 29,
    (2024, 1): 30,
    (2023, 4): 31,
    (2023, 3): 32,
    (2023, 2): 33,
    (2023, 1): 34,
}

METRIC_EXPORT_ORDER = (
    "revenue",
    "ebitda",
    "ebitda_margin_pct",
    "interest_expense_loans",
    "depreciation",
    "cash_and_cash_equivalents",
    "property_plant_and_equipment",
    "operating_profit",
    "short_term_debt_and_lease",
    "long_term_debt_and_lease",
    "total_debt",
    "net_debt",
    "ebitda_to_interest_expense",
    "net_debt_to_ebitda_ltm",
)


@dataclass(frozen=True, slots=True)
class MetricSheetSpec:
    label: str
    segment_1: str | None = None
    segment_2: str | None = None
    unit_override: str | None = None
    force_ltm: bool = False


METRIC_SPECS: dict[str, MetricSheetSpec] = {
    "revenue": MetricSheetSpec("Выручка", unit_override="млрд руб."),
    "ebitda": MetricSheetSpec("EBITDA", unit_override="млрд руб."),
    "ebitda_margin_pct": MetricSheetSpec("Рентабельность EBITDA (EBITDA/выручка)", unit_override="%"),
    "interest_expense_loans": MetricSheetSpec("% расходы", unit_override="млрд руб."),
    "depreciation": MetricSheetSpec("Амортизация", unit_override="млрд руб."),
    "cash_and_cash_equivalents": MetricSheetSpec("Денежные средства и экв.", unit_override="млрд руб."),
    "property_plant_and_equipment": MetricSheetSpec("ОС", unit_override="млрд руб."),
    "operating_profit": MetricSheetSpec("Операционная прибыль", unit_override="млрд руб."),
    "short_term_debt_and_lease": MetricSheetSpec("Долг", segment_1="всего", segment_2="КС", unit_override="млрд руб."),
    "long_term_debt_and_lease": MetricSheetSpec("Долг", segment_1="всего", segment_2="ДС", unit_override="млрд руб."),
    "total_debt": MetricSheetSpec("Долг", segment_1="всего", segment_2="всего", unit_override="млрд руб."),
    "net_debt": MetricSheetSpec("Чистый долг", unit_override="млрд руб."),
    "ebitda_to_interest_expense": MetricSheetSpec("EBITDA/ % расходы", unit_override="x"),
    "net_debt_to_ebitda_ltm": MetricSheetSpec("Чистый долг/ EBITDA LTM", unit_override="x", force_ltm=True),
}


@dataclass(slots=True)
class SheetsExportConfig:
    enabled: bool = True
    credentials_json: str | None = None
    spreadsheet_id: str | None = None
    spreadsheet_title: str = "IFRS Parser Export"
    worksheet_name: str = "Импорт"
    create_spreadsheet_if_missing: bool = True
    share_with: list[str] = field(default_factory=list)
    source_label: str = "МСФО"
    type_label: str = "Source"
    default_industry: str = ""
    include_not_found_metrics: bool = False
    ubo_by_company: dict[str, str] = field(default_factory=dict)
    ubo_unknown_value: str = "Не определен"


def append_result_to_google_sheets(
    result: dict[str, Any],
    config_path: str | Path | None = None,
) -> dict[str, Any] | None:
    loaded = load_sheets_export_config(config_path)
    if loaded is None:
        return None

    config, resolved_path, raw_payload = loaded
    if not config.enabled:
        return {"status": "disabled", "appended_rows": 0}

    exporter = GoogleSheetsExporter(config)
    summary = exporter.append_result(result)
    if resolved_path is not None:
        _persist_spreadsheet_id_if_needed(
            config_path=resolved_path,
            raw_payload=raw_payload,
            spreadsheet_id=summary.get("spreadsheet_id"),
        )
    return summary


def fetch_company_rows_from_google_sheets(
    company_name: str,
    config_path: str | Path | None = None,
) -> dict[str, Any] | None:
    loaded = load_sheets_export_config(config_path)
    if loaded is None:
        return None

    config, _, _ = loaded
    if not config.enabled:
        return {"status": "disabled", "rows": []}

    exporter = GoogleSheetsExporter(config)
    return exporter.fetch_company_rows(company_name)


def initialize_google_sheet(config_path: str | Path | None = None) -> dict[str, Any]:
    loaded = load_sheets_export_config(config_path)
    if loaded is None:
        raise FileNotFoundError(
            "Google Sheets config file not found. Create config/sheets_export.json "
            "from config/sheets_export.example.json."
        )
    config, resolved_path, raw_payload = loaded
    exporter = GoogleSheetsExporter(config)
    info = exporter.ensure_ready()
    if resolved_path is not None:
        _persist_spreadsheet_id_if_needed(
            config_path=resolved_path,
            raw_payload=raw_payload,
            spreadsheet_id=info.get("spreadsheet_id"),
        )
    return info


class GoogleSheetsExporter:
    def __init__(self, config: SheetsExportConfig) -> None:
        try:
            import gspread
        except ImportError as exc:
            raise RuntimeError("gspread is not installed. Run: pip install gspread") from exc

        credentials_json = _as_non_empty_str(config.credentials_json)
        if not credentials_json:
            raise ValueError("credentials_json is required in sheets export config.")

        credentials_path = Path(credentials_json)
        if not credentials_path.exists():
            raise FileNotFoundError(f"Credentials JSON not found: {credentials_path}")
        credentials_payload = json.loads(credentials_path.read_text(encoding="utf-8"))
        self._service_account_email = _as_non_empty_str(credentials_payload.get("client_email"))

        self._gspread = gspread
        self._config = config
        self._client = gspread.service_account(filename=str(credentials_path))
        self._spreadsheet: Any | None = None
        self._worksheet: Any | None = None
        self._year_cols = dict(DEFAULT_YEAR_COLS)
        self._ltm_col = DEFAULT_LTM_COL
        self._half_cols = dict(DEFAULT_HALF_COLS)
        self._quarter_cols = dict(DEFAULT_QUARTER_COLS)
        self._max_write_retries = 6

    def ensure_ready(self) -> dict[str, Any]:
        self._ensure_spreadsheet()
        self._ensure_worksheet()
        self._ensure_headers()
        self._detect_columns()
        return {
            "status": "ready",
            "spreadsheet_id": self._spreadsheet.id,
            "spreadsheet_url": self._spreadsheet.url,
            "worksheet_name": self._worksheet.title,
        }

    def append_result(self, result: dict[str, Any]) -> dict[str, Any]:
        info = self.ensure_ready()
        rows = build_rows_for_sheet(
            result=result,
            config=self._config,
            ltm_col=self._ltm_col,
            year_cols=self._year_cols,
            half_cols=self._half_cols,
            quarter_cols=self._quarter_cols,
        )
        if not rows:
            return {**info, "status": "no_rows", "appended_rows": 0}

        company_name = _as_non_empty_str(result.get("company_name"))
        self._remove_legacy_ubo_metric_rows(company_name)
        self._merge_duplicate_rows()
        self._upsert_rows(rows)
        return {**info, "status": "ok", "appended_rows": len(rows)}

    def _remove_legacy_ubo_metric_rows(self, company_name: str | None) -> None:
        if not company_name:
            return
        all_rows = self._worksheet.get_all_values()
        if len(all_rows) <= 1:
            return
        target_company = _normalize_lookup_text(company_name)
        to_delete: list[int] = []
        for row_idx, row in enumerate(all_rows[1:], start=2):
            row_company = _normalize_lookup_text(_row_value(row, 3))
            metric_name = _normalize_lookup_text(_row_value(row, 5))
            if row_company == target_company and metric_name == "ubo":
                to_delete.append(row_idx)
        for row_idx in sorted(to_delete, reverse=True):
            self._call_with_write_retry(self._worksheet.delete_rows, row_idx)

    def _upsert_rows(self, rows: list[list[Any]]) -> None:
        existing_rows = self._worksheet.get_all_values()
        key_to_row_index: dict[tuple[str, ...], int] = {}
        for idx, row in enumerate(existing_rows[1:], start=2):
            key = _sheet_row_key(row)
            if key and key not in key_to_row_index:
                key_to_row_index[key] = idx

        existing_row_map: dict[int, list[Any]] = {
            idx: list(row)
            for idx, row in enumerate(existing_rows[1:], start=2)
        }

        time_cols = sorted(
            {
                self._ltm_col,
                *self._year_cols.values(),
                *self._half_cols.values(),
                *self._quarter_cols.values(),
            }
        )
        update_cols = [*range(1, 10), *time_cols]
        max_col = max(update_cols) if update_cols else 10

        new_rows: list[list[Any]] = []
        pending_updates: dict[int, list[Any]] = {}
        next_append_row = self._next_append_row()

        for row in rows:
            key = _sheet_row_key(row)
            if not key:
                continue

            target_row_idx = key_to_row_index.get(key)
            if target_row_idx is None:
                new_rows.append(row)
                target_row_idx = next_append_row + len(new_rows) - 1
                key_to_row_index[key] = target_row_idx
                continue

            base_row = pending_updates.get(target_row_idx)
            if base_row is None:
                base_row = list(existing_row_map.get(target_row_idx, []))
                pending_updates[target_row_idx] = base_row

            changed = False
            for col_idx in update_cols:
                new_value = _row_value(row, col_idx)
                if _is_blank(new_value):
                    continue
                current_value = _row_value(base_row, col_idx)
                if _cell_values_equal(current_value, new_value):
                    continue
                _set_cell(base_row, col_idx, new_value)
                changed = True

            if not changed:
                pending_updates.pop(target_row_idx, None)

        if new_rows:
            self._append_new_rows(new_rows)

        if not pending_updates:
            return

        requests: list[dict[str, Any]] = []
        for row_idx in sorted(pending_updates):
            row_payload = pending_updates[row_idx]
            if len(row_payload) < max_col:
                row_payload.extend([""] * (max_col - len(row_payload)))
            requests.append(
                {
                    "range": f"A{row_idx}:{_column_letter(max_col)}{row_idx}",
                    "values": [row_payload[0:max_col]],
                }
            )

        self._write_batch_update(requests)

    def _append_new_rows(self, rows: list[list[Any]]) -> None:
        if not rows:
            return
        max_width = max(len(row) for row in rows)
        padded_rows = [row + [""] * (max_width - len(row)) for row in rows]
        start_row = self._next_append_row()
        end_row = start_row + len(padded_rows) - 1
        end_col = _column_letter(max_width)
        target_range = f"A{start_row}:{end_col}{end_row}"
        self._write_update(target_range, padded_rows)

    def _merge_duplicate_rows(self) -> None:
        all_rows = self._worksheet.get_all_values()
        if len(all_rows) <= 2:
            return

        time_cols = sorted(
            {
                self._ltm_col,
                *self._year_cols.values(),
                *self._half_cols.values(),
                *self._quarter_cols.values(),
            }
        )

        primary_for_key: dict[tuple[str, ...], int] = {}
        duplicate_indices: list[int] = []
        primary_updates: dict[int, list[Any]] = {}
        changed_primary: set[int] = set()
        for row_idx, row in enumerate(all_rows[1:], start=2):
            key = _sheet_row_key(row)
            if not key:
                continue
            primary_idx = primary_for_key.get(key)
            if primary_idx is None:
                primary_for_key[key] = row_idx
                continue

            primary_row = primary_updates.get(primary_idx)
            if primary_row is None:
                primary_row = list(all_rows[primary_idx - 1])
                primary_updates[primary_idx] = primary_row
            duplicate_row = row

            for col_idx in range(1, 10):
                if _is_blank(_row_value(primary_row, col_idx)) and not _is_blank(_row_value(duplicate_row, col_idx)):
                    _set_cell(primary_row, col_idx, _row_value(duplicate_row, col_idx))
                    changed_primary.add(primary_idx)

            for col_idx in time_cols:
                primary_value = _row_value(primary_row, col_idx)
                duplicate_value = _row_value(duplicate_row, col_idx)
                if _is_blank(primary_value) and not _is_blank(duplicate_value):
                    _set_cell(primary_row, col_idx, duplicate_value)
                    changed_primary.add(primary_idx)

            duplicate_indices.append(row_idx)

        max_col = max([9, *time_cols])
        requests: list[dict[str, Any]] = []
        for primary_idx, primary_row in sorted(primary_updates.items()):
            if primary_idx not in changed_primary:
                continue
            if len(primary_row) < max_col:
                primary_row.extend([""] * (max_col - len(primary_row)))
            requests.append(
                {
                    "range": f"A{primary_idx}:{_column_letter(max_col)}{primary_idx}",
                    "values": [primary_row[0:max_col]],
                }
            )
        if requests:
            self._write_batch_update(requests)

        for row_idx in sorted(duplicate_indices, reverse=True):
            self._call_with_write_retry(self._worksheet.delete_rows, row_idx)

    def _next_append_row(self) -> int:
        all_rows = self._worksheet.get_all_values()
        if not all_rows:
            return 2
        last_non_empty = 0
        for idx, row in enumerate(all_rows, start=1):
            if any(str(cell).strip() for cell in row):
                last_non_empty = idx
        return max(2, last_non_empty + 1)

    def fetch_company_rows(self, company_name: str) -> dict[str, Any]:
        info = self.ensure_ready()
        target = _normalize_lookup_text(company_name)
        if not target:
            return {**info, "status": "invalid_company_name", "rows": []}

        all_rows = self._worksheet.get_all_values()
        if not all_rows:
            return {**info, "status": "empty_sheet", "rows": []}

        headers = all_rows[0]
        rows: list[list[str]] = []
        for row in all_rows[1:]:
            if len(row) < 5:
                continue
            metric_cell = row[4].strip().lower()
            if metric_cell == "показатель":
                continue
            row_company = row[2] if len(row) >= 3 else ""
            if _normalize_lookup_text(row_company) == target:
                rows.append(row)

        status = "ok" if rows else "company_not_found"
        return {
            **info,
            "status": status,
            "company_name": company_name,
            "headers": headers,
            "rows": rows,
        }

    def _ensure_spreadsheet(self) -> None:
        if self._spreadsheet is not None:
            return

        spreadsheet_id = _as_non_empty_str(self._config.spreadsheet_id)
        if spreadsheet_id:
            self._spreadsheet = self._client.open_by_key(spreadsheet_id)
            return

        if not self._config.create_spreadsheet_if_missing:
            raise ValueError("spreadsheet_id is missing and create_spreadsheet_if_missing=false.")

        title = _as_non_empty_str(self._config.spreadsheet_title) or "IFRS Parser Export"
        try:
            self._spreadsheet = self._client.create(title)
        except Exception as exc:
            error_text = str(exc).lower()
            if "quota" in error_text and "drive" in error_text:
                email_hint = self._service_account_email or "<service-account-email>"
                raise RuntimeError(
                    "Service account cannot create new Google Sheet due Drive quota. "
                    "Create the spreadsheet manually in your Google account, share it with "
                    f"{email_hint}, then set spreadsheet_id in config/sheets_export.json."
                ) from exc
            raise
        self._config.spreadsheet_id = self._spreadsheet.id
        for email in self._config.share_with:
            clean_email = email.strip()
            if not clean_email:
                continue
            self._spreadsheet.share(clean_email, perm_type="user", role="writer", notify=False)

    def _ensure_worksheet(self) -> None:
        if self._worksheet is not None:
            return
        if self._spreadsheet is None:
            self._ensure_spreadsheet()
        assert self._spreadsheet is not None

        title = _as_non_empty_str(self._config.worksheet_name) or "Импорт"
        try:
            self._worksheet = self._spreadsheet.worksheet(title)
        except self._gspread.exceptions.WorksheetNotFound:
            self._worksheet = self._spreadsheet.add_worksheet(title=title, rows=2000, cols=40)

    def _ensure_headers(self) -> None:
        if self._worksheet is None:
            self._ensure_worksheet()
        assert self._worksheet is not None

        first_row = self._worksheet.row_values(1)
        if _row_matches_template_header(first_row, DEFAULT_HEADERS):
            return
        end_col = _column_letter(len(DEFAULT_HEADERS))
        self._write_update(f"A1:{end_col}1", [DEFAULT_HEADERS])

    def _detect_columns(self) -> None:
        if self._worksheet is None:
            self._ensure_worksheet()
        assert self._worksheet is not None

        row1 = self._worksheet.row_values(1)
        row2 = self._worksheet.row_values(2)

        ltm_col = None
        year_cols: dict[int, int] = {}
        half_cols: dict[tuple[int, int], int] = {}
        quarter_cols: dict[tuple[int, int], int] = {}

        for row in (row2, row1):
            for idx, raw in enumerate(row, start=1):
                marker = _parse_header_marker(raw)
                if marker is None:
                    continue
                marker_kind = marker[0]
                if marker_kind == "ltm" and ltm_col is None:
                    ltm_col = idx
                    continue
                if marker_kind == "year":
                    year = marker[1]
                    year_cols[year] = idx
                    continue
                if marker_kind == "half":
                    year, half_index = marker[1], marker[2]
                    half_cols[(year, half_index)] = idx
                    continue
                if marker_kind == "quarter":
                    year, quarter_index = marker[1], marker[2]
                    quarter_cols[(year, quarter_index)] = idx

        self._ltm_col = ltm_col or DEFAULT_LTM_COL
        self._year_cols = year_cols or dict(DEFAULT_YEAR_COLS)
        self._half_cols = half_cols or dict(DEFAULT_HALF_COLS)
        self._quarter_cols = quarter_cols or dict(DEFAULT_QUARTER_COLS)

    def _write_update(self, target_range: str, values: list[list[Any]]) -> None:
        self._call_with_write_retry(
            self._worksheet.update,
            values,
            target_range,
            value_input_option="USER_ENTERED",
        )

    def _write_batch_update(self, requests: list[dict[str, Any]]) -> None:
        if not requests:
            return
        self._call_with_write_retry(
            self._worksheet.batch_update,
            requests,
            value_input_option="USER_ENTERED",
        )

    def _call_with_write_retry(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        attempt = 0
        while True:
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                if not _is_retryable_sheets_write_error(exc):
                    raise
                if attempt >= self._max_write_retries:
                    raise
                time.sleep(_sheets_retry_delay_seconds(attempt))
                attempt += 1


def build_rows_for_sheet(
    result: dict[str, Any],
    config: SheetsExportConfig,
    ltm_col: int,
    year_cols: dict[int, int],
    half_cols: dict[tuple[int, int], int],
    quarter_cols: dict[tuple[int, int], int],
) -> list[list[Any]]:
    metric_candidates_by_key = _collect_metric_candidates(result)

    company_name = _as_non_empty_str(result.get("company_name")) or "Не указано"
    ubo_surname = _resolve_ubo_surname(result=result, config=config, company_name=company_name)
    reporting_period = _as_non_empty_str(result.get("reporting_period"))
    reporting_period_end_date = _as_non_empty_str(result.get("reporting_period_end_date"))
    fallback_year = _extract_year(reporting_period_end_date or "") or _extract_year(reporting_period or "")
    fallback_period_slot = _resolve_period_slot(
        reporting_period=reporting_period,
        reporting_period_end_date=reporting_period_end_date,
    )

    max_col_candidates = [15, ltm_col]
    max_col_candidates.extend(year_cols.values())
    max_col_candidates.extend(half_cols.values())
    max_col_candidates.extend(quarter_cols.values())
    max_col = max(max_col_candidates)
    rows: list[list[Any]] = []
    for metric_key in METRIC_EXPORT_ORDER:
        spec = METRIC_SPECS.get(metric_key)
        metric_items = metric_candidates_by_key.get(metric_key, [])
        if spec is None or not metric_items:
            continue

        row = [""] * max_col
        _set_cell(row, 1, config.default_industry)
        _set_cell(row, 2, ubo_surname)
        _set_cell(row, 3, company_name)
        _set_cell(row, 4, config.type_label)
        _set_cell(row, 5, spec.label)
        _set_cell(row, 6, spec.segment_1)
        _set_cell(row, 7, spec.segment_2)
        _set_cell(row, 8, config.source_label)
        resolved_unit = _resolve_unit(_pick_unit_metric(metric_items), spec)
        _set_cell(row, 9, resolved_unit)

        has_written_value = False
        has_any_found = False
        for metric in metric_items:
            found = bool(metric.get("found"))
            value = _to_float(metric.get("value"))
            if found:
                has_any_found = True
            if not found or value is None:
                continue

            metric_period_label = _as_non_empty_str(metric.get("period_label"))
            metric_period_end_date = _as_non_empty_str(metric.get("period_end_date"))
            metric_report_year = _extract_year(metric_period_end_date or "") or _extract_year(metric_period_label or "")
            metric_period_slot = _resolve_period_slot(
                reporting_period=metric_period_label or reporting_period,
                reporting_period_end_date=metric_period_end_date or reporting_period_end_date,
            )

            target_col = _pick_target_column(
                period_slot=metric_period_slot or fallback_period_slot,
                report_year=metric_report_year or fallback_year,
                force_ltm=spec.force_ltm,
                ltm_col=ltm_col,
                year_cols=year_cols,
                half_cols=half_cols,
                quarter_cols=quarter_cols,
            )
            write_value = value
            if resolved_unit == "млрд руб.":
                write_value = _normalize_bn_rub_value(write_value)
            existing = _to_float(_row_value(row, target_col))
            if existing is None:
                _set_cell(row, target_col, write_value)
            has_written_value = True

        if not has_written_value and not config.include_not_found_metrics:
            continue
        rows.append(row)

    return rows


def _collect_metric_candidates(result: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    metric_candidates_by_key: dict[str, list[dict[str, Any]]] = {}
    sources = [
        result.get("metrics"),
        result.get("comparative_metrics"),
    ]
    for source in sources:
        if not isinstance(source, list):
            continue
        for item in source:
            if not isinstance(item, dict):
                continue
            key = _as_non_empty_str(item.get("metric_key"))
            if not key:
                continue
            metric_candidates_by_key.setdefault(key, []).append(item)
    return metric_candidates_by_key


def _pick_unit_metric(metrics: list[dict[str, Any]]) -> dict[str, Any]:
    for metric in metrics:
        if bool(metric.get("found")) and _to_float(metric.get("value")) is not None:
            return metric
    return metrics[0]


def load_sheets_export_config(
    config_path: str | Path | None = None,
) -> tuple[SheetsExportConfig, Path | None, dict[str, Any]] | None:
    resolved_path = _resolve_config_path(config_path)
    if resolved_path is None or not resolved_path.exists():
        return None

    raw_payload = json.loads(resolved_path.read_text(encoding="utf-8"))
    if not isinstance(raw_payload, dict):
        raise ValueError("Google Sheets config must be a JSON object.")

    config = SheetsExportConfig(
        enabled=_to_bool(raw_payload.get("enabled"), default=True),
        credentials_json=_as_non_empty_str(raw_payload.get("credentials_json")),
        spreadsheet_id=_as_non_empty_str(raw_payload.get("spreadsheet_id")),
        spreadsheet_title=_as_non_empty_str(raw_payload.get("spreadsheet_title")) or "IFRS Parser Export",
        worksheet_name=_as_non_empty_str(raw_payload.get("worksheet_name")) or "Импорт",
        create_spreadsheet_if_missing=_to_bool(
            raw_payload.get("create_spreadsheet_if_missing"), default=True
        ),
        share_with=_to_str_list(raw_payload.get("share_with")),
        source_label=_as_non_empty_str(raw_payload.get("source_label")) or "МСФО",
        type_label=_as_non_empty_str(raw_payload.get("type_label")) or "Source",
        default_industry=_as_non_empty_str(raw_payload.get("default_industry")) or "",
        include_not_found_metrics=_to_bool(raw_payload.get("include_not_found_metrics"), default=False),
        ubo_by_company=_to_str_map(raw_payload.get("ubo_by_company")),
        ubo_unknown_value=_as_non_empty_str(raw_payload.get("ubo_unknown_value")) or "Не определен",
    )
    return config, resolved_path, raw_payload


def _resolve_config_path(config_path: str | Path | None) -> Path | None:
    if config_path:
        return Path(config_path).resolve()
    env_path = _as_non_empty_str(os.getenv("IFRS_SHEETS_CONFIG_PATH"))
    if env_path:
        return Path(env_path).resolve()
    if DEFAULT_CONFIG_PATH.exists():
        return DEFAULT_CONFIG_PATH.resolve()
    return None


def _persist_spreadsheet_id_if_needed(
    config_path: Path,
    raw_payload: dict[str, Any],
    spreadsheet_id: Any,
) -> None:
    sheet_id = _as_non_empty_str(spreadsheet_id)
    if not sheet_id:
        return
    current = _as_non_empty_str(raw_payload.get("spreadsheet_id"))
    if current == sheet_id:
        return
    raw_payload["spreadsheet_id"] = sheet_id
    config_path.write_text(
        json.dumps(raw_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _resolve_unit(metric: dict[str, Any], spec: MetricSheetSpec) -> str:
    if spec.unit_override:
        return spec.unit_override
    unit = _as_non_empty_str(metric.get("unit"))
    if unit is None:
        return "млрд руб."
    normalized = unit.lower()
    if "%" in normalized:
        return "%"
    if normalized in {"x", "х"}:
        return "x"
    if "rub bn" in normalized or "млрд" in normalized:
        return "млрд руб."
    return unit


def _extract_year(value: str) -> int | None:
    match = re.search(r"(20\d{2})", value)
    if not match:
        return None
    year = int(match.group(1))
    if 2000 <= year <= 2100:
        return year
    return None


def _set_cell(row: list[Any], col_index: int, value: Any) -> None:
    if col_index <= 0:
        return
    idx = col_index - 1
    if idx >= len(row):
        row.extend([""] * (idx + 1 - len(row)))
    row[idx] = value if value is not None else ""


def _as_non_empty_str(value: Any) -> str | None:
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return None


def _to_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "no", "n", "off"}:
            return False
    return default


def _to_float(value: Any) -> float | None:
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


def _to_str_list(value: Any) -> list[str]:
    if isinstance(value, list):
        result: list[str] = []
        for item in value:
            text = _as_non_empty_str(item)
            if text:
                result.append(text)
        return result
    return []


def _to_str_map(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    result: dict[str, str] = {}
    for key, raw_val in value.items():
        key_text = _as_non_empty_str(key)
        val_text = _as_non_empty_str(raw_val)
        if key_text and val_text:
            result[key_text] = val_text
    return result


def _normalize_lookup_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower().replace("ё", "е")
    text = re.sub(r"[^a-zа-я0-9]+", "", text)
    return text


def _resolve_ubo_surname(
    result: dict[str, Any],
    config: SheetsExportConfig,
    company_name: str,
) -> str | None:
    from_result = _as_non_empty_str(result.get("ubo_surname"))
    if from_result:
        return from_result

    normalized_company = _normalize_lookup_text(company_name)
    for raw_company, raw_ubo in config.ubo_by_company.items():
        if _normalize_lookup_text(raw_company) == normalized_company:
            resolved = _as_non_empty_str(raw_ubo)
            if resolved:
                return resolved

    return _as_non_empty_str(config.ubo_unknown_value)


def _sheet_row_key(row: list[Any]) -> tuple[str, ...] | None:
    company = _normalize_lookup_text(_row_value(row, 3))
    metric = _normalize_lookup_text(_row_value(row, 5))
    if not company or not metric:
        return None
    key = (
        company,
        _normalize_lookup_text(_row_value(row, 4)),
        metric,
        _normalize_lookup_text(_row_value(row, 6)),
        _normalize_lookup_text(_row_value(row, 7)),
        _normalize_lookup_text(_row_value(row, 8)),
        _normalize_lookup_text(_row_value(row, 9)),
    )
    return key


def _row_value(row: list[Any], col_idx: int) -> Any:
    if col_idx <= 0:
        return ""
    if len(row) < col_idx:
        return ""
    return row[col_idx - 1]


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def _cell_values_equal(left: Any, right: Any) -> bool:
    if _is_blank(left) and _is_blank(right):
        return True
    left_num = _to_float(left)
    right_num = _to_float(right)
    if left_num is not None and right_num is not None:
        return abs(left_num - right_num) < 1e-9
    return str(left).strip() == str(right).strip()


def _is_retryable_sheets_write_error(exc: Exception) -> bool:
    text = str(exc).lower()
    retryable_markers = [
        "429",
        "quota exceeded",
        "rate limit",
        "too many requests",
        "resource exhausted",
        "internal error",
        "backend error",
        "timed out",
    ]
    return any(marker in text for marker in retryable_markers)


def _sheets_retry_delay_seconds(attempt: int) -> float:
    base = 1.0
    max_delay = 45.0
    exp_delay = min(max_delay, base * (2**attempt))
    jitter = random.uniform(0.0, min(1.0, exp_delay * 0.25))
    return exp_delay + jitter


def _row_matches_template_header(current_row: list[Any], template_row: list[str]) -> bool:
    if not current_row:
        return False
    # Check key non-empty anchors only; allow extra trailing columns.
    anchors = {
        1: "Отрасль",
        2: "UBO",
        3: "Компания",
        4: "Type",
        5: "Показатель",
        9: "Ед.изм.",
        10: "LTM",
        12: "2025",
        17: "2H25",
        24: "3Q25",
        34: "1Q23",
    }
    for col, expected in anchors.items():
        actual = current_row[col - 1].strip() if len(current_row) >= col and isinstance(current_row[col - 1], str) else ""
        if actual != expected:
            return False
    return True


def _normalize_bn_rub_value(value: float) -> float:
    normalized = float(value)
    # Guard against accidental export in RUB, thousands, or millions instead of RUB bn.
    # Keep dividing by 1000 until value is in realistic bn range.
    for _ in range(5):
        if abs(normalized) <= 100000:
            break
        normalized /= 1000.0
    return round(normalized, 6)


def _column_letter(col_idx: int) -> str:
    if col_idx <= 0:
        return "A"
    result = []
    current = col_idx
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        result.append(chr(65 + remainder))
    return "".join(reversed(result))


def _parse_header_marker(value: Any) -> tuple[str, int, int] | tuple[str, int] | tuple[str] | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return ("year", value.year)

    text = str(value).strip()
    if not text:
        return None
    lowered = text.lower()

    if lowered == "ltm":
        return ("ltm",)

    year_match = re.fullmatch(r"(20\d{2})", lowered)
    if year_match:
        return ("year", int(year_match.group(1)))

    half_match = re.fullmatch(r"([12])\s*h\s*(\d{2,4})", lowered)
    if half_match:
        half_idx = int(half_match.group(1))
        year = _to_four_digit_year(half_match.group(2))
        if year is not None:
            return ("half", year, half_idx)

    quarter_match = re.fullmatch(r"([1-4])\s*q\s*(\d{2,4})", lowered)
    if quarter_match:
        quarter_idx = int(quarter_match.group(1))
        year = _to_four_digit_year(quarter_match.group(2))
        if year is not None:
            return ("quarter", year, quarter_idx)

    return None


def _to_four_digit_year(raw: str) -> int | None:
    digits = re.sub(r"[^0-9]", "", raw)
    if not digits:
        return None
    if len(digits) == 4:
        year = int(digits)
    elif len(digits) == 2:
        year = 2000 + int(digits)
    else:
        return None
    if 2000 <= year <= 2100:
        return year
    return None


def _resolve_period_slot(
    reporting_period: str | None,
    reporting_period_end_date: str | None,
) -> tuple[str, int] | tuple[str, int, int] | None:
    period_text = (reporting_period or "").strip().lower()
    end_date_text = (reporting_period_end_date or "").strip()

    end_year = _extract_year(end_date_text) or _extract_year(period_text or "")
    end_month = _extract_month_from_iso(end_date_text)

    explicit_quarter = _extract_explicit_quarter(period_text)
    if explicit_quarter is not None and end_year is not None:
        return ("quarter", end_year, explicit_quarter)

    explicit_half = _extract_explicit_half(period_text)
    if explicit_half is not None and end_year is not None:
        return ("half", end_year, explicit_half)

    months_span = _extract_explicit_month_span(period_text)
    if months_span is None:
        months_span = _infer_month_span_from_end_month(end_month)

    if end_year is None:
        return None

    if months_span == 12:
        return ("year", end_year)
    if months_span == 6:
        if end_month == 12:
            return ("half", end_year, 2)
        return ("half", end_year, 1)
    if months_span == 3:
        quarter = _quarter_from_month(end_month)
        if quarter is None:
            return None
        return ("quarter", end_year, quarter)
    if months_span == 9:
        quarter = _quarter_from_month(end_month) or 3
        return ("quarter", end_year, quarter)

    return None


def _pick_target_column(
    period_slot: tuple[str, int] | tuple[str, int, int] | None,
    report_year: int | None,
    force_ltm: bool,
    ltm_col: int,
    year_cols: dict[int, int],
    half_cols: dict[tuple[int, int], int],
    quarter_cols: dict[tuple[int, int], int],
) -> int:
    if force_ltm:
        return ltm_col

    if period_slot is not None:
        slot_type = period_slot[0]
        if slot_type == "year":
            year = period_slot[1]
            if year in year_cols:
                return year_cols[year]
        if slot_type == "half":
            year, half_index = period_slot[1], period_slot[2]
            if (year, half_index) in half_cols:
                return half_cols[(year, half_index)]
        if slot_type == "quarter":
            year, quarter_index = period_slot[1], period_slot[2]
            if (year, quarter_index) in quarter_cols:
                return quarter_cols[(year, quarter_index)]
            if year in year_cols and quarter_index == 4:
                return year_cols[year]

    if report_year is not None and report_year in year_cols:
        return year_cols[report_year]
    return ltm_col


def _extract_month_from_iso(value: str) -> int | None:
    match = re.search(r"\b20\d{2}-(\d{2})-(\d{2})\b", value)
    if not match:
        return None
    month = int(match.group(1))
    if 1 <= month <= 12:
        return month
    return None


def _quarter_from_month(month: int | None) -> int | None:
    if month is None:
        return None
    if month <= 3:
        return 1
    if month <= 6:
        return 2
    if month <= 9:
        return 3
    return 4


def _extract_explicit_quarter(text: str) -> int | None:
    match = re.search(r"\b([1-4])\s*(?:q|кв)\b", text)
    if match:
        return int(match.group(1))
    return None


def _extract_explicit_half(text: str) -> int | None:
    match = re.search(r"\b([12])\s*h\b", text)
    if match:
        return int(match.group(1))
    if re.search(r"\b1\s*(?:пг|полугод)", text):
        return 1
    if re.search(r"\b2\s*(?:пг|полугод)", text):
        return 2
    return None


def _extract_explicit_month_span(text: str) -> int | None:
    patterns = [
        r"\b(3|6|9|12)\s*m\b",
        r"\b(3|6|9|12)\s*м\b",
        r"\b(3|6|9|12)\s*мес",
        r"\bза\s*(3|6|9|12)\s*ме",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return int(match.group(1))
    if re.search(r"\bfy\b", text) or re.search(r"\bannual\b", text):
        return 12
    return None


def _infer_month_span_from_end_month(month: int | None) -> int | None:
    if month is None:
        return None
    if month in {3, 6, 9, 12}:
        return month
    return None

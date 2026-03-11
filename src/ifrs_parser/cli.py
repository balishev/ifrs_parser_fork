from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .metrics import load_metrics
from .parser import GoogleIFRSPdfParser, IFRSParserConfig
from .sheets_export import (
    append_bank_debt_result_to_google_sheets,
    append_result_to_google_sheets,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ifrs-parser",
        description="Extract IFRS financial data using Google AI API.",
    )
    parser.add_argument(
        "--mode",
        choices=("metrics", "bank-debt-notes"),
        default="metrics",
        help="Parser mode: metrics (default) or bank-debt-notes.",
    )
    parser.add_argument("--pdf", help="Path to IFRS PDF document.")
    parser.add_argument(
        "--images-text-file",
        help="Path to UTF-8 text file with OCR/image-extracted report text (optional fallback for mode=bank-debt-notes).",
    )
    parser.add_argument(
        "--rep-year",
        help="Optional year filter in YYYY (if omitted, period is detected from report).",
    )
    parser.add_argument(
        "--out",
        default="output/ifrs_metrics.json",
        help="Path to output JSON file.",
    )
    parser.add_argument(
        "--metrics-config",
        help="Path to JSON config with metric definitions. If omitted, default metrics are used.",
    )
    parser.add_argument(
        "--model",
        default="gemini-2.5-flash",
        help="Gemini model name.",
    )
    parser.add_argument(
        "--period-hint",
        help="Optional hint for period selection (for example: FY2025 or 2025-12-31).",
    )
    parser.add_argument(
        "--api-key",
        help="Google API key. If omitted, GOOGLE_API_KEY or GEMINI_API_KEY env var is used.",
    )
    parser.add_argument(
        "--credentials-json",
        help="Path to Google service account JSON for Vertex AI mode.",
    )
    parser.add_argument(
        "--project",
        help="Google Cloud project id (optional if present in service account JSON).",
    )
    parser.add_argument(
        "--location",
        default="us-central1",
        help="Vertex AI location (default: us-central1).",
    )
    parser.add_argument(
        "--timeout-sec",
        type=int,
        default=300,
        help="Timeout for uploaded file processing in seconds.",
    )
    parser.add_argument(
        "--keep-uploaded-file",
        action="store_true",
        help="Do not delete uploaded PDF from Google Files API after parsing.",
    )
    parser.add_argument(
        "--sheets-config",
        help="Optional path to Google Sheets export config JSON. If set, parsed result is appended to Google Sheets.",
    )
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    config = IFRSParserConfig(
        model=args.model,
        location=args.location,
        timeout_sec=args.timeout_sec,
        keep_uploaded_file=args.keep_uploaded_file,
    )
    parser = GoogleIFRSPdfParser(
        api_key=args.api_key,
        credentials_json=args.credentials_json,
        project=args.project,
        config=config,
    )

    if args.mode == "metrics":
        if not args.pdf:
            raise ValueError("--pdf is required for mode=metrics.")
        metrics = load_metrics(args.metrics_config)
        result = parser.extract_metrics(
            pdf_path=args.pdf,
            metrics=metrics,
            period_hint=args.period_hint,
        )
    else:
        if args.pdf:
            result = parser.extract_bank_debt_notes_from_pdf(
                pdf_path=args.pdf,
                rep_year=args.rep_year,
                period_hint=args.period_hint,
            )
        elif args.images_text_file:
            text_path = Path(args.images_text_file)
            if not text_path.exists():
                raise FileNotFoundError(f"images text file not found: {text_path}")
            images_text = text_path.read_text(encoding="utf-8")
            result = parser.extract_bank_debt_notes_from_images_text(
                images_text=images_text,
                rep_year=args.rep_year,
            )
        else:
            raise ValueError(
                "For mode=bank-debt-notes provide either --pdf or --images-text-file."
            )

    output_path = Path(args.out)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.mode == "metrics":
        try:
            sheets_summary = append_result_to_google_sheets(result, args.sheets_config)
        except Exception as exc:
            print(f"Warning: failed to append to Google Sheets: {exc}", file=sys.stderr)
        else:
            if sheets_summary is not None:
                status = sheets_summary.get("status")
                if status == "ok":
                    print(
                        "Appended to Google Sheets: "
                        f"{sheets_summary.get('spreadsheet_url')} "
                        f"(rows: {sheets_summary.get('appended_rows')})"
                    )
                else:
                    print(f"Google Sheets export status: {status}")
    else:
        try:
            sheets_summary = append_bank_debt_result_to_google_sheets(result, args.sheets_config)
        except Exception as exc:
            print(f"Warning: failed to append bank-debt result to Google Sheets: {exc}", file=sys.stderr)
        else:
            if sheets_summary is not None:
                status = sheets_summary.get("status")
                if status == "ok":
                    print(
                        "Appended bank-debt analysis to Google Sheets: "
                        f"{sheets_summary.get('spreadsheet_url')} "
                        f"(written: {sheets_summary.get('written_rows')})"
                    )
                else:
                    print(f"Google Sheets bank-debt export status: {status}")
        markdown_table = result.get("markdown_table")
        if isinstance(markdown_table, str) and markdown_table.strip():
            md_path = output_path.with_suffix(".md")
            md_path.write_text(markdown_table, encoding="utf-8")
            print(f"Saved markdown table to {md_path}")

    if args.mode == "metrics":
        print(f"Saved parsed metrics to {output_path}")
    else:
        print(f"Saved bank-debt analysis to {output_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)

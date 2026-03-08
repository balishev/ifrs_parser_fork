from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .metrics import load_metrics
from .parser import GoogleIFRSPdfParser, IFRSParserConfig


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ifrs-parser",
        description="Extract IFRS financial metrics from a PDF using Google AI API.",
    )
    parser.add_argument("--pdf", required=True, help="Path to IFRS PDF document.")
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
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()

    metrics = load_metrics(args.metrics_config)
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
    result = parser.extract_metrics(
        pdf_path=args.pdf,
        metrics=metrics,
        period_hint=args.period_hint,
    )

    output_path = Path(args.out)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Saved parsed metrics to {output_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)

from __future__ import annotations

import argparse
import json
import sys

from .sheets_export import initialize_google_sheet


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ifrs-sheets-init",
        description="Initialize Google Sheets target for IFRS parser export.",
    )
    parser.add_argument(
        "--config",
        default="config/sheets_export.json",
        help="Path to Google Sheets export config JSON.",
    )
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    info = initialize_google_sheet(args.config)
    print(json.dumps(info, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict

from .validator import ValidationError, validate_dataset, _load_rows  # noqa: SLF001 (local helper import)


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValidationError(f"File not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValidationError(f"Invalid JSON in {path}: {exc}") from exc


def _parse_as_of(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValidationError(f"Invalid --as-of date '{value}', expected YYYY-MM-DD") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate a feature dataset against a contract")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser("validate", help="Validate a CSV dataset")
    validate_parser.add_argument("--contract", required=True, type=Path, help="Path to contract JSON")
    validate_parser.add_argument("--data", required=True, type=Path, help="Path to CSV file")
    validate_parser.add_argument("--as-of", help="As-of date for freshness checks (YYYY-MM-DD)")
    validate_parser.add_argument("--report", type=Path, help="Write structured JSON report to this path")
    validate_parser.add_argument("--format", choices=["summary", "json"], default="summary", help="Stdout output format")

    return parser


def _render_summary(report: Dict[str, Any]) -> str:
    lines = [
        f"Dataset: {report['dataset_name']}",
        f"Decision: {'PASS' if report['passed'] else 'FAIL'} ({report['summary']['passed']}/{report['summary']['total']} checks passed)",
        "",
    ]
    for check in report["checks"]:
        status = "PASS" if check["passed"] else "FAIL"
        lines.append(f"[{status}] {check['name']}: {check['detail']}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "validate":
            contract = _read_json(args.contract)
            rows = _load_rows(args.data)
            as_of = _parse_as_of(args.as_of)
            report_obj = validate_dataset(contract, rows, as_of=as_of)
            payload = report_obj.to_dict()

            if args.report:
                args.report.parent.mkdir(parents=True, exist_ok=True)
                args.report.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

            if args.format == "json":
                print(json.dumps(payload, indent=2))
            else:
                print(_render_summary(payload))

            return 0 if payload["passed"] else 2

        parser.error(f"Unsupported command: {args.command}")
        return 1
    except ValidationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

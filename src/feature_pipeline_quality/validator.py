from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


class ValidationError(ValueError):
    """Raised for invalid contracts or runtime inputs."""


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str
    stats: Dict[str, Any]


@dataclass
class ValidationReport:
    dataset_name: str
    passed: bool
    summary: Dict[str, int]
    checks: List[CheckResult]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dataset_name": self.dataset_name,
            "passed": self.passed,
            "summary": self.summary,
            "checks": [asdict(c) for c in self.checks],
        }


def _is_null(value: str) -> bool:
    return value is None or value.strip() == ""


def _parse_typed(value: str, type_name: str) -> Tuple[bool, Optional[Any]]:
    if _is_null(value):
        return True, None

    text = value.strip()
    try:
        if type_name == "str":
            return True, text
        if type_name == "int":
            return True, int(text)
        if type_name == "float":
            return True, float(text)
        if type_name == "bool":
            lowered = text.lower()
            if lowered in {"true", "1", "yes", "y"}:
                return True, True
            if lowered in {"false", "0", "no", "n"}:
                return True, False
            return False, None
        if type_name == "date":
            return True, date.fromisoformat(text)
        if type_name == "datetime":
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            return True, datetime.fromisoformat(text)
    except ValueError:
        return False, None

    raise ValidationError(f"Unsupported type in contract: {type_name}")


def _load_rows(csv_path: Path) -> List[Dict[str, str]]:
    try:
        with csv_path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames is None:
                raise ValidationError("CSV has no header row")
            return [dict(row) for row in reader]
    except FileNotFoundError as exc:
        raise ValidationError(f"CSV file not found: {csv_path}") from exc


def _require_object(contract: Dict[str, Any], key: str) -> Dict[str, Any]:
    value = contract.get(key)
    if not isinstance(value, dict):
        raise ValidationError(f"Contract field '{key}' must be an object")
    return value


def validate_dataset(contract: Dict[str, Any], rows: List[Dict[str, str]], as_of: Optional[date] = None) -> ValidationReport:
    if not isinstance(contract, dict):
        raise ValidationError("Contract must be a JSON object")

    dataset_name = str(contract.get("dataset_name") or "unnamed-dataset")
    columns_contract = _require_object(contract, "columns")
    as_of_date = as_of or date.today()

    checks: List[CheckResult] = []

    header_columns = set(rows[0].keys()) if rows else set(columns_contract.keys())
    expected_columns = set(columns_contract.keys())
    missing_columns = sorted(expected_columns - header_columns)
    schema_ok = len(missing_columns) == 0
    checks.append(
        CheckResult(
            name="schema.columns_present",
            passed=schema_ok,
            detail="All expected columns present" if schema_ok else f"Missing columns: {', '.join(missing_columns)}",
            stats={"missing_columns": missing_columns, "expected_count": len(expected_columns), "actual_count": len(header_columns)},
        )
    )

    min_rows = int(contract.get("min_rows", 0))
    row_count_ok = len(rows) >= min_rows
    checks.append(
        CheckResult(
            name="row_count.minimum",
            passed=row_count_ok,
            detail=f"Rows {len(rows)} >= min_rows {min_rows}",
            stats={"rows": len(rows), "min_rows": min_rows},
        )
    )

    # Column-level checks
    parsed_cache: Dict[str, List[Tuple[bool, Optional[Any]]]] = {}
    for column_name, spec_any in columns_contract.items():
        if not isinstance(spec_any, dict):
            raise ValidationError(f"Column contract for '{column_name}' must be an object")
        if column_name not in header_columns:
            continue

        spec = spec_any
        type_name = str(spec.get("type", "str"))
        required = bool(spec.get("required", False))
        null_ratio_max = float(spec.get("null_ratio_max", 1.0))

        raw_values = [row.get(column_name, "") for row in rows]
        null_count = sum(1 for v in raw_values if _is_null(v))
        null_ratio = (null_count / len(raw_values)) if raw_values else 0.0

        parsed_values = [_parse_typed(v, type_name) for v in raw_values]
        parsed_cache[column_name] = parsed_values
        type_error_count = sum(1 for (ok, parsed) in parsed_values if not ok and parsed is None)
        if required:
            # required + empty rows count as violations via null ratio (default 0.0 recommended)
            pass

        column_ok = null_ratio <= null_ratio_max and type_error_count == 0
        checks.append(
            CheckResult(
                name=f"column.{column_name}",
                passed=column_ok,
                detail=(
                    f"null_ratio={null_ratio:.3f} (max {null_ratio_max:.3f}), type_errors={type_error_count}"
                ),
                stats={
                    "column": column_name,
                    "type": type_name,
                    "rows": len(raw_values),
                    "null_count": null_count,
                    "null_ratio": round(null_ratio, 6),
                    "null_ratio_max": null_ratio_max,
                    "type_error_count": type_error_count,
                    "required": required,
                },
            )
        )

    unique_key = contract.get("unique_key") or []
    if unique_key:
        if not isinstance(unique_key, list) or not all(isinstance(x, str) for x in unique_key):
            raise ValidationError("Contract field 'unique_key' must be an array of strings")
        missing_key_cols = [c for c in unique_key if c not in header_columns]
        if missing_key_cols:
            checks.append(
                CheckResult(
                    name="unique_key.duplicates",
                    passed=False,
                    detail=f"Unique key columns missing: {', '.join(missing_key_cols)}",
                    stats={"unique_key": unique_key, "duplicate_rows": None},
                )
            )
        else:
            seen = set()
            duplicates = 0
            for row in rows:
                key = tuple(row.get(col, "") for col in unique_key)
                if key in seen:
                    duplicates += 1
                else:
                    seen.add(key)
            checks.append(
                CheckResult(
                    name="unique_key.duplicates",
                    passed=duplicates == 0,
                    detail=f"duplicate_rows={duplicates}",
                    stats={"unique_key": unique_key, "duplicate_rows": duplicates},
                )
            )

    freshness = contract.get("freshness")
    if freshness is not None:
        if not isinstance(freshness, dict):
            raise ValidationError("Contract field 'freshness' must be an object")
        column = str(freshness.get("column") or "")
        if not column:
            raise ValidationError("Freshness config requires 'column'")
        if column not in header_columns:
            checks.append(
                CheckResult(
                    name="freshness.max_age",
                    passed=False,
                    detail=f"Freshness column missing: {column}",
                    stats={"column": column},
                )
            )
        else:
            max_age_days = int(freshness.get("max_age_days", 0))
            parsed_values = parsed_cache.get(column) or [_parse_typed(row.get(column, ""), "date") for row in rows]
            valid_dates = [parsed for ok, parsed in parsed_values if ok and parsed is not None]
            if not valid_dates:
                checks.append(
                    CheckResult(
                        name="freshness.max_age",
                        passed=False,
                        detail=f"No parseable freshness values for column '{column}'",
                        stats={"column": column, "max_age_days": max_age_days},
                    )
                )
            else:
                normalized_dates = [v.date() if isinstance(v, datetime) else v for v in valid_dates]
                newest = max(normalized_dates)
                age_days = (as_of_date - newest).days
                passed = age_days <= max_age_days
                checks.append(
                    CheckResult(
                        name="freshness.max_age",
                        passed=passed,
                        detail=f"newest={newest.isoformat()} age_days={age_days} max_age_days={max_age_days}",
                        stats={
                            "column": column,
                            "newest": newest.isoformat(),
                            "as_of": as_of_date.isoformat(),
                            "age_days": age_days,
                            "max_age_days": max_age_days,
                        },
                    )
                )

    passed_count = sum(1 for c in checks if c.passed)
    failed_count = len(checks) - passed_count
    return ValidationReport(
        dataset_name=dataset_name,
        passed=failed_count == 0,
        summary={"total": len(checks), "passed": passed_count, "failed": failed_count},
        checks=checks,
    )

import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from feature_pipeline_quality.cli import main
from feature_pipeline_quality.validator import _load_rows, validate_dataset


class ValidatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = json.loads((ROOT / "examples" / "contract.json").read_text(encoding="utf-8"))

    def test_good_dataset_passes(self) -> None:
        rows = _load_rows(ROOT / "examples" / "features_good.csv")
        report = validate_dataset(self.contract, rows)
        self.assertTrue(report.passed)
        self.assertEqual(report.summary["failed"], 0)

    def test_bad_dataset_fails(self) -> None:
        rows = _load_rows(ROOT / "examples" / "features_bad.csv")
        report = validate_dataset(self.contract, rows)
        self.assertFalse(report.passed)
        self.assertGreater(report.summary["failed"], 0)


class CliTests(unittest.TestCase):
    def test_cli_report_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report_path = Path(tmp) / "report.json"
            stdout = StringIO()
            with redirect_stdout(stdout):
                exit_code = main([
                    "validate",
                    "--contract",
                    str(ROOT / "examples" / "contract.json"),
                    "--data",
                    str(ROOT / "examples" / "features_good.csv"),
                    "--as-of",
                    "2026-02-25",
                    "--report",
                    str(report_path),
                    "--format",
                    "json",
                ])
            self.assertEqual(exit_code, 0)
            payload = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertTrue(payload["passed"])


if __name__ == "__main__":
    unittest.main()

# dbt_lens/utils/io_utils.py

import json
from pathlib import Path

from dbt_lens.core.coverage import CoverageReport, CoverageType

def read_coverage_report(path: Path) -> CoverageReport:
    with open(path) as f:
        report_json = json.load(f)
    return CoverageReport.from_dict(report_json, CoverageType(report_json["cov_type"]))

def write_coverage_report(coverage_report: CoverageReport, path: Path) -> None:
    with open(path, "w") as f:
        f.write(coverage_report.to_json())

def fail_under(coverage_report, min_coverage: float) -> None:
    if coverage_report.coverage < min_coverage:
        raise RuntimeError(
            f"Measured coverage {coverage_report.coverage:.3f} lower than min required {min_coverage}"
        )

def fail_compare(coverage_report, compare_report: CoverageReport) -> None:
    from dbt_lens.core.diff import compare_reports
    diff = compare_reports(coverage_report, compare_report)
    if diff.after.coverage < diff.before.coverage:
        raise RuntimeError(
            f"Coverage decreased from {diff.before.coverage:.2%} to {diff.after.coverage:.2%}"
        )

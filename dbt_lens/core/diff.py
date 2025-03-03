# dbt_lens/core/diff.py

from dbt_lens.core.coverage import CoverageReport, CoverageDiff

def compare_reports(after_report: CoverageReport, before_report: CoverageReport) -> CoverageDiff:
    diff = CoverageDiff(before_report, after_report)
    print(diff.summary())
    print(diff.new_misses_summary())
    return diff

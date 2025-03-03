# dbt_lens/cli.py

import logging
from pathlib import Path
from typing import List, Optional

import typer

from dbt_lens.core.manifest import load_manifest
from dbt_lens.core.catalog import load_catalog
from dbt_lens.core.coverage import CoverageType, CoverageFormat
from dbt_lens.core.diff import compare_reports
from dbt_lens.utils.io_utils import write_coverage_report, read_coverage_report, fail_under, fail_compare
from dbt_lens.utils.git_utils import get_file_from_commit

app = typer.Typer(help="Compute test coverage for dbt-managed projects.")


def do_compute(
    project_dir: Path,
    run_artifacts_dir: Optional[Path],
    cov_report: Path,
    cov_type: CoverageType,
    cov_fail_under: Optional[float],
    cov_fail_compare: Optional[Path],
    model_path_filter: Optional[List[str]],
    cov_format: CoverageFormat,
):
    # Load manifest and catalog using our modules.
    manifest = load_manifest(project_dir, run_artifacts_dir)
    catalog = load_catalog(project_dir, run_artifacts_dir, manifest)

    if model_path_filter:
        catalog = catalog.filter_tables(model_path_filter)
        if not catalog.tables:
            raise ValueError("No tables after filtering. Check your model_path_filter.")

    # Compute coverage (internally calls CoverageReport.from_catalog)
    from dbt_lens.core.coverage import compute_coverage
    coverage_report = compute_coverage(catalog, cov_type)

    if cov_format == CoverageFormat.MARKDOWN_TABLE:
        print(coverage_report.to_markdown_table())
    else:
        print(coverage_report.to_formatted_string())

    write_coverage_report(coverage_report, cov_report)

    if cov_fail_under is not None:
        fail_under(coverage_report, cov_fail_under)

    if cov_fail_compare is not None:
        compare_report = read_coverage_report(cov_fail_compare)
        fail_compare(coverage_report, compare_report)

    return coverage_report


@app.command()
def compute(
    project_dir: Path = typer.Option(".", help="dbt project directory path."),
    run_artifacts_dir: Optional[Path] = typer.Option(None, help="Custom directory for catalog and manifest."),
    cov_report: Path = typer.Option("coverage.json", help="Output coverage report path."),
    cov_type: CoverageType = typer.Argument(..., help="Type of test coverage to compute."),
    cov_fail_under: Optional[float] = typer.Option(None, help="Fail if coverage is below threshold."),
    cov_fail_compare: Optional[Path] = typer.Option(None, help="Path to a coverage report to compare with."),
    model_path_filter: Optional[List[str]] = typer.Option(None, help="Model path filter strings."),
    cov_format: CoverageFormat = typer.Option(CoverageFormat.STRING_TABLE, help="Output format: string or markdown."),
):
    """Compute test coverage for a dbt project."""
    return do_compute(project_dir, run_artifacts_dir, cov_report, cov_type, cov_fail_under, cov_fail_compare, model_path_filter, cov_format)


@app.command()
def compare(
    report: Path = typer.Argument(..., help="Path to current coverage report."),
    compare_report: Path = typer.Argument(..., help="Path to previous coverage report."),
):
    """Compare two coverage reports."""
    current_report = read_coverage_report(report)
    previous_report = read_coverage_report(compare_report)
    diff = compare_reports(current_report, previous_report)
    return diff


@app.command()
def compare_git(
    report: Path = typer.Option("coverage.json", help="Path to current coverage report."),
    commit: str = typer.Argument(..., help="Git commit hash or reference for comparison."),
):
    """
    Compare current coverage report with the version from a given Git commit.
    
    This command retrieves the old coverage report (assumed to be tracked in Git)
    from the specified commit and then compares it to the current report.
    """
    # Retrieve file content from the given commit.
    old_report_content = get_file_from_commit(commit, report)
    import json
    from tempfile import NamedTemporaryFile
    # Write the old report content to a temporary file.
    with NamedTemporaryFile("w+", delete=False) as tmp_file:
        tmp_file.write(old_report_content)
        tmp_file_path = Path(tmp_file.name)
    previous_report = read_coverage_report(tmp_file_path)
    current_report = read_coverage_report(report)
    diff = compare_reports(current_report, previous_report)
    return diff


@app.callback()
def main(verbose: bool = typer.Option(False, help="Enable verbose logging.")):
    logging.basicConfig(level=logging.INFO if verbose else logging.WARNING)

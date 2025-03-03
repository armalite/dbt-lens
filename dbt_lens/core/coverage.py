# dbt_lens/core/coverage.py

import io
import json
import logging
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Dict, Optional, Set

from dbt_lens.core.catalog import Column, Catalog, Table
from dbt_lens.core.manifest import Manifest


class CoverageType(Enum):
    DOC = "doc"
    TEST = "test"


class CoverageFormat(str, Enum):
    STRING_TABLE = "string"
    MARKDOWN_TABLE = "markdown"


@dataclass
class CoverageReport:
    class EntityType(Enum):
        CATALOG = "catalog"
        TABLE = "table"
        COLUMN = "column"

    @dataclass(frozen=True)
    class ColumnRef:
        table_name: Optional[str]
        column_name: str

    entity_type: EntityType
    cov_type: CoverageType
    entity_name: Optional[str]
    covered: Set[ColumnRef]
    total: Set[ColumnRef]
    subentities: Dict[str, CoverageReport] = field(default_factory=dict)
    misses: Set[ColumnRef] = field(init=False)
    coverage: float = field(init=False)

    def __post_init__(self):
        if self.covered is not None and self.total:
            self.misses = self.total - self.covered
            self.coverage = len(self.covered) / len(self.total)
        else:
            self.misses = set()
            self.coverage = 0.0

    @classmethod
    def from_catalog(cls, catalog: Catalog, cov_type: CoverageType) -> CoverageReport:
        subentities = {
            table.name: CoverageReport.from_table(table, cov_type)
            for table in catalog.tables.values()
        }
        covered = {col for table_report in subentities.values() for col in table_report.covered}
        total = {col for table_report in subentities.values() for col in table_report.total}
        return CoverageReport(cls.EntityType.CATALOG, cov_type, None, covered, total, subentities)

    @classmethod
    def from_table(cls, table: Table, cov_type: CoverageType) -> CoverageReport:
        subentities = {
            col.name: CoverageReport.from_column(col, cov_type) for col in table.columns.values()
        }
        covered = {replace(col, table_name=table.name)
                   for col_report in subentities.values() for col in col_report.covered}
        total = {replace(col, table_name=table.name)
                 for col_report in subentities.values() for col in col_report.total}
        return CoverageReport(cls.EntityType.TABLE, cov_type, table.name, covered, total, subentities)

    @classmethod
    def from_column(cls, column: Column, cov_type: CoverageType) -> CoverageReport:
        if cov_type == CoverageType.DOC:
            is_covered = column.doc
        elif cov_type == CoverageType.TEST:
            is_covered = column.test
        else:
            raise ValueError(f"Unsupported cov_type {cov_type}")
        covered = {CoverageReport.ColumnRef(None, column.name)} if is_covered else set()
        total = {CoverageReport.ColumnRef(None, column.name)}
        return CoverageReport(cls.EntityType.COLUMN, cov_type, column.name, covered, total, {})

    def to_markdown_table(self) -> str:
        if self.entity_type == CoverageReport.EntityType.TABLE:
            return (
                f"| {self.entity_name:70} | {len(self.covered):5}/{len(self.total):<5} | "
                f"{self.coverage * 100:5.1f}% |"
            )
        elif self.entity_type == CoverageReport.EntityType.CATALOG:
            buf = io.StringIO()
            buf.write(f"# Coverage report ({self.cov_type.value})\n")
            buf.write("| Model | Columns Covered | % |\n")
            buf.write("|:------|----------------:|:-:|\n")
            for _, table_cov in sorted(self.subentities.items()):
                buf.write(table_cov.to_markdown_table() + "\n")
            buf.write(
                f"| {'Total':70} | {len(self.covered):5}/{len(self.total):<5} | "
                f"{self.coverage * 100:5.1f}% |\n"
            )
            return buf.getvalue()
        else:
            raise TypeError(f"Unsupported entity type for markdown output: {self.entity_type}")

    def to_formatted_string(self) -> str:
        if self.entity_type == CoverageReport.EntityType.TABLE:
            return (
                f"{self.entity_name:50} {len(self.covered):5}/{len(self.total):<5} "
                f"{self.coverage * 100:5.1f}%"
            )
        elif self.entity_type == CoverageReport.EntityType.CATALOG:
            buf = io.StringIO()
            buf.write(f"Coverage report ({self.cov_type.value})\n")
            buf.write("=" * 69 + "\n")
            for _, table_cov in sorted(self.subentities.items()):
                buf.write(table_cov.to_formatted_string() + "\n")
            buf.write("=" * 69 + "\n")
            buf.write(
                f"{'Total':50} {len(self.covered):5}/{len(self.total):<5} "
                f"{self.coverage * 100:5.1f}%\n"
            )
            return buf.getvalue()
        else:
            raise TypeError(f"Unsupported entity type for formatted string output: {self.entity_type}")

    def to_dict(self) -> dict:
        if self.entity_type == CoverageReport.EntityType.COLUMN:
            return {
                "name": self.entity_name,
                "covered": len(self.covered),
                "total": len(self.total),
                "coverage": self.coverage,
            }
        elif self.entity_type == CoverageReport.EntityType.TABLE:
            return {
                "name": self.entity_name,
                "covered": len(self.covered),
                "total": len(self.total),
                "coverage": self.coverage,
                "columns": [col_report.to_dict() for col_report in self.subentities.values()],
            }
        elif self.entity_type == CoverageReport.EntityType.CATALOG:
            return {
                "cov_type": self.cov_type.value,
                "covered": len(self.covered),
                "total": len(self.total),
                "coverage": self.coverage,
                "tables": [table_report.to_dict() for table_report in self.subentities.values()],
            }
        else:
            raise TypeError("Unsupported entity type for dict conversion.")

    @staticmethod
    def from_dict(report: dict, cov_type: CoverageType) -> CoverageReport:
        if "tables" in report:
            subentities = {
                table_report["name"]: CoverageReport.from_dict(table_report, cov_type)
                for table_report in report["tables"]
            }
            covered = {col for tbl in subentities.values() for col in tbl.covered}
            total = {col for tbl in subentities.values() for col in tbl.total}
            return CoverageReport(CoverageReport.EntityType.CATALOG, cov_type, None, covered, total, subentities)
        elif "columns" in report:
            table_name = report["name"]
            subentities = {
                col_report["name"]: CoverageReport.from_dict(col_report, cov_type)
                for col_report in report["columns"]
            }
            covered = {replace(col, table_name=table_name) for tbl in subentities.values() for col in tbl.covered}
            total = {replace(col, table_name=table_name) for tbl in subentities.values() for col in tbl.total}
            return CoverageReport(CoverageReport.EntityType.TABLE, cov_type, table_name, covered, total, subentities)
        else:
            column_name = report["name"]
            return CoverageReport(
                CoverageReport.EntityType.COLUMN,
                cov_type,
                column_name,
                {CoverageReport.ColumnRef(None, column_name)} if report["covered"] > 0 else set(),
                {CoverageReport.ColumnRef(None, column_name)},
                {}
            )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


@dataclass
class CoverageDiff:
    before: Optional[CoverageReport]
    after: CoverageReport
    new_misses: Dict[str, CoverageDiff] = field(init=False)

    def __post_init__(self):
        if self.before is not None:
            if self.before.cov_type != self.after.cov_type:
                raise ValueError("Coverage types do not match")
            if self.before.entity_type != self.after.entity_type:
                raise ValueError("Entity types do not match")
        self.new_misses = self.find_new_misses()

    def find_new_misses(self) -> Dict[str, CoverageDiff]:
        if self.after.entity_type == CoverageReport.EntityType.COLUMN:
            return {}
        new_misses = self.after.misses - (self.before.misses if self.before is not None else set())
        res = {}
        for new_miss in new_misses:
            key = new_miss.table_name if self.after.entity_type == CoverageReport.EntityType.CATALOG else new_miss.column_name
            before_entity = self.before.subentities.get(key) if self.before else None
            after_entity = self.after.subentities[key]
            res[key] = CoverageDiff(before_entity, after_entity)
        return res

    def summary(self) -> str:
        if self.after.entity_type != CoverageReport.EntityType.CATALOG:
            raise TypeError("Summary only supported for catalog level diff.")
        buf = io.StringIO()
        buf.write(f"{'':10}{'before':>10}{'after':>10}{'+/-':>15}\n")
        buf.write("=" * 45 + "\n")
        buf.write(
            f"{'Coverage':10}{self.before.coverage:10.2%}{self.after.coverage:10.2%}"
            f"{(self.after.coverage - self.before.coverage):+15.2%}\n"
        )
        buf.write("=" * 45 + "\n")
        add_del_tables = (
            f"{len(set(self.after.subentities) - set(self.before.subentities)):+d}/"
            f"{-len(set(self.before.subentities) - set(self.after.subentities)):+d}"
        )
        buf.write(
            f"{'Tables':10}{len(self.before.subentities):10d}"
            f"{len(self.after.subentities):10d}"
            f"{add_del_tables:>15}\n"
        )
        add_del_columns = (
            f"{len(self.after.total - self.before.total):+d}/"
            f"{-len(self.before.total - self.after.total):+d}"
        )
        buf.write(
            f"{'Columns':10}{len(self.before.total):10d}{len(self.after.total):10d}"
            f"{add_del_columns:>15}\n"
        )
        add_del_hits = (
            f"{len(self.after.covered - self.before.covered):+d}/"
            f"{-len(self.before.covered - self.after.covered):+d}"
        )
        buf.write(
            f"{'Hits':10}{len(self.before.covered):10d}{len(self.after.covered):10d}"
            f"{add_del_hits:>15}\n"
        )
        add_del_misses = (
            f"{len(self.after.misses - self.before.misses):+d}/"
            f"{-len(self.before.misses - self.after.misses):+d}"
        )
        buf.write(
            f"{'Misses':10}{len(self.before.misses):10d}{len(self.after.misses):10d}"
            f"{add_del_misses:>15}\n"
        )
        buf.write("=" * 45 + "\n")
        return buf.getvalue()

    def new_misses_summary(self) -> str:
        if self.after.entity_type == CoverageReport.EntityType.COLUMN:
            return self._new_miss_summary_row()
        elif self.after.entity_type == CoverageReport.EntityType.TABLE:
            buf = io.StringIO()
            buf.write(self._new_miss_summary_row())
            for col in self.new_misses.values():
                buf.write(col.new_misses_summary())
            return buf.getvalue()
        elif self.after.entity_type == CoverageReport.EntityType.CATALOG:
            buf = io.StringIO()
            buf.write("=" * 94 + "\n")
            buf.write(self._new_miss_summary_row())
            buf.write("=" * 94 + "\n")
            for table in self.new_misses.values():
                buf.write(table.new_misses_summary())
                buf.write("=" * 94 + "\n")
            return buf.getvalue()
        else:
            raise TypeError("Unsupported entity type for new misses summary.")

    def _new_miss_summary_row(self) -> str:
        if self.after.entity_type == CoverageReport.EntityType.CATALOG:
            prefix = ""
        elif self.after.entity_type == CoverageReport.EntityType.TABLE:
            prefix = "- "
        elif self.after.entity_type == CoverageReport.EntityType.COLUMN:
            prefix = "-- "
        else:
            prefix = ""
        title = self.after.entity_name if self.after.entity_name else "Catalog"
        title = prefix + title
        before_covered = len(self.before.covered) if self.before else "-"
        before_total = len(self.before.total) if self.before else "-"
        before_coverage = f"({self.before.coverage:.2%})" if self.before else "(-)"
        after_covered = len(self.after.covered)
        after_total = len(self.after.total)
        after_coverage = f"({self.after.coverage:.2%})"
        buf = io.StringIO()
        buf.write(f"{title:50}")
        buf.write(f"{before_covered:>5}/{before_total:<5}{before_coverage:^9}")
        buf.write(" -> ")
        buf.write(f"{after_covered:>5}/{after_total:<5}{after_coverage:^9}\n")
        return buf.getvalue()

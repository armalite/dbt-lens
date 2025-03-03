# dbt_lens/core/manifest.py

import json
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import Dict

from dbt_lens.config import SUPPORTED_MANIFEST_SCHEMA_VERSIONS


@dataclass
class Manifest:
    sources: Dict[str, Dict]
    models: Dict[str, Dict]
    seeds: Dict[str, Dict]
    snapshots: Dict[str, Dict]
    tests: Dict[str, Dict]

    @classmethod
    def from_nodes(cls, manifest_nodes: Dict[str, Dict]) -> Manifest:
        sources = [
            table for table in manifest_nodes.values() if table["resource_type"] == "source"
        ]
        sources = {
            table["unique_id"]: {
                "columns": cls._normalize_column_names(table["columns"]),
                "original_file_path": cls._normalize_path(table["original_file_path"]),
                "name": cls._full_table_name(table),
            }
            for table in sources
        }

        models = [table for table in manifest_nodes.values() if table["resource_type"] == "model"]
        models = {
            table["unique_id"]: {
                "columns": cls._normalize_column_names(table["columns"]),
                "original_file_path": cls._normalize_path(table["original_file_path"]),
                "name": cls._full_table_name(table),
            }
            for table in models
        }

        seeds = [table for table in manifest_nodes.values() if table["resource_type"] == "seed"]
        seeds = {
            table["unique_id"]: {
                "columns": cls._normalize_column_names(table["columns"]),
                "original_file_path": cls._normalize_path(table["original_file_path"]),
                "name": cls._full_table_name(table),
            }
            for table in seeds
        }

        snapshots = [
            table for table in manifest_nodes.values() if table["resource_type"] == "snapshot"
        ]
        snapshots = {
            table["unique_id"]: {
                "columns": cls._normalize_column_names(table["columns"]),
                "original_file_path": cls._normalize_path(table["original_file_path"]),
                "name": cls._full_table_name(table),
            }
            for table in snapshots
        }

        tests = cls._parse_tests(manifest_nodes)

        return Manifest(sources, models, seeds, snapshots, tests)

    def get_table(self, table_id: str):
        candidates = [
            self.sources.get(table_id),
            self.models.get(table_id),
            self.seeds.get(table_id),
            self.snapshots.get(table_id),
        ]
        non_empty = [c for c in candidates if c]
        if len(non_empty) > 1:
            raise ValueError(f"Duplicate unique_id: {table_id}, duplicates: {non_empty}")
        return non_empty[0] if non_empty else None

    @classmethod
    def _parse_tests(cls, manifest_nodes: Dict[str, Dict]) -> Dict[str, Dict]:
        tests = {}
        for node in manifest_nodes.values():
            if node["resource_type"] != "test" or "test_metadata" not in node:
                continue

            depends_on = node["depends_on"]["nodes"]
            if not depends_on:
                continue

            if node["test_metadata"]["name"] == "relationships":
                table_id = depends_on[-1]
            else:
                table_id = depends_on[0]

            column_name = (
                node.get("column_name")
                or node["test_metadata"]["kwargs"].get("column_name")
                or node["test_metadata"]["kwargs"].get("arg")
            )
            if not column_name:
                continue

            column_name = column_name.lower()
            table_tests = tests.setdefault(table_id, {})
            column_tests = table_tests.setdefault(column_name, [])
            column_tests.append(node)
        return tests

    @staticmethod
    def _full_table_name(table: Dict) -> str:
        return f"{table['schema']}.{table['name']}".lower()

    @staticmethod
    def _normalize_column_names(columns: Dict) -> Dict:
        for col in columns.values():
            col["name"] = col["name"].lower()
        return {col["name"]: col for col in columns.values()}

    @staticmethod
    def _normalize_path(path: str) -> str:
        return str(Path(path).as_posix())


def check_manifest_version(manifest_json: Dict) -> None:
    manifest_version = manifest_json["metadata"]["dbt_schema_version"]
    if manifest_version not in SUPPORTED_MANIFEST_SCHEMA_VERSIONS:
        logging.warning(
            "Unsupported manifest.json version %s, unexpected behavior can occur. Supported versions: %s.",
            manifest_version,
            SUPPORTED_MANIFEST_SCHEMA_VERSIONS,
        )


def load_manifest(project_dir: Path, run_artifacts_dir: Path) -> Manifest:
    if run_artifacts_dir is None:
        manifest_path = project_dir / "target/manifest.json"
    else:
        manifest_path = run_artifacts_dir / "manifest.json"

    if not manifest_path.exists():
        raise FileNotFoundError(
            "manifest.json not found. Run a dbt command to generate the manifest."
        )

    with open(manifest_path) as f:
        manifest_json = json.load(f)

    check_manifest_version(manifest_json)
    manifest_nodes = {**manifest_json["sources"], **manifest_json["nodes"]}
    manifest = Manifest.from_nodes(manifest_nodes)
    return manifest

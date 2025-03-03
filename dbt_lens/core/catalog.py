# dbt_lens/core/catalog.py

import json
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List

from dbt_lens.core.manifest import Manifest


@dataclass
class Column:
    name: str
    doc: bool = False
    test: bool = False

    @staticmethod
    def from_node(node: Dict) -> Column:
        return Column(name=node["name"].lower())

    @staticmethod
    def is_valid_doc(doc) -> bool:
        return bool(doc and doc != "")

    @staticmethod
    def is_valid_test(tests) -> bool:
        return bool(tests)


@dataclass
class Table:
    unique_id: str
    name: str
    original_file_path: str
    columns: Dict[str, Column]

    @staticmethod
    def from_node(node: Dict, manifest: Manifest) -> Table:
        unique_id = node["unique_id"]
        manifest_table = manifest.get_table(unique_id)
        if manifest_table is None:
            raise ValueError(f"Unique ID {unique_id} not found in manifest.json")
        columns = [Column.from_node(col) for col in node["columns"].values()]
        original_file_path = manifest_table.get("original_file_path")
        if original_file_path is None:
            logging.warning("original_file_path not found in manifest for %s", unique_id)
        return Table(
            unique_id=unique_id,
            name=manifest_table["name"].lower(),
            original_file_path=original_file_path,
            columns={col.name: col for col in columns},
        )

    def get_column(self, column_name: str) -> Column:
        return self.columns.get(column_name)


@dataclass
class Catalog:
    tables: Dict[str, Table]

    @staticmethod
    def from_nodes(nodes: List[Dict], manifest: Manifest) -> Catalog:
        tables = [Table.from_node(table, manifest) for table in nodes]
        return Catalog({table.unique_id: table for table in tables})

    def filter_tables(self, model_path_filter: List[str]) -> Catalog:
        filters = tuple(model_path_filter)
        filtered = {
            t_id: t for t_id, t in self.tables.items()
            if t.original_file_path.startswith(filters)
        }
        logging.info("Filtered tables count: %d", len(filtered))
        return Catalog(tables=filtered)

    def get_table(self, table_id: str) -> Table:
        return self.tables.get(table_id)


def load_catalog(project_dir: Path, run_artifacts_dir: Path, manifest: Manifest) -> Catalog:
    if run_artifacts_dir is None:
        catalog_path = project_dir / "target/catalog.json"
    else:
        catalog_path = run_artifacts_dir / "catalog.json"

    if not catalog_path.exists():
        raise FileNotFoundError(
            "catalog.json not found. Before using dbt_lens, run: dbt docs generate"
        )

    with open(catalog_path) as f:
        catalog_json = json.load(f)

    catalog_nodes = {**catalog_json["sources"], **catalog_json["nodes"]}
    # Filter out tables storing test failures
    catalog_nodes = {n_id: n for n_id, n in catalog_nodes.items() if not n_id.startswith("test.")}
    catalog = Catalog.from_nodes(list(catalog_nodes.values()), manifest)

    logging.info("Loaded %d tables from catalog", len(catalog.tables))
    return catalog

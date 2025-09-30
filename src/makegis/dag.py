import sys
import graphlib
from pathlib import Path
import subprocess
from typing import Dict
from typing import List
from typing import Set
from typing import Tuple

from pydantic import BaseModel
import yaml

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

from .config import Target
from . import postgis
from .utils import expand_dict_strings


class SQLSource(BaseModel):
    path: Path


class WFSSource(BaseModel):
    url: str


class FileSource(BaseModel):
    path: Path


class DuckDBSource(BaseModel):
    path: Path
    table: str


class NodeDefaults(BaseModel):
    geom_index: bool = False
    src_epsg: int | None = None
    dst_epsg: int | None = None


class Table(BaseModel):
    name: str
    geom_index: bool
    src_epsg: int | None
    dst_epsg: int | None
    src: SQLSource | DuckDBSource | FileSource | WFSSource


class NodeConfig(BaseModel):
    id: str
    deps: List[str] = []
    prep: List[Path] = []
    tables: List[Table]


class DAG:

    def __init__(self, nodes: List[NodeConfig]):
        self._nodes: Dict[str, NodeConfig] = {}
        self._table2node: Dict[str, str] = {}
        self._graph: Dict[str, Set[str]] = {}

        # Build node lookup
        self._nodes = {nc.id: nc for nc in nodes}

        # Register tables
        for nc in nodes:
            for table in nc.tables:
                assert table.name not in self._table2node
                self._table2node[table.name] = nc.id

        # Build graph
        for nc in nodes:
            parent_nodes = [self._table2node[dep] for dep in nc.deps]
            self._graph[nc.id] = set(parent_nodes)

    @classmethod
    def from_root_path(cls, root: Path):
        print(f"building dag from '{root}'")
        nodes = []
        for path in root.rglob("makegis.yml"):
            relative = path.relative_to(root)
            schema = relative.parts[0]
            prefix = "_".join(relative.parts[1:-1])
            # print(f"path: '{path}', schema: '{schema}' prefix: '{prefix}'")
            node_config = parse_makegis_yml(path, schema, prefix)
            nodes.append(node_config)

        return DAG(nodes)

    def check(self):
        ts = graphlib.TopologicalSorter(self._graph)
        ts.prepare()

    def print(self):
        ts = graphlib.TopologicalSorter(self._graph)
        node_ids = tuple(ts.static_order())
        for nid in node_ids:
            nc = self._nodes[nid]
            print(nc.id)
            for table in nc.tables:
                print(f"\t{table.name}")

    def run(self, node_id: str, target: Target):
        nc = self._nodes[node_id]
        n = len(nc.prep)
        for i, action in enumerate(nc.prep, start=1):
            ret = run_action(action, i, n)
            if ret == 0:
                continue
            print(f"error - prep {i}/{n} {action} failed")
            raise RuntimeError("prep step failed")
        for table in nc.tables:
            load_table(target, table)


def parse_makegis_yml(path: Path, schema: str, prefix: str) -> NodeConfig:
    with open(path) as f:
        d = yaml.load(f, Loader)
    expand_dict_strings(d)

    # defaults
    node_defaults = parse_node_defaults(d)

    deps = []
    if "deps" in d:
        deps = d["deps"]
    prep = []
    if "prep" in d and d["prep"] is not None:
        prep = [Path(path.parent) / Path(item) for item in d["prep"]]
    tables = []
    for local_name, kvs in d["tables"].items():
        src = None
        if "sql" in kvs:
            src = SQLSource(path=kvs["sql"])
        elif "duckdb" in kvs:
            ddb_table = local_name
            if "table" in kvs:
                ddb_table = kvs["table"]
            src = DuckDBSource(path=kvs["duckdb"], table=ddb_table)
        elif "wfs" in kvs:
            src = WFSSource(url=kvs["wfs"])
        elif "file" in kvs:
            src = FileSource(path=kvs["file"])
        else:
            raise RuntimeError(f"unknown source type in {path}")
        table_name = f"{schema}.{prefix}{'_' if prefix else ''}{local_name}"
        if "epsg" in kvs:
            src_epsg, dst_epsg = parse_epsg(kvs["epsg"])
        else:
            src_epsg, dst_epsg = node_defaults.src_epsg, node_defaults.dst_epsg
        table = Table(
            name=table_name,
            src=src,
            geom_index=kvs.get("index", node_defaults.geom_index),
            src_epsg=src_epsg,
            dst_epsg=dst_epsg,
        )
        tables.append(table)

    return NodeConfig(
        id=f"{schema}{'.' if prefix else ''}{prefix}",
        deps=deps,
        prep=prep,
        tables=tables,
    )


def parse_epsg(e: int | str | None) -> Tuple[int | None, int | None]:
    """Return (src, dst) EPSG's from `int` or `int:int` type string"""
    if e is None:
        return (None, None)
    if isinstance(e, int):
        return (None, e)

    parts = e.split(":")
    assert len(parts) == 2
    return (int(parts[0]), int(parts[1]))


def parse_node_defaults(d: Dict) -> NodeDefaults:
    if "defaults" not in d or d["defaults"] is None:
        return NodeDefaults()
    dd = d["defaults"]
    defs = {}
    if "epsg" in dd:
        defs["src_epsg"], defs["dst_epsg"] = parse_epsg(dd["epsg"])
    if "index" in dd:
        defs["geom_index"] = dd["index"]

    return NodeDefaults(**defs)


def load_table(target: Target, table: Table):
    if isinstance(table.src, DuckDBSource):
        load_duckdb_table(target, table)
    elif isinstance(table.src, SQLSource):
        load_sql_source(target, table.name, table.src)
    else:
        raise NotImplementedError(f"table source not supported yet: f{table.src}")


def load_duckdb_table(target: Target, table: Table):
    print(f"loading duckdb table {table.name}")
    assert isinstance(table.src, DuckDBSource)
    src = table.src
    src_table = src.table
    dst_table = table.name
    postgis.ddb2pg(
        src.path,
        target.conn_str(),
        src_table,
        dst_table,
        src_epsg=table.src_epsg,
        dst_epsg=table.dst_epsg,
        geom_index=table.geom_index,
    )


def load_sql_source(target: Target, table_name: str, src: SQLSource):
    print(f"loading sql table {table_name}")


def run_action(action: Path, i: int, n: int):
    cmd = []
    if action.suffix == ".py":
        python_exe = sys.executable
        cmd.append(python_exe)
        cmd.append("-u")  # unbuffered mode to display logs as they appear
        cmd.append(action)
    else:
        raise NotImplementedError(f"unsupported action: {action}")

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    if process.stdout is not None:
        for line in process.stdout:
            print(f"prep ({i}/{n}) | {line}", end="")

    return process.wait()

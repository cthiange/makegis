import os
import sys
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict
from typing import List
from typing import Literal
from typing import NamedTuple
from typing import Set

import graphlib

from ..core.load import LoadJob
from ..core.transforms import Transform
from ..core.commands import Command
from ..config import TargetConfig
from .. import postgis


class DatabaseObject(NamedTuple):
    schema: str
    name: str
    type: Literal["relation", "function"]

    @property
    def full_name(self):
        return f"{self.schema}.{self.name}"


@dataclass(frozen=True)
class BaseNode:
    id: str
    owns: Set[DatabaseObject]


@dataclass(frozen=True)
class SourceNode(BaseNode):
    job: LoadJob

    @property
    def deps(self):
        return set()


@dataclass(frozen=True)
class TransformNode(BaseNode):
    deps: Set[DatabaseObject]
    transform: Transform


@dataclass(frozen=True)
class CustomNode(BaseNode):
    deps: Set[DatabaseObject]
    prep: List[Command]
    load: List[LoadJob]
    run: List[Command]
    cleanup: List[Command]


type Node = SourceNode | TransformNode | CustomNode


class DAG:

    def __init__(self, nodes: List[Node]):
        # Node id to node lookup
        self._nodes: Dict[str, Node] = {}
        # Database object to node lookup
        self._dbo2node: Dict[DatabaseObject, Node] = {}
        # DAG graph
        self._graph: Dict[str, Set[str]] = {}

        # Build node lookup
        self._nodes = {nc.id: nc for nc in nodes}
        if len(self._nodes) != len(nodes):
            # Report duplicated ids and exit
            dupes = [node.id for node in nodes if node not in self._nodes.values()]
            raise ValueError(f"Found non-unique node ids: {dupes}")

        # Register tables
        for node in nodes:
            for dbo in node.owns:
                assert dbo not in self._dbo2node
                self._dbo2node[dbo] = node

        # Build graph
        for node in nodes:
            parent_nodes = [self._dbo2node[dep].id for dep in node.deps]
            self._graph[node.id] = set(parent_nodes)

    def print(self):
        ts = graphlib.TopologicalSorter(self._graph)
        node_ids = tuple(ts.static_order())
        for node_id in node_ids:
            node = self._nodes[node_id]
            match node:
                case SourceNode():
                    node_type = "S"
                case TransformNode():
                    node_type = "T"
                case CustomNode():
                    node_type = "C"
                case _:
                    node_type = "?"
            print(f"[{node_type}] {node.id}")
            for dbo in node.owns:
                print(f"\t{dbo.full_name}")

    def run(self, node_id: str, target: TargetConfig):
        node = self._nodes[node_id]
        match node:
            case SourceNode():
                postgis.load_table(target, node.job)
            case TransformNode():
                run_sql(target, node.transform.sql)
            case CustomNode():
                n = len(node.prep)
                for i, action in enumerate(node.prep, start=1):
                    ret = run_action(action.path, i, n)
                    if ret == 0:
                        continue
                    print(f"error - prep {i}/{n} {action} failed")
                    raise RuntimeError("prep step failed")
                for job in node.load:
                    postgis.load_table(target, job)
                for i, action in enumerate(node.run):
                    ret = run_action(action.path, i, n)
                    if ret == 0:
                        continue
                    print(f"error - task {i}/{n} {action} failed")
                    raise RuntimeError("run step failed")
                for i, action in enumerate(node.cleanup):
                    ret = run_action(action.path, i, n)
                    if ret == 0:
                        continue
                    print(f"error - cleanup {i}/{n} {action} failed")
                    raise RuntimeError("cleanup step failed")

    def show(self, pattern: str):
        """
        Print a DAG node or subset.

        Usefull to test out selection patterns.
        """
        node = self._nodes[pattern]
        print_node(node)


def print_node(node: Node):
    match node:
        case SourceNode():
            node_type = "S"
        case TransformNode():
            node_type = "T"
        case CustomNode():
            node_type = "C"
        case _:
            node_type = "?"
    print(f"[{node_type}] {node.id}")
    for dbo in node.deps:
        print(f"\t{dbo.full_name} -->")
    for dbo in node.owns:
        print(f"\t--> {dbo.full_name}")


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


def run_sql(target: TargetConfig, path: Path):
    assert path.suffix == ".sql"
    psql = os.environ.get("MKGS_PSQL", "psql")
    cmd = [
        psql,
        "-h",
        target.host,
        "-U",
        target.user,
        "-p",
        str(target.port),
        "-d",
        target.db,
        "-v",
        "ON_ERROR_STOP=ON",
        "-f",
        path,
    ]

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    if process.stdout is not None:
        for line in process.stdout:
            print(f"transform ({path.name}) | {line}", end="")

    ret = process.wait()

    if ret != 0:
        raise RuntimeError(f"error while running sql transform {path}")

import logging
import os
import re
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
from .. import journal
from .. import errors
from ..utils import capture_logs
from ..targets import Target

log = logging.getLogger("makegis")


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
        # DAG graph - maps node id to parent node ids
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

    def run_node(self, node_id: str, target: Target):
        node = self._nodes[node_id]
        event = journal.RunEvent(node_id).start()
        match node:
            case SourceNode():
                target.load_table(node.job)
            case TransformNode():
                target.run_transform(node.transform)
            case CustomNode():
                n = len(node.prep)
                for i, action in enumerate(node.prep, start=1):
                    ret = run_action(action.path, f"prep {i}/{n}")
                    if ret == 0:
                        continue
                    log.error(f"prep {i}/{n} {action} failed")
                    raise RuntimeError("prep step failed")
                for job in node.load:
                    target.load_table(job)
                for i, action in enumerate(node.run):
                    ret = run_action(action.path, f"run {i}/{n}")
                    if ret == 0:
                        continue
                    log.error(f"task {i}/{n} {action} failed")
                    raise RuntimeError("run step failed")
                for i, action in enumerate(node.cleanup):
                    ret = run_action(action.path, f"cleanup {i}/{n}")
                    if ret == 0:
                        continue
                    log.error(f"cleanup {i}/{n} {action} failed")
                    raise RuntimeError("cleanup step failed")
        target.log_event(event)

    def get_outdated(
        self,
        target: Target,
        limit_to: List[str] | None = None,
    ) -> Set[str]:
        """Get ids of outdated nodes"""
        manifest = target.fetch_manifest()

        # Collects ids of missing or outdated nodes
        outdated = set()

        ts = graphlib.TopologicalSorter(self._graph)
        node_ids = tuple(ts.static_order())
        for node_id in node_ids:
            # Nodes not in manifest have never been run, and thus outdated.
            if node_id not in manifest:
                outdated.add(node_id)
                continue
            # Any node with an outdated parent is itself outdated.
            # Outdated parents are guaranteed to have been detected at this point
            # because we are iterating over nodes in topological order.
            parent_ids = self._graph[node_id]
            if any([pid in outdated for pid in parent_ids]):
                outdated.add(node_id)
                continue
            # Finally, if timestamp of at least one dependency is missing or more recent
            # than the node's own timestamp, then the node is outdated.
            node_ts = manifest[node_id]
            for pid in parent_ids:
                if pid not in manifest or node_ts < manifest[pid]:
                    outdated.add(node_id)

        if limit_to is not None:
            log.debug(f"limiting outdated nodes to {limit_to}")
            outdated = outdated & set(limit_to)

        log.info(f"found {len(outdated)} outdated node(s)")

        return outdated

    def list_schemas(self) -> List[str]:
        schemas = [dbo.schema for dbo in self._dbo2node.keys()]
        schemas = list(set(schemas))
        schemas.sort()
        return schemas

    def render_node(self, node_id: str) -> str:
        """
        Render a DAG node to string.
        """
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
        s = f"[{node_type}] {node.id}\n"
        for dbo in node.deps:
            s += f"\t{dbo.full_name} -->\n"
        for dbo in node.owns:
            s += f"\t--> {dbo.full_name}"
        return s

    def select_nodes(self, pattern: str) -> List[str]:
        """
        Get list of topologically sorted node ids matching given selection pattern.

        Args:
            pattern: DAG selection pattern. See below.
            outdated: Include outdated nodes only. Defaults to False.

        Pattern syntax:

            - `node*`   select all nodes starting with `node`

        Todo:
            - `node+`   select node and all descendants
            - `+node`   select node and all ancestors
            - `node+1`  select node and its 1st degree descendants
            - `2+node`  select node and its 1st and 2nd degree ancestors
        """
        # Default graph propagation flags
        upstream = False
        downstream = False

        search = pattern
        if pattern.startswith("+"):
            upstream = True
            search = search[1:]
        if pattern.endswith("+"):
            downstream = True
            search = search[:-1]
        if "+" in search:
            raise ValueError(
                "The '+' graph operator can only be used at the start and end of a selectio pattern"
            )

        # Convert search term to equivalent regex
        search = re.escape(search).replace("\\*", ".*")
        search = f"^{search}$"
        p = re.compile(search)

        # Collect nodes matching by name pattern
        selection = set()
        for node_id in self._nodes:
            if re.match(p, node_id):
                selection.add(node_id)

        # Collect upstream and downstream nodes if needed
        if upstream or downstream:
            raise NotImplementedError(
                "The `+` graph selection operator is not supported yet"
            )

        # Sort nodes topologically
        ts = graphlib.TopologicalSorter(self._graph)
        selection = [nid for nid in ts.static_order() if nid in selection]

        log.info(
            f"found {len(selection)} node(s) matching selection pattern '{pattern}'"
        )

        return selection


def run_action(action: Path, log_prefix: str):
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

    capture_logs(process.stdout, log_prefix)

    return process.wait()

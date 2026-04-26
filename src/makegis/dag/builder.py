import logging
from typing import Tuple


from ..config import makegis as config
from ..config.makegis import LoadItem
from ..config.makegis import DatabaseItem
from ..config.project import Project
from ..config.project import ProjectSource
from ..config.project import ProjectTransform
from ..config.project import ProjectNode
from ..core.load import Destination
from ..core.load import LoadJob
from ..core.load import CSVSource
from ..core.load import EsriSource
from ..core.load import DuckDBSource
from ..core.load import FileSource
from ..core.load import RasterSource
from ..core.load import WFSSource
from ..core.transforms import Transform
from ..core.commands import Command
from .dag import DAG
from .dag import DatabaseObject
from .dag import SourceNode
from .dag import TransformNode
from .dag import CustomNode
from .sql import analyze_sql_file

log = logging.getLogger("makegis")


class Builder:

    @staticmethod
    def build_project(project: Project) -> DAG:
        """Build DAG from project"""
        log.debug("building DAG")
        # Init a list to collect DAG nodes in
        nodes = []

        for ps in project.sources:
            nodes.append(process_project_source(ps))

        for pt in project.transforms:
            nodes.append(process_project_transform(pt))

        for pn in project.custom:
            nodes.append(process_project_node(pn))

        return DAG(nodes)


def process_project_source(ps: ProjectSource) -> SourceNode:
    log.debug(f"processing source {ps.name}")
    job = prepare_load_job(ps.name, ps.source)
    table = DatabaseObject(
        schema=job.dst.table_schema,
        name=job.dst.table_name,
        type="relation",
    )
    return SourceNode(id=table.full_name, owns=set([table]), job=job)


def prepare_load_job(
    item_name: str,
    item: LoadItem,
) -> LoadJob:
    # Split item name into schema and table name
    schema, table_name = parse_item_name(item_name)
    # Infer source and destination EPSG's from epsg setting
    src_epsg, dst_epsg = parse_epsg(item.epsg)
    # Destination
    dest = Destination(
        table_schema=schema,
        table_name=table_name,
        epsg=dst_epsg,
        **item.model_dump(exclude={"epsg"}, exclude_unset=True),
    )
    # Source
    source_options = {"epsg": src_epsg}
    if isinstance(item, config.CSVSource):
        src = CSVSource(
            path=item.csv,
            pk=item.pk,
            **source_options,
        )
    elif isinstance(item, config.EsriSource):
        src = EsriSource(url=item.esri, f=item.f, pk=item.pk, **source_options)
    elif isinstance(item, config.DuckDBSource):
        assert item.pk is None, "explicit pk not implemented for duckdb source"
        assert item.table is not None
        src = DuckDBSource(
            path=item.duckdb,
            table=item.table,
            pk=item.pk,
            **source_options,
        )
    elif isinstance(item, config.FileSource):
        src = FileSource(
            path=item.file,
            layer=item.layer,
            pk=item.pk,
            **source_options,
        )
    elif isinstance(item, config.RasterSource):
        src = RasterSource(
            path=item.raster,
            pk=item.pk,
            **source_options,
        )
    elif isinstance(item, config.WFSSource):
        src = WFSSource(url=item.wfs, pk=item.pk, **source_options)
    else:
        raise NotImplementedError("Unhandled source type")
    return LoadJob(src=src, dst=dest)


def process_project_transform(pt: ProjectTransform) -> TransformNode:
    log.debug(f"processing transform {pt.name}")
    assert pt.script.suffix == ".sql"
    report = analyze_sql_file(pt.script)
    deps = {
        DatabaseObject(schema=d.schema, name=d.name, type=d.type)
        for d in report.dependencies
    }
    owns = {
        DatabaseObject(schema=d.schema, name=d.name, type=d.type)
        for d in report.created
    }
    return TransformNode(
        id=pt.name,
        deps=deps,
        owns=owns,
        transform=Transform(sql=pt.script),
    )


def process_project_node(pn: ProjectNode) -> CustomNode:
    log.debug(f"processing node {pn.name}")
    owns = set()
    # Handle load block
    load_jobs = []
    load_block: dict[str, LoadItem] = pn.load or {}
    for item_name, item in load_block.items():
        # Creat load job
        job = prepare_load_job(item_name, item)
        # Add job's target table to node's owned tables
        owns.add(
            DatabaseObject(
                schema=job.dst.table_schema,
                name=job.dst.table_name,
                type="relation",
            )
        )
        load_jobs.append(job)
    # Handle run block
    run_commands = []
    for task in pn.run or []:
        run_commands.append(Command(path=task.cmd))
        for db_item in task.creates:
            owns.add(db_object_from_db_item(db_item))

    # Pleasing the type checker.
    # Context expansion ensures even anon nodes have a name.
    assert pn.name is not None

    return CustomNode(
        id=pn.name,
        deps=set([db_object_from_db_item(dep) for dep in pn.deps or []]),
        owns=owns,
        prep=[Command(path=p) for p in pn.prep or []],
        load=load_jobs,
        run=run_commands,
        cleanup=[Command(path=p) for p in pn.cleanup or []],
    )


def parse_item_name(item_name: str) -> Tuple[str, str]:
    """Split name into schema and table"""
    assert "." in item_name
    parts = item_name.split(".")
    schema = parts[0]
    name = ".".join(parts[1:])
    return schema, name


def parse_epsg(e: int | str | None) -> Tuple[int | None, int | None]:
    """Return (src, dst) EPSG's from `int` or `int:int` type string"""
    if e is None:
        return (None, None)
    elif isinstance(e, int):
        return (None, e)
    elif isinstance(e, str):
        parts = e.split(":")
        match len(parts):
            case 1:
                return (None, int(e))
            case 2:
                return (int(parts[0]), int(parts[1]))
    raise ValueError("epsg should be an integer or a string of the form <int>:<int>")


def db_object_from_db_item(item: DatabaseItem) -> DatabaseObject:
    schema, name = parse_item_name(item.name)
    match item.type:
        case "table":
            return DatabaseObject(schema=schema, name=name, type="relation")
        case "function":
            return DatabaseObject(schema=schema, name=name, type="function")
    raise ValueError(f"Unknown database item type {item.type}")

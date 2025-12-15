from dataclasses import dataclass
from pathlib import Path
from typing import List
from typing import Tuple
from typing import Iterator


from ..config import RootConfig
from ..config.makegis import MakeGISConfig
from ..config.makegis import LoadDefaults
from ..config.makegis import LoadBlock
from ..config.makegis import SourceBlock
from ..config.makegis import TransformBlock
from ..config.makegis import NodeBlock
from ..config.makegis import LoadItem
from ..config.makegis import DatabaseItem
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


@dataclass(frozen=True)
class MakeGISFileContext:
    # Top-level directories under root map to a schema
    schema: str
    # Parent directories, excluding top-level schema, form a table name prefix
    # e.g. root/schema/some_group/makegis.yml --> prefix = 'some_group'
    prefix: str
    # Actual path to file
    path: Path

    def resolve_path(self, path: Path) -> Path:
        """Contextualize and resolve given path"""
        path = path.expanduser()
        if not path.is_absolute():
            # Add context to relative path
            path = self.path.parent / path
        return path.resolve()


@dataclass(frozen=True)
class MakeGISFile:
    context: MakeGISFileContext
    # Config object
    block: LoadBlock | TransformBlock | NodeBlock


class Builder:

    def __init__(self, root_config: RootConfig):
        self.root_cfg = root_config

    def build(self) -> DAG:
        """Build DAG from makegis.yml files found in dir tree"""
        # Init a list to collect DAG nodes in
        nodes = []
        # Iterate over all makegis.yml files under the project's root dir
        for mf in collect_makegis_files(self.root_cfg.src_dir):
            match mf.block:
                case LoadBlock():
                    global_defaults = self.root_cfg.defaults.load
                    nodes.extend(
                        process_load_block(mf.context, mf.block, global_defaults)
                    )
                case TransformBlock():
                    nodes.extend(process_transform_block(mf.context, mf.block))
                case NodeBlock():
                    global_defaults = self.root_cfg.defaults.load
                    nodes.append(
                        process_node_block(mf.context, mf.block, global_defaults)
                    )
                case _:
                    raise NotImplementedError("Unhandled makegis.yml block type")

        return DAG(nodes)


def process_load_block(
    ctx: MakeGISFileContext, block: LoadBlock, global_defaults: LoadDefaults
) -> List[SourceNode]:
    nodes = []
    local_defaults = block.defaults
    for item in block.items:
        job = prepare_load_job(ctx, item, local_defaults, global_defaults)
        table = DatabaseObject(
            schema=job.dst.schema,
            name=job.dst.table,
            type="relation",
        )
        nodes.append(SourceNode(id=table.full_name, owns=set([table]), job=job))
    return nodes


def process_transform_block(
    ctx: MakeGISFileContext, block: TransformBlock
) -> List[TransformNode]:
    nodes = []
    for t in block.transforms:
        assert t.path.suffix == ".sql"
        contextualized_script_path = ctx.path.parent / t.path
        report = analyze_sql_file(contextualized_script_path)
        deps = {
            DatabaseObject(schema=d.schema, name=d.name, type=d.type)
            for d in report.dependencies
        }
        owns = {
            DatabaseObject(schema=d.schema, name=d.name, type=d.type)
            for d in report.created
        }
        node = TransformNode(
            id=f"{ctx.schema}.{ctx.prefix + '_' if ctx.prefix else ''}{t.path.stem}",
            deps=deps,
            owns=owns,
            transform=Transform(sql=contextualized_script_path),
        )
        nodes.append(node)
    return nodes


def process_node_block(
    ctx: MakeGISFileContext,
    block: NodeBlock,
    global_defaults: LoadDefaults,
) -> CustomNode:
    deps = set([db_object_from_db_item(item) for item in block.deps or []])
    owns = set()
    load_jobs = []
    if block.do.load is not None:
        # Parse load block
        source_nodes = process_load_block(ctx, block.do.load, global_defaults)
        # Extract load jobs from resulting nodes
        load_jobs = [sn.job for sn in source_nodes]
        # Collect created tables
        for sn in source_nodes:
            owns |= sn.owns
    run_commands = []
    for task in block.do.run or []:
        run_commands.append(Command(path=ctx.path.parent / Path(task.cmd)))
        for item in task.creates:
            owns.add(db_object_from_db_item(item))
    return CustomNode(
        id=f"{ctx.schema}{'.' + ctx.prefix if ctx.prefix else ''}",
        deps=deps,
        owns=owns,
        prep=[Command(path=ctx.path.parent / Path(s)) for s in block.prep or []],
        load=load_jobs,
        run=run_commands,
        cleanup=[Command(path=ctx.path.parent / Path(s)) for s in block.cleanup or []],
    )


def collect_makegis_files(root: Path) -> Iterator[MakeGISFile]:
    """
    Provides an iteraror over all makegis.yml files in the directory tree under *root*

    Files get parsed and embedded in a MakeGISFile instance.
    """
    for path in root.rglob("makegis.yml"):
        relative = path.relative_to(root)
        schema = relative.parts[0]
        # First part is schema, last part is file name
        # Prefix is everything in between
        prefix = "_".join(relative.parts[1:-1]) if len(relative.parts) > 2 else ""
        # Bundle context with parsed contents
        yield MakeGISFile(
            context=MakeGISFileContext(schema=schema, prefix=prefix, path=path),
            block=MakeGISConfig.from_file(path).block,
        )


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
    assert "." in item.name
    parts = item.name.split(".")
    schema = parts[0]
    name = ".".join(parts[1:])
    match item.type:
        case "table":
            return DatabaseObject(schema=schema, name=name, type="relation")
        case "function":
            return DatabaseObject(schema=schema, name=name, type="function")
    raise ValueError(f"Unknown database item type {item.type}")


def prepare_load_job(
    ctx: MakeGISFileContext,
    item: LoadItem,
    local_defaults: LoadDefaults,
    global_defaults: LoadDefaults,
) -> LoadJob:
    dh = LoadDefaultHandler(local_defaults, global_defaults)
    src_epsg, dst_epsg = parse_epsg(dh.epsg(item.src))
    # Destination
    dest = Destination(
        schema=ctx.schema,
        table=f"{ctx.prefix + '_' if ctx.prefix else ''}{item.name}",
        epsg=dst_epsg,
        geom_index=dh.geom_index(item.src),
        geom_column=dh.geom_column(item.src),
        raster_index=dh.raster_index(item.src),
        raster_column=dh.raster_column(item.src),
        raster_constraints=dh.raster_constraints(item.src),
        tile_size=dh.tile_size(item.src),
    )
    # Source
    settings = {"epsg": src_epsg}
    if item.src.type == "csv":
        src = CSVSource(
            path=ctx.resolve_path(item.src.path),
            pk=item.src.pk,
            **settings,
        )
    elif item.src.type == "esri":
        src = EsriSource(url=item.src.url, f=item.src.f, pk=item.src.pk, **settings)
    elif item.src.type == "duckdb":
        assert item.src.pk is None, "explicit pk not implemented for duckdb source"
        src = DuckDBSource(
            path=item.src.path,
            table=item.src.table or item.name,
            pk=item.src.pk,
            **settings,
        )
    elif item.src.type == "file":
        src = FileSource(
            path=ctx.resolve_path(item.src.path),
            layer=item.src.layer,
            pk=item.src.pk,
            **settings,
        )
    elif item.src.type == "raster":
        src = RasterSource(
            path=ctx.resolve_path(item.src.path),
            pk=item.src.pk,
            **settings,
        )
    elif item.src.type == "wfs":
        src = WFSSource(url=item.src.url, pk=item.src.pk, **settings)
    else:
        raise NotImplementedError("Unhandled source type")
    return LoadJob(src=src, dst=dest)


class LoadDefaultHandler:

    def __init__(self, local_defaults: LoadDefaults, global_defaults: LoadDefaults):
        self._local = local_defaults
        self._global = global_defaults

    def epsg(self, src: SourceBlock) -> int | str | None:
        return self._fallback(src, "epsg")

    def geom_index(self, src: SourceBlock) -> bool:
        return self._fallback(src, "geom_index")

    def geom_column(self, src: SourceBlock) -> str | None:
        return self._fallback(src, "geom_column")

    def raster_index(self, src: SourceBlock) -> bool:
        return self._fallback(src, "raster_index")

    def raster_column(self, src: SourceBlock) -> str | None:
        return self._fallback(src, "raster_column")

    def raster_constraints(self, src: SourceBlock) -> str | None:
        return self._fallback(src, "raster_constraints")
    
    def tile_size(self, src: SourceBlock) -> str | None:
        return self._fallback(src, "tile_size")
    
    def _fallback(self, src: SourceBlock, key: str):
        if key in src.model_fields_set:
            return getattr(src, key)
        elif key in self._local.model_fields_set:
            return getattr(self._local, key)
        else:
            return getattr(self._global, key)

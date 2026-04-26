import copy
import logging
from pathlib import Path
from typing import Iterator
from typing import Literal

from pydantic import BaseModel
import yaml

from .makegis import ConfigFile
from .makegis import DatabaseItem
from .makegis import Defaults
from .makegis import CSVSource
from .makegis import DuckDBSource
from .makegis import FileSource
from .makegis import CustomNode
from .makegis import LoadItem
from .utils import expand_dict_strings

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

"""
A project collects config files and interprets them in their context.

The context of a config file affects it in 3 different ways.

1. Path expansion
Any relative path in a config file is interpreted relative to the path of the config file itself.
This affects the path of local sources (csv, file, duckdb), SQL transform script paths,
and scripts and sources present in custom nodes.

2. Name expansion
The fully qualified name of an item is schema + path to file + group name + local name.
For sources, the local name is the key.
For transforms, the local name is the sql file's stem, unless specied as key.
For custom nodes, the local name is specied with a `name` key or omitted.

```yaml
- group_a:
  load:
    table_1:                                # local name: table_1
      file: layer.shp
  transform:
    - path/to/script_one.sql                # local name: script_one
    - better_name: script_two.sql           # local name: better_name

  custom:
    - name: node_x                          # local name: node_x
      prep: ...
```

3. Table expansion
Only affects sources (including the ones in custom nodes) and tables owned by custom nodes.
A source creates exactly one table, the full name of which is determined in a similar way as
for item names.
For sources in a load block, the table name is the item name.
For sources in a custom node, it is the full node name + local table name.
Tables (and other relations) listed as explicit dependencies or under a custom node's `creates`
section should always be speciefied with a fully qualified name.

```yaml
# <root>/schema/topic/makegis.yml
- group_a:
  load:
    table_1:                                # table name: schema.topic_group_a_table_1
      file: layer.shp

  custom:
    - load:
        table_2:                            # table name: schema.topic_group_a_table_2 
          file: local.shp
    - name: node_x
      deps:
        - table: other.fq_table_1           # used as is
      load:
        table_3:                            # table name: schema.topic_group_a_node_x_table_3
          file: output.shp
      run:
        - cmd: run.sh
          creates:
            - table: table_4                # table name: schema.topic_group_a_node_x_table_4
```

"""
log = logging.getLogger("makegis")


class ProjectError(Exception):
    """Project configuration issue"""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class Context:
    root_dir: Path
    conf_dir: Path
    group: str | None
    schema: str
    prefix: str

    def __init__(self, project_root: Path, cf_path: Path, group: str | None):
        self.root_dir: Path = project_root.absolute()
        relative_conf_dir = cf_path.parent.relative_to(self.root_dir)
        self.conf_dir: Path = (self.root_dir / relative_conf_dir).absolute()
        self.group = group
        # Might allow this later, but for now, config file in root project dir is not allowed
        if len(relative_conf_dir.parts) == 0:
            raise ProjectError(
                "Found config file in root project directory, must be in child directory"
            )
        # First part of relative conf path is schema
        self.schema = relative_conf_dir.parts[0]
        # Prefix is everything after schema + group if applicable
        prefix_parts = list(relative_conf_dir.parts[1:])
        if self.group is not None:
            prefix_parts.append(self.group)
        self.prefix = "_".join(prefix_parts)

    def for_node(self, node_name: str | None) -> NodeContext:
        """Return new node-specific instance"""
        ctx = copy.copy(self)
        if node_name is None:
            if not self.prefix:
                # No node name and no prefix. Node id would boil down to schema.
                # Might be confusing to don't allow.
                raise ProjectError(
                    f"Unnamed custom node in unnamed group under schema {self.schema}"
                )
        else:
            underscore = "_" if ctx.prefix else ""
            ctx.prefix += underscore + node_name
        return NodeContext(ctx)

    def expand_name(self, name: str | None):
        if name is not None:
            return f"{self.schema}.{self.prefix + '_' if self.prefix else ''}{name}"
        # Custom nodes may not be named locally but still inherit their parent context
        return f"{self.schema}.{self.prefix}"

    def expand_path(self, p: Path) -> Path:
        """
        Resolve path in context of config file.

        Relative paths are interpreted as relative to the config file's directory.
        Absolute paths are kept unchanged.
        """
        if p.is_absolute():
            return p
        return (self.conf_dir / p).absolute()

    def expand_database_item(self, dbi: DatabaseItem):
        """Expands name of a DatabaseItem in place"""
        dbi.name = self.expand_name(dbi.name)

    def expand_source_paths(self, src: LoadItem):
        """If applicable, expands paths of a LoadItem in place"""
        if isinstance(src, CSVSource):
            src.csv = self.expand_path(src.csv)
        elif isinstance(src, DuckDBSource):
            src.duckdb = self.expand_path(src.duckdb)
        elif isinstance(src, FileSource):
            src.file = self.expand_path(src.file)


class NodeContext(Context):

    def __init__(self, ctx: Context):
        self.root_dir = ctx.root_dir
        self.conf_dir = ctx.conf_dir
        self.group = ctx.group
        self.schema = ctx.schema
        self.prefix = ctx.prefix

    @property
    def node_name(self):
        return f"{self.schema}.{self.prefix}"


class ProjectDefaults(Defaults):
    target: str | None = None


class TargetConfig(BaseModel):
    """Describes a target database"""

    # optional description
    description: str | None = None
    host: str = "localhost"
    port: int = 5432
    user: str = "postgres"
    # database name
    db: str

    def conn_uri(self) -> str:
        s = self
        return f"postgresql://{s.user}@{s.host}:{s.port}/{s.db}"

    def conn_str(self) -> str:
        s = self
        return f"host={s.host} port={s.port} dbname={s.db} user={s.user}"


class ProjectFile(BaseModel):
    # Project file path
    path: Path
    src_dir: Path | None = None
    defaults: ProjectDefaults
    targets: dict[str, TargetConfig]

    @classmethod
    def from_path(cls, path: Path):
        log.debug(f"reading {path}")
        with open(path) as f:
            d = yaml.load(f, Loader)
        return cls.from_dict(path, d)

    @classmethod
    def from_yaml(cls, path: Path, s: str):
        d = yaml.load(s, Loader)
        return cls.from_dict(path, d)

    @classmethod
    def from_dict(cls, path: Path, d: dict):
        expand_dict_strings(d)
        d["path"] = path
        return ProjectFile(**d)


class ProjectSource(BaseModel):
    type: Literal["source"]
    name: str
    source: LoadItem

    def apply_context(self, ctx: Context):
        # DuckDB origin table is item name if not provided.
        # Capturing it before expanding name.
        if isinstance(self.source, DuckDBSource) and self.source.table is None:
            self.source.table = self.name
        self.name = ctx.expand_name(self.name)
        ctx.expand_source_paths(self.source)

    def apply_defaults(self, defaults: Defaults | None):
        if defaults is None or defaults.load is None:
            return
        fallback = {
            k: v
            for k, v in defaults.load.model_dump(exclude_unset=True).items()
            if k not in self.source.model_fields_set
        }
        self.source = self.source.model_copy(update=fallback)


class ProjectTransform(BaseModel):
    type: Literal["transform"]
    name: str
    script: Path

    def apply_context(self, ctx: Context):
        self.name = ctx.expand_name(self.name)
        self.script = ctx.expand_path(self.script)


# Just wrapping CustomNode as it already has a name attribute.
class ProjectNode(CustomNode):

    def apply_context(self, ctx: NodeContext):
        self.name = ctx.node_name
        # Dependencies must be fully qualified names, so no expanding needed here.
        # Just a quick check
        for dep in self.deps or []:
            assert "." in dep.name, "Custom node dependencies should be fully qualified"
        self.prep = [ctx.expand_path(p) for p in self.prep or []]
        if self.load is not None:
            # Exapand sources in place
            for src_name, src in self.load.items():
                # DuckDB origin table is item name if not provided.
                # Capturing it before expanding name.
                if isinstance(src, DuckDBSource) and src.table is None:
                    src.table = src_name
                ctx.expand_source_paths(src)
            # New dict with expanded names (keys)
            self.load = {ctx.expand_name(name): src for name, src in self.load.items()}
        if self.run is not None:
            for task in self.run:
                task.cmd = ctx.expand_path(task.cmd)
                # Explicit owned tables must be fully qualified names, so no expanding here.
                # Just a quick check
                for database_item in task.creates:
                    assert (
                        "." in database_item.name
                    ), "Custom node outputs should be fully qualified"

    def apply_defaults(self, defaults: Defaults | None):
        if defaults is None:
            return
        if self.load is not None and defaults.load is not None:
            fallback = defaults.load.model_dump(exclude_unset=True)

            new_sources = {}
            for name, source in self.load.items():
                fallback = {
                    k: v
                    for k, v in defaults.load.model_dump(exclude_unset=True).items()
                    if k not in source.model_fields_set
                }
                new_sources[name] = source.model_copy(update=fallback)
            self.load = new_sources


class Project:
    root: Path
    defaults: ProjectDefaults
    targets: dict[str, TargetConfig]
    sources: list[ProjectSource]
    transforms: list[ProjectTransform]
    custom: list[ProjectNode]

    def __init__(self, pf: Path | ProjectFile):
        """Create a project from a ProjectFile instance or a path to a project file"""
        if isinstance(pf, Path):
            pf = ProjectFile.from_path(pf)

        self.root = pf.path.parent.absolute()
        if pf.src_dir is not None:
            self.root = self.root / pf.src_dir
        self.defaults = pf.defaults
        self.targets = pf.targets
        self.sources = []
        self.transforms = []
        self.custom = []

    def load(self):
        """
        Find and enroll all configuration files present in project's directory tree.
        """
        for cf in discover_config_files(self.root):
            self.enroll_config_file(cf)

    def enroll_config_file(self, cf: ConfigFile):
        path = cf.path.parent.relative_to(self.root)
        for group in cf.groups:
            ctx = Context(self.root, cf.path, group.name)
            if group.load is not None:
                for name, item in group.load.items():
                    ps = ProjectSource(type="source", name=name, source=item)
                    ps.apply_context(ctx)
                    ps.apply_defaults(group.defaults)
                    ps.apply_defaults(self.defaults)
                    self.sources.append(ps)
            for transform in group.transform or []:
                if isinstance(transform, Path):
                    name = transform.stem
                    pt = ProjectTransform(type="transform", name=name, script=transform)
                else:
                    assert isinstance(transform, dict) and len(transform) == 1
                    name, path = next(iter(transform.items()))
                    assert "." not in name
                    pt = ProjectTransform(type="transform", name=name, script=path)
                pt.apply_context(ctx)
                self.transforms.append(pt)
            if group.custom is not None:
                for node in group.custom:
                    pn = ProjectNode.model_validate(dict(node))
                    pn.apply_context(ctx.for_node(node.name))
                    pn.apply_defaults(group.defaults)
                    pn.apply_defaults(self.defaults)
                    self.custom.append(pn)


def discover_config_files(root: Path) -> Iterator[ConfigFile]:
    """
    Provides an iteraror over all config files found in a project's directory tree.
    """
    log.debug(f"discovering makegis files under {root}")
    for path in root.rglob("makegis.yml"):
        yield ConfigFile.from_path(path)


def merge_defaults(main: Defaults, fallback: Defaults) -> Defaults:
    d = {k: v for k, v in fallback.model_dump(exclude_unset=True).items()} | {
        k: v for k, v in main.model_dump(exclude_unset=True).items()
    }
    return Defaults(**d)

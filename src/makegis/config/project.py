import copy
import logging
from pathlib import Path
from typing import Iterator

from pydantic import BaseModel
import yaml

from .makegis import ConfigFile
from .makegis import DatabaseItem
from .makegis import Defaults
from .makegis import CSVSource
from .makegis import DuckDBSource
from .makegis import FileSource
from .makegis import Command
from .makegis import Run
from .makegis import Source
from .makegis import Transform
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
The fully qualified name of an item is schema + path to file + optional group name + local name.
For sources, the local name is the key.
For transforms, the local name is the sql file's stem, unless specied as key.
For run nodes, the local name is specified with the `run` key or left blank.

```yaml
- name: group_a
  nodes:
    - load: table_1                           # local name: table_1
      file: layer.shp
    - transform: path/to/script_one.sql       # local name: script_one
    - run: node_x                             # local name: node_x
      deps: ...
```

3. Table expansion
Only affects sources (including the ones in run steps) and tables owned by run nodes.
A source creates exactly one table, the full name of which is determined in a similar way as
for item names.
For standalone sources, the table name is the item name.
For sources in a step block, it is the full node name + local table name.
Tables (and other relations) listed as explicit dependencies or under a run's `creates`
section should always be speciefied with a fully qualified name.

```yaml
# <root>/schema/topic/makegis.yml
- name: group_a:
  nodes:
    - load: table_1                         # table name: schema.topic_group_a_table_1
      file: layer.shp
    - load: table_2                         # table name: schema.topic_group_a_table_2 
      file: local.shp
    - run: node_x
      deps:
        - table: other.fq_table_1           # used as is
      creates:
        - table: schema.table_4             # used as is
      steps:
        - load: table_3                     # table name: schema.topic_group_a_node_x_table_3
          file: output.shp
        - cmd: make_table_4.sh
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
                # Might be confusing so don't allow.
                raise ProjectError(
                    f"Unnamed custom node in unnamed group under schema {self.schema}"
                )
        else:
            underscore = "_" if ctx.prefix else ""
            print("prefix was:", ctx.prefix)
            print("node_name was:", node_name)
            ctx.prefix += underscore + node_name
            print("prefix is:", ctx.prefix)
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
        p = p.expanduser()
        if p.is_absolute():
            return p
        return (self.conf_dir / p).absolute()

    def expand_database_item(self, dbi: DatabaseItem):
        """Expands name of a DatabaseItem in place"""
        dbi.name = self.expand_name(dbi.name)

    def expand_source_paths(self, src: Source):
        """If applicable, expands paths of a Source in place"""
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
        d = expand_dict_strings(d)
        d["path"] = path
        return ProjectFile(**d)


class ProjectSource(BaseModel):
    name: str
    source: Source

    def apply_context(self, ctx: Context):
        # DuckDB origin table is item name if not provided.
        # Capturing it before expanding name.
        # if isinstance(self.source, DuckDBSource) and self.source.table is None:
        # self.source.table = self.name
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
    name: str
    script: Path

    def apply_context(self, ctx: Context):
        self.name = ctx.expand_name(self.name)
        self.script = ctx.expand_path(self.script)


class ProjectRun(BaseModel):
    """Same as run but with explicit name and contextualized step items"""

    name: str
    deps: list[DatabaseItem] | None = None
    # Explicit declaration of owned tables created by commands
    # Not required for tables created by sources or transforms
    creates: list[DatabaseItem] | None = None
    steps: list[Command | ProjectSource | Transform]

    def apply_context(self, ctx: NodeContext):
        self.name = ctx.node_name
        # Dependencies must be fully qualified names, so no expanding needed here.
        # Just a quick check
        for dep in self.deps or []:
            assert "." in dep.name, "Custom node dependencies should be fully qualified"
        # Explicit owned tables must be fully qualified names, so no expanding here.
        # Just a quick check
        for db_item in self.creates or []:
            assert "." in db_item.name, "Run node outputs should be fully qualified"
        assert len(self.steps) > 0, "Run node needs at least one step"
        for step in self.steps:
            if isinstance(step, ProjectSource):
                step.apply_context(ctx)
            elif isinstance(step, Transform):
                step.transform = ctx.expand_path(step.transform)
            elif isinstance(step, Command):
                step.cmd = ctx.expand_path(step.cmd)
            else:
                assert False

    def apply_defaults(self, defaults: Defaults | None):
        if defaults is None:
            return

        for i, step in enumerate(self.steps):
            if defaults.load is not None and isinstance(step, ProjectSource):
                fallback = {
                    k: v
                    for k, v in defaults.load.model_dump(exclude_unset=True).items()
                    if k not in step.source.model_fields_set
                }
                self.steps[i] = step.model_copy(update=fallback)


class Project:
    root: Path
    defaults: ProjectDefaults
    targets: dict[str, TargetConfig]
    sources: list[ProjectSource]
    transforms: list[ProjectTransform]
    runs: list[ProjectRun]

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
        self.runs = []

    def load(self):
        """
        Find and enroll all configuration files present in project's directory tree.
        """
        for cf in discover_config_files(self.root):
            self.enroll_config_file(cf)

    def enroll_config_file(self, cf: ConfigFile):
        for group in cf.groups:
            ctx = Context(self.root, cf.path, group.name)
            for node in group.nodes:
                if isinstance(node, Source):
                    ps = ProjectSource(name=node.load, source=node)
                    ps.apply_context(ctx)
                    ps.apply_defaults(group.defaults)
                    ps.apply_defaults(self.defaults)
                    self.sources.append(ps)
                elif isinstance(node, Transform):
                    name = node.name if node.name is not None else node.transform.stem
                    pt = ProjectTransform(name=name, script=node.transform)
                    pt.apply_context(ctx)
                    self.transforms.append(pt)
                elif isinstance(node, Run):
                    # Steps are one of Source, Transform or Command
                    # Source's are wrapped in ProjectSource's so we can contextualize
                    # their name/destination table too.
                    steps = []
                    for step in node.steps:
                        if isinstance(step, Source):
                            steps.append(ProjectSource(name=step.load, source=step))
                        else:
                            steps.append(step)
                    name = node.run
                    pn = ProjectRun(
                        name=name or "",
                        deps=node.deps,
                        creates=node.creates,
                        steps=steps,
                    )
                    pn.apply_context(ctx.for_node(name))
                    pn.apply_defaults(group.defaults)
                    pn.apply_defaults(self.defaults)
                    self.runs.append(pn)
                else:
                    assert False


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

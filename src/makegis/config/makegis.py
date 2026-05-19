import logging
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator
from pydantic import model_validator
import yaml

from .utils import expand_dict_strings

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


log = logging.getLogger("makegis")


class CommonOptions(BaseModel):
    epsg: int | str | None = None
    # Name of column to use as primary key
    pk: str | None = None


class VectorOptions(BaseModel):
    geom_index: bool | None = None
    geom_column: str | None = None
    attributes_only: bool | None = None


class RasterOptions(BaseModel):
    raster_index: bool | None = None
    raster_column: str | None = None
    raster_constraints: bool | None = None
    tile_size: int | None = None


class LoadDefaults(CommonOptions, VectorOptions, RasterOptions):
    pass


class Defaults(BaseModel):
    model_config = ConfigDict(extra="forbid")
    load: LoadDefaults | None = None


class BaseSource(CommonOptions):
    load: str
    meta: Dict[str, str | int | float | None] | None = {}
    epsg: int | str | None = None

    model_config = ConfigDict(extra="forbid")


class CSVSource(BaseSource, VectorOptions):
    csv: Path
    # TODO:
    # x_column: str | None = None
    # y_column: str | None = None
    # keep_xy_columns: bool = False


class DuckDBSource(BaseSource, VectorOptions):
    duckdb: Path
    table: str | None = None

    def resolved_table(self):
        return self.load if self.table is None else self.table


class EsriSource(BaseSource, VectorOptions):
    esri: str
    f: Literal["pgeojson", "pjson"] = "pjson"


class FileSource(BaseSource, VectorOptions):
    file: Path
    layer: str | None = None


class RasterSource(BaseSource, RasterOptions):
    raster: Path


class WFSSource(BaseSource, VectorOptions):
    wfs: str


Source = CSVSource | DuckDBSource | EsriSource | FileSource | RasterSource | WFSSource


class Transform(BaseModel):
    transform: Path
    name: str | None = None

    model_config = ConfigDict(extra="forbid")


class DatabaseItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["table", "function"]
    name: str

    @model_validator(mode="before")
    @classmethod
    def desugar(cls, data: Any) -> Any:
        if isinstance(data, dict):
            assert len(data) == 1
            if "table" in data:
                tpe = "table"
            elif "function" in data:
                tpe = "function"
            return {"type": tpe, "name": data[tpe]}


class Command(BaseModel):
    cmd: Path


class Run(BaseModel):
    # Optional local name
    run: str | None = None
    deps: list[DatabaseItem] | None = None
    # Explicit declaration of owned tables created by commands
    # Not required for tables created by sources or transforms
    creates: list[DatabaseItem] | None = None
    steps: list[Command | Source | Transform]

    model_config = ConfigDict(extra="forbid")


class Group(BaseModel):
    name: str | None = None
    defaults: Defaults | None = None
    nodes: list[Source | Transform | Run]


class ConfigFile(BaseModel):
    """A makegis.yml file and its contents"""

    path: Path
    groups: list[Group]

    @classmethod
    def from_path(cls, path: Path):
        log.debug(f"reading {path}")
        with open(path) as f:
            s = f.read()
        return cls.from_yaml(path, s)

    @classmethod
    def from_yaml(cls, path: Path, s: str):
        y = yaml.load(s, Loader)
        if isinstance(y, list):
            return cls.from_list(path, y)
        else:
            return cls.from_list(path, [y])

    @classmethod
    def from_list(cls, path: Path, ls: list):
        groups = [Group(**expand_dict_strings(item)) for item in ls]
        return cls(path=path, groups=groups)

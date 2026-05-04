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


LoadItem = CSVSource | DuckDBSource | EsriSource | FileSource | RasterSource | WFSSource


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


class RunTask(BaseModel):
    cmd: Path
    creates: List[DatabaseItem]


class CustomNode(BaseModel):
    # Optional local name
    name: str | None = None
    deps: list[DatabaseItem] | None = None
    prep: list[Path] | None = []

    load: dict[str, LoadItem] | None = None
    run: list[RunTask] | None = None
    cleanup: List[Path] | None = None


class Group(BaseModel):
    name: str | None = None
    defaults: Defaults | None = None
    load: dict[str, LoadItem] | None = None
    transform: list[Path | dict[str, Path]] | None = None
    custom: list[CustomNode] | None = None

    @field_validator("transform", mode="after")
    @classmethod
    def ensure_single_sql_maps(cls, value: Path | dict[str, Path]):
        """
        A sql item can be path or name:path mapping.
        This validator checks that dict items contain exactly 1 mapping.

        ```yaml
        - sql:
            - path_one.sql           # path only, ok
            - two: path_two.sql      # single name:path, ok
            - this: is.sql           # map > 1, not ok
              not: allowed.sql
        ```
        """
        if isinstance(value, dict) and len(value) != 1:
            raise ValueError("sql name:path mapping is not of length 1")
        return value


class ConfigFile(BaseModel):
    """A makegis.yml file and its contents"""

    path: Path
    groups: list[Group]

    @classmethod
    def from_path(cls, path: Path):
        log.debug(f"reading {path}")
        with open(path) as f:
            ls = yaml.load(f, Loader)
        return cls.from_list(path, ls)

    @classmethod
    def from_yaml(cls, path: Path, s: str):
        ls = yaml.load(s, Loader)
        return cls.from_list(path, ls)

    @classmethod
    def from_list(cls, path: Path, ls: list):
        groups = [Group(**expand_dict_strings(item)) for item in ls]
        return cls(path=path, groups=groups)

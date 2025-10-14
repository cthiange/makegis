from pathlib import Path
from typing import Dict
from typing import List
from typing import Optional
from typing import Literal

from pydantic import BaseModel
from pydantic import model_validator
from pydantic import ValidationError
import yaml

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

from .utils import expand_dict_strings


class LoadDefaults(BaseModel):
    epsg: int | str | None = None
    geom_index: bool | None = None
    geom_column: str | None = None


class BaseSourceBlock(BaseModel):
    epsg: int | str | None = None
    geom_index: bool | None = None
    geom_column: str | None = None


class EsriSourceBlock(BaseSourceBlock):
    type: Literal["esri"] = "esri"
    url: str
    f: Literal["pjson", "pgeojson"]


class DuckDBSourceBlock(BaseSourceBlock):
    type: Literal["duckdb"] = "duckdb"
    path: Path
    table: Optional[str] = None


class FileSourceBlock(BaseSourceBlock):
    type: Literal["file"] = "file"
    path: Path
    layer: str | None = None


class WFSSourceBlock(BaseSourceBlock):
    type: Literal["wfs"] = "wfs"
    url: str


type SourceBlock = EsriSourceBlock | DuckDBSourceBlock | FileSourceBlock | WFSSourceBlock

SOURCE_KEYS = set(["esri", "duckdb", "file", "wfs"])


class LoadItem(BaseModel):
    name: str
    src: SourceBlock
    meta: Dict[str, str | int | float | None]

    @classmethod
    def from_kv(cls, k: str, v: Dict):
        name = k
        meta = v.pop("meta", {})
        matched_source_keys = [sk for sk in SOURCE_KEYS if sk in v]
        if len(matched_source_keys) == 0:
            raise RuntimeError(
                f"Missing source key in load block item, execting one of {SOURCE_KEYS}"
            )
        elif len(matched_source_keys) > 1:
            raise RuntimeError(
                f"Too many source keys in load block item, expecting exactly one of {SOURCE_KEYS}"
            )
        if "esri" in matched_source_keys:
            url = v.pop("esri")
            src = EsriSourceBlock(url=url, **v)
        elif "duckdb" in matched_source_keys:
            path = v.pop("duckdb")
            src = DuckDBSourceBlock(path=path, **v)
        elif "file" in matched_source_keys:
            path = v.pop("file")
            src = FileSourceBlock(path=path, **v)
        elif "wfs" in matched_source_keys:
            url = v.pop("wfs")
            src = WFSSourceBlock(url=url, **v)
        else:
            raise NotImplementedError("Unhandled source key in load block item")

        return LoadItem(name=name, src=src, meta=meta)


class SQLTransform(BaseModel):
    path: Path


class LoadBlock(BaseModel):
    defaults: LoadDefaults
    items: List[LoadItem]

    @classmethod
    def from_dict(cls, d: Dict):
        defaults = LoadDefaults(**d.pop("defaults", {}))
        items = [LoadItem.from_kv(k, v) for k, v in d.items()]
        return LoadBlock(defaults=defaults, items=items)


class TransformBlock(BaseModel):
    transforms: List[SQLTransform]

    @classmethod
    def from_sequence(cls, s: List):
        transforms = [SQLTransform(path=p) for p in s]
        return TransformBlock(transforms=transforms)


class DatabaseItem(BaseModel):
    type: Literal["table", "function"]
    name: str

    @classmethod
    def from_dict(cls, d: Dict):
        assert (
            len(d) == 1
        ), "each item in a 'creates' block must have exactly 1 key e.g. - table: name"
        k, v = next(iter(d.items()))
        return DatabaseItem(type=k, name=v)


class RunTask(BaseModel):
    cmd: str
    creates: List[DatabaseItem]

    @classmethod
    def from_dict(cls, d: Dict):
        creates = [DatabaseItem.from_dict(item) for item in d.pop("creates", [])]
        return RunTask(creates=creates, **d)


class DoBlock(BaseModel):
    """The 'do' key in a 'node' block"""

    load: Optional[LoadBlock] = None
    run: Optional[List[RunTask]] = None

    @model_validator(mode="after")
    def at_least_one(self):
        if self.load is None and self.run is None:
            raise ValidationError(
                "A node's do block must have a 'load' and/or a 'run' key"
            )
        return self

    @classmethod
    def from_dict(cls, d: Dict):
        load, tasks = None, None
        if "load" in d:
            load = LoadBlock.from_dict(d["load"])
        if "run" in d:
            tasks = [RunTask.from_dict(t) for t in d["run"]]
        return DoBlock(load=load, run=tasks)


class NodeBlock(BaseModel):
    """Top-level 'node' block in a makegis.yml file"""

    deps: List[DatabaseItem] | None = []
    prep: List[str] | None = []
    do: DoBlock
    post: List[str] | None = []
    cleanup: List[str] | None = []

    @classmethod
    def from_dict(cls, d: Dict):
        deps = [DatabaseItem.from_dict(d) for d in d.pop("deps", []) or []]
        do = DoBlock.from_dict(d.pop("do"))
        return NodeBlock(do=do, **d)


class MakeGISConfig(BaseModel):
    block: LoadBlock | TransformBlock | NodeBlock
    type: Literal["load", "transform", "node"]

    @classmethod
    def from_file(cls, path: Path):
        print(f"debug - reading {path}")
        with open(path) as f:
            d = yaml.load(f, Loader)
        return cls.from_dict(d)

    @classmethod
    def from_yaml(cls, s: str):
        d = yaml.load(s, Loader)
        return cls.from_dict(d)

    @classmethod
    def from_dict(cls, d: Dict):
        expand_dict_strings(d)
        assert len(d) == 1
        if "load" in d:
            typ = "load"
            block = LoadBlock.from_dict(d["load"])
        elif "transform" in d:
            typ = "transform"
            block = TransformBlock.from_sequence(d["transform"])
        elif "node" in d:
            typ = "node"
            block = NodeBlock.from_dict(d["node"])
        else:
            raise RuntimeError("Unknown makegis file key")
        return MakeGISConfig(type=typ, block=block)

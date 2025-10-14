from pathlib import Path
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class BaseSource:
    # Explicit srid of geometry in source dataset
    epsg: int | None


@dataclass(frozen=True)
class EsriSource(BaseSource):
    url: str
    f: Literal["pgeojson", "pjson"]


@dataclass(frozen=True)
class DuckDBSource(BaseSource):
    # Path to database file
    path: Path
    # Fully qualified name of table to import
    table: str


@dataclass(frozen=True)
class FileSource(BaseSource):
    path: Path
    # Optional layer name for file formats supporting it
    layer: str | None


@dataclass(frozen=True)
class WFSSource(BaseSource):
    url: str


type Source = EsriSource | DuckDBSource | FileSource | WFSSource


@dataclass(frozen=True)
class Destination:
    schema: str
    table: str
    # Desired srid of geometry column in destination table
    epsg: int | None
    # Name to assign to geometry column. Keep original if None.
    geom_column: str | None
    # Wether to index geometries or not.
    # Defaults to False to be conservative.
    geom_index: bool


@dataclass(frozen=True)
class LoadJob:
    src: Source
    dst: Destination

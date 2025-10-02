from pathlib import Path
from dataclasses import dataclass


@dataclass(frozen=True)
class BaseSource:
    # Explicit srid of geometry in source dataset
    epsg: int | None


@dataclass(frozen=True)
class DuckDBSource(BaseSource):
    # Path to database file
    path: Path
    # Fully qualified name of table to import
    table: str


@dataclass(frozen=True)
class FileSource(BaseSource):
    path: Path


@dataclass(frozen=True)
class WFSSource(BaseSource):
    url: str


type Source = DuckDBSource | FileSource | WFSSource


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

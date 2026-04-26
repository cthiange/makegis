from pathlib import Path
from dataclasses import dataclass
from typing import Literal


from pydantic import BaseModel
from pydantic import Field


class BaseSource(BaseModel):
    # Explicit srid of geometry in source dataset
    epsg: int | None
    # Name of column to use as primary key
    pk: str | None

    model_config = {"frozen": True}


class CSVSource(BaseSource):
    path: Path


class EsriSource(BaseSource):
    url: str
    f: Literal["pgeojson", "pjson"]


class DuckDBSource(BaseSource):
    # Path to database file
    path: Path
    # Fully qualified name of table to import
    table: str


class FileSource(BaseSource):
    path: Path
    # Optional layer name for file formats supporting it
    layer: str | None


class RasterSource(BaseSource):
    path: Path


class WFSSource(BaseSource):
    url: str


type Source = CSVSource | DuckDBSource | EsriSource | FileSource | RasterSource | WFSSource


# TODO: refactor into VectorSource and RasterSource
class Destination(BaseModel):
    table_schema: str
    table_name: str
    # Desired srid of geometry column in destination table
    epsg: int | None = None
    # Name to assign to geometry column. Keep original if None.
    geom_column: str | None = None
    # Wether to index geometries or not.
    # Defaults to False to be conservative.
    geom_index: bool = Field(default=False)
    # Load attributes only and skip geometry.
    attributes_only: bool = Field(default=False)
    # Name to assign to raster column.
    raster_column: str = Field(default="rast")
    # Wether to index raster bounds
    raster_index: bool = Field(default=False)
    # Wether to set the standard set of raster constraints
    raster_constraints: bool = Field(default=False)
    # Raster tile size
    tile_size: int | None = None

    model_config = {"frozen": True}


@dataclass(frozen=True)
class LoadJob(BaseModel):
    src: Source
    dst: Destination

    model_config = {"frozen": True}

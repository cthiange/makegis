import subprocess
import os

import duckdb
import psycopg
from psycopg import sql

from .config import TargetConfig
from .core.load import LoadJob
from .core.load import CSVSource
from .core.load import EsriSource
from .core.load import DuckDBSource
from .core.load import WFSSource
from .core.load import FileSource
from .core.load import RasterSource
from .core.load import Destination


def load_table(target: TargetConfig, job: LoadJob):
    match job.src:
        case CSVSource():
            load_csv(target.conn_str(), job.src, job.dst)
        case EsriSource():
            load_esri(target.conn_uri(), job.src, job.dst)
        case DuckDBSource():
            ddb2pg(target.conn_str(), job.src, job.dst)
        case WFSSource():
            load_wfs(target.conn_uri(), job.src, job.dst)
        case FileSource():
            match job.src.path.suffix:
                case ".gdb":
                    load_gdb(target.conn_uri(), job.src, job.dst)
                case ".shp":
                    load_shp(target.conn_uri(), job.src, job.dst)
                case _:
                    raise NotImplementedError(
                        f"Loading {job.src.path.suffix} files is not supported yet"
                    )
        case RasterSource():
            raster2pgsql(target, job.src, job.dst)
        case _:
            raise NotImplementedError


class Table:

    def __init__(self, dst: Destination):
        schema = dst.schema
        name = dst.table
        self._schema = schema
        self._name = name
        self.ident = sql.Identifier(schema, name)
        self.literal_schema = sql.Literal(schema)
        self.literal_table = sql.Literal(name)

    def __str__(self):
        return f"{self._schema}.{self._name}"


class Column:

    def __init__(self, name: str):
        self._name = name
        self.ident = sql.Identifier(name)
        self.literal = sql.Literal(name)

    def __str__(self):
        return self._name


# Helper function to load a single table from DuckDB to Postgres
def ddb2pg(
    conn_str: str,
    src: DuckDBSource,
    dst: Destination,
):
    print(
        f"postgis - loading duckdb table from {src.path}:{src.table} to {dst.schema}.{dst.table}"
    )
    if src.pk is not None:
        raise NotImplementedError("Explicit PK not implemented for DuckDB sources")
    db = duckdb.connect()
    db.sql("install spatial;")
    db.sql("load spatial;")
    db.sql(f"attach '{src.path}' as src (READ_ONLY);")
    db.sql(f"attach '{conn_str}' as pg (TYPE postgres);")

    # https://duckdb.org/docs/configuration/pragmas.html#table-information
    columns = db.sql(f"pragma table_info('src.{src.table}');").fetchall()

    statement = f"create or replace table pg.{dst.schema}.{dst.table} as select"
    for i, col, dtype, not_null, default, pk in columns:
        if dtype == "GEOMETRY":
            # Convert geometry to hexwkb for PostGIS
            statement += f"\n    st_ashexwkb({col}) as {col},"
        else:
            # Strings need to be quoted
            statement += f'\n    "{col}",'

    # Remove last trailing comma
    statement = statement[:-1]

    statement += f"\nfrom src.{src.table};"

    # print(statement)
    db.sql(statement)
    db.close()

    # Add constraints to new postgres table
    with psycopg.connect(conn_str) as conn:
        table = Table(dst)

        for i, col_str, dtype, not_null, default, pk in columns:
            col = Column(col_str)
            if not_null:
                conn.execute(
                    sql.SQL(
                        "alter table {table} alter column {col} set not null"
                    ).format(table=table.ident, col=col.ident)
                )
            if default:
                conn.execute(
                    sql.SQL(
                        "alter table {table} alter column {col} set default {default}"
                    ).format(
                        table=table.ident, col=col.ident, default=sql.Literal(default)
                    )
                )
            # Process geo columns
            if dtype == "GEOMETRY":
                _process_duckdb_geo_column(
                    conn,
                    table,
                    col,
                    src_epsg=src.epsg,
                    dst_epsg=dst.epsg,
                    geom_index=dst.geom_index,
                )

        # Add primary key if any
        pks = [col_str for i, col_str, dtype, not_null, default, pk in columns if pk]
        pks = [sql.Identifier(k) for k in pks]
        if pks:
            print("debug - adding primary key")
            stmt = sql.SQL("alter table {table} add primary key ({key})").format(
                table=table.ident,
                key=sql.SQL(",").join(pks),
            )
            print(f"trace - {stmt.as_string()}")
            conn.execute(stmt)

        conn.commit()


def _process_duckdb_geo_column(
    conn: psycopg.Connection,
    table: Table,
    col: Column,
    src_epsg: int | None,
    dst_epsg: int | None,
    geom_index: bool = False,
):
    print(f"processing geometry column '{col}' for table '{table}'")
    print(
        f"trace - src_epsg: {src_epsg}, dst_epsg: {dst_epsg}, geom_index: {geom_index}"
    )
    if dst_epsg is None:
        # Because epsg setting is either dst or src:dst, src cannot be set without dst
        assert src_epsg is None, "src_epsg cannot be set without a dst_epsg"
        # No epsg settings, just cast to generic geom
        _duckdb_cast_geom_without_srid(conn, table, col)
    if dst_epsg is not None:
        if src_epsg is None or src_epsg == dst_epsg:
            # Just set srid dst_epsg
            _duckdb_cast_geom_with_srid(conn, table, col, dst_epsg)
        else:
            # Set srid and transform
            _duckdb_cast_geom_with_transform(conn, table, col, src_epsg, dst_epsg)

    if geom_index:
        _ensure_gist_index(conn, table, col)


def _duckdb_cast_geom_without_srid(
    conn: psycopg.Connection,
    table: Table,
    col: Column,
):
    """Casts the geo column to a generic geometry type"""
    print("trace - _duckdb_cast_geom_without_srid")
    conn.execute(
        sql.SQL(
            "alter table {table} alter column {col} type geometry using {col}::geometry"
        ).format(table=table.ident, col=col.ident)
    )


def _duckdb_cast_geom_with_srid(
    conn: psycopg.Connection,
    table: Table,
    col: Column,
    epsg: int,
):
    """Casts the geo column to a geometry type with srid"""
    print("trace - _duckdb_cast_geom_with_srid")
    conn.execute(
        sql.SQL(
            """
            alter table {table}
                alter column {col} type geometry(GEOMETRY, {epsg})
                using {col}::geometry(GEOMETRY, {epsg})
            """
        ).format(table=table.ident, col=col.ident, epsg=sql.Literal(epsg))
    )


def _duckdb_cast_geom_with_transform(
    conn: psycopg.Connection,
    table: Table,
    col: Column,
    src_epsg: int,
    dst_epsg: int,
):
    """Casts the geo column to a geometry type while transforming between srids"""
    print("trace - _duckdb_cast_geom_with_transform")
    conn.execute(
        sql.SQL(
            """
            alter table {table}
                alter column {col} type geometry(GEOMETRY, {dst_epsg})
                using st_transform({col}::geometry(GEOMETRY, {src_epsg}), {dst_epsg})
            """
        ).format(
            table=table.ident,
            col=col.ident,
            src_epsg=sql.Literal(src_epsg),
            dst_epsg=sql.Literal(dst_epsg),
        )
    )


def _ensure_gist_index(conn: psycopg.Connection, table: Table, col: Column):
    if _column_has_index(conn, table, col, "gist"):
        return

    print(f"debug - creating index on {col} in {table}")
    conn.execute(
        sql.SQL(
            """
            create index on {table} using GIST({col});
            """
        ).format(table=table.ident, col=col.ident)
    )


def _column_has_index(
    conn: psycopg.Connection,
    table: Table,
    col: Column,
    index_type: str,
) -> bool:
    qry = sql.SQL(
        """
        select
            c.relname as index_name,
            x.indisunique as is_unique,
            x.indisprimary as is_primary,
            x.indisvalid as is_valid,
            x.indisready as is_ready,
            am.amname as index_type,
            a.attname as column_name
        from pg_class t
        join pg_namespace n on n.oid = t.relnamespace
        join pg_index x on t.oid = x.indrelid
        join pg_class c on c.oid = x.indexrelid
        join pg_am am on c.relam = am.oid
        join pg_attribute a on a.attrelid = t.oid and a.attnum = any(x.indkey)
        where n.nspname = {schema}
            and t.relname = {table}
            and a.attname = {column}
            and am.amname ilike {index_type};
    """
    ).format(
        schema=table.literal_schema,
        table=table.literal_table,
        column=col.literal,
        index_type=sql.Literal(index_type),
    )
    rows = conn.execute(qry).fetchall()
    return len(rows) > 0


def load_csv(
    conn_str: str,
    src: FileSource,
    dst: Destination,
):
    db = duckdb.connect()
    db.sql(f"attach '{conn_str}' as pg (TYPE postgres);")
    db.sql(
        f"""
        create or replace table pg.{dst.schema}.{dst.table} as
            select * from '{src.path}';
        """
    )
    db.close()

    with psycopg.connect(conn_str) as conn:
        if src.pk is not None:
            conn.execute(f"alter table {dst.schema}.{dst.table} add primary key ({src.pk})")


def load_wfs(
    conn_str: str,
    src: WFSSource,
    dst: Destination,
):
    """
    Uses local ogr2ogr executable.
    """
    cmd = f'ogr2ogr -f "PostgreSQL" PG:"{conn_str}"'
    cmd += f' WFS:"{src.url}"'
    options = ""
    options += f' -nln "{dst.schema}.{dst.table}"'
    options += f" -lco FID={'gid' if src.pk is None else src.pk}"
    if dst.geom_column is not None:
        options += f" -lco GEOMETRY_NAME={dst.geom_column}"
    if src.epsg is not None:
        options += f" -s_srs EPSG:{src.epsg}"
    if dst.epsg is not None:
        options += f" -t_srs EPSG:{dst.epsg}"
    options += " --config OGR_PG_ENABLE_METADATA=NO"
    options += " -overwrite"
    if dst.geom_index:
        options += " -lco SPATIAL_INDEX=GIST"
    else:
        options += " -lco SPATIAL_INDEX=NONE"
    cmd += options

    ret = run_ogr_cmd(cmd, f"{dst.schema}.{dst.table}")
    print(f"debug - return code: {ret}")
    if ret != 0:
        print(f"error - loading wfs failed with code ({ret})")
        raise RuntimeError("loading wfs failed")


def load_gdb(
    conn_str: str,
    src: FileSource,
    dst: Destination,
):
    """
    Uses local ogr2ogr executable.
    """
    cmd = f'ogr2ogr -f "PostgreSQL" PG:"{conn_str}"'
    assert src.layer is not None
    cmd += f' "{src.path}" "{src.layer}"'
    options = ""
    options += f' -nln "{dst.schema}.{dst.table}"'
    options += f" -lco FID={'gid' if src.pk is None else src.pk}"
    options += " -progress"
    if dst.geom_column is not None:
        options += f" -lco GEOMETRY_NAME={dst.geom_column}"
    if src.epsg is not None:
        options += f" -s_srs EPSG:{src.epsg}"
    if dst.epsg is not None:
        options += f" -t_srs EPSG:{dst.epsg}"
    options += " --config OGR_PG_ENABLE_METADATA=NO"
    options += " -overwrite"
    if dst.geom_index:
        options += " -lco SPATIAL_INDEX=GIST"
    else:
        options += " -lco SPATIAL_INDEX=NONE"
    cmd += options

    ret = run_ogr_cmd(cmd, f"{dst.schema}.{dst.table}")
    print(f"debug - return code: {ret}")
    if ret != 0:
        print(f"error - loading gdb failed with code ({ret})")
        raise RuntimeError("loading gdb layer failed")


def load_shp(
    conn_str: str,
    src: FileSource,
    dst: Destination,
):
    """
    Uses local ogr2ogr executable.
    """
    cmd = f'ogr2ogr -f "PostgreSQL" PG:"{conn_str}"'
    cmd += f' "{src.path}" '
    options = ""
    options += f' -nln "{dst.schema}.{dst.table}"'
    options += f" -lco FID={'gid' if src.pk is None else src.pk}"
    options += " -progress"
    if dst.geom_column is not None:
        options += f" -lco GEOMETRY_NAME={dst.geom_column}"
    if src.epsg is not None:
        options += f" -s_srs EPSG:{src.epsg}"
    if dst.epsg is not None:
        options += f" -t_srs EPSG:{dst.epsg}"
    options += " --config OGR_PG_ENABLE_METADATA=NO"
    options += " -overwrite"
    options += " -nlt PROMOTE_TO_MULTI"
    if dst.geom_index:
        options += " -lco SPATIAL_INDEX=GIST"
    else:
        options += " -lco SPATIAL_INDEX=NONE"
    cmd += options

    ret = run_ogr_cmd(cmd, f"{dst.schema}.{dst.table}")
    print(f"debug - return code: {ret}")
    if ret != 0:
        print(f"error - loading shp failed with code ({ret})")
        raise RuntimeError("loading shp layer failed")


def load_esri(
    conn_str: str,
    src: EsriSource,
    dst: Destination,
):
    """
    Uses local ogr2ogr executable.
    """
    cmd = f'ogr2ogr -f "PostgreSQL" PG:"{conn_str}"'
    # This will do for now but might want to expose more query parameters
    cmd += f' "{src.url}/query?where=1=1&outFields=*&f={src.f}"'
    options = ""
    options += f' -nln "{dst.schema}.{dst.table}"'
    options += f" -lco FID={'gid' if src.pk is None else src.pk}"
    if dst.geom_column is not None:
        options += f" -lco GEOMETRY_NAME={dst.geom_column}"
    if src.epsg is not None:
        options += f" -s_srs EPSG:{src.epsg}"
    if dst.epsg is not None:
        options += f" -t_srs EPSG:{dst.epsg}"
    options += " --config OGR_PG_ENABLE_METADATA=NO"
    options += " -overwrite"
    options += " -nlt PROMOTE_TO_MULTI"
    if dst.geom_index:
        options += " -lco SPATIAL_INDEX=GIST"
    else:
        options += " -lco SPATIAL_INDEX=NONE"
    # Seems to be used anyways but including to be explicit
    options += " -oo FEATURE_SERVER_PAGING=YES"
    cmd += options

    ret = run_ogr_cmd(cmd, f"{dst.schema}.{dst.table}")
    print(f"debug - return code: {ret}")
    if ret != 0:
        print(f"error - loading esri failed with code ({ret})")
        raise RuntimeError("loading esri failed")


def run_ogr_cmd(cmd: str, label: str) -> int:

    # Check if we're using OSGeo4W
    o4w_env = os.environ.get("MKGS_O4W_ENV", None)
    if o4w_env is not None:
        cmd = f"@echo off && {o4w_env} && " + cmd

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        shell=True,
    )

    if process.stdout is not None:
        for line in process.stdout:
            print(f"load ({label}) | {line}", end="")

    return process.wait()


def raster2pgsql(
    target: TargetConfig,
    src: EsriSource,
    dst: Destination,
):
    psql = os.environ.get("MKGS_PSQL", "psql")
    r2p = os.environ.get("MKGS_RASTER2PGSQL", "raster2pgsql")

    # raster2pgsql options
    options = []

    # -f: explicit raster column name
    options += ["-f", dst.raster_column]

    # -d: drop table and recreate it
    options += ["-d"]

    # Add tile size
    assert dst.tile_size is not None
    t = dst.tile_size
    options += ["-t", f"{t}x{t}"]

    if dst.epsg is not None:
        options += ["-s", str(dst.epsg)]

    if dst.raster_index:
        options += ["-I"]

    if dst.raster_constraints:
        options += ["-C"]


    r2p_cmd = [r2p] + options + [src.path, f"{dst.schema}.{dst.table}"]

    psql_cmd = [psql, "-h", target.host, "-p", str(target.port), "-d", target.db, "-U", target.user]


    r2p_process = subprocess.Popen(r2p_cmd)

    # Pipe raster2pgsql output to psql
    psql_process = subprocess.Popen(
        psql_cmd,
        stdin=r2p_process.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    label = f"{dst.schema}.{dst.table}"
    if psql_process.stdout is not None:
        for line in psql_process.stdout:
            print(f"load ({label}) | {line}", end="")

    ret = r2p_process.wait()
    print(f"debug - raster2pgsql return code: {ret}")
    if ret != 0: 
        print(f"error - loading raster (raster2pgsql) failed with code ({ret})")
        raise RuntimeError("loading raster failed")

    ret = psql_process.wait()
    print(f"debug - psql return code: {ret}")
    if ret != 0: 
        print(f"error - loading raster (psql) failed with code ({ret})")
        raise RuntimeError("loading raster failed")


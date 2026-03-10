from datetime import timezone
import logging
import os
import re
import subprocess

import duckdb
import psycopg
from psycopg import sql

from ..config.root import TargetConfig
from ..core.load import CSVSource
from ..core.load import Destination
from ..core.load import DuckDBSource
from ..core.load import EsriSource
from ..core.load import FileSource
from ..core.load import LoadJob
from ..core.load import RasterSource
from ..core.load import WFSSource
from ..core.transforms import Transform
from ..errors import FailedNodeRun
from ..utils import capture_logs
from ..journal import RunRecord
from ..journal import Manifest

log = logging.getLogger("makegis")


class PostgisTarget:

    def __init__(self, config: TargetConfig):
        self.host = config.host
        self.user = config.user
        self.port = config.port
        self.user = config.user
        self.db = config.db
        self.conn_str = config.conn_str()
        self.conn_uri = config.conn_uri()

    def load_table(self, job: LoadJob):
        match job.src:
            case CSVSource():
                load_csv(self.conn_str, job.src, job.dst)
            case EsriSource():
                load_esri(self.conn_uri, job.src, job.dst)
            case DuckDBSource():
                ddb2pg(self.conn_str, job.src, job.dst, launder=True)
            case WFSSource():
                load_wfs(self.conn_uri, job.src, job.dst)
            case FileSource():
                match job.src.path.suffix:
                    case ".gdb":
                        load_gdb(self.conn_uri, job.src, job.dst)
                    case ".shp":
                        load_shp(self.conn_uri, job.src, job.dst)
                    case _:
                        raise NotImplementedError(
                            f"Loading {job.src.path.suffix} files is not supported yet"
                        )
            case RasterSource():
                raster2pgsql(target, job.src, job.dst)
            case _:
                raise NotImplementedError

    def run_transform(self, transform: Transform):
        path = transform.sql
        assert path.suffix == ".sql"
        psql = os.environ.get("MKGS_PSQL", "psql")
        cmd = [
            psql,
            "-h",
            self.host,
            "-U",
            self.user,
            "-p",
            str(self.port),
            "-d",
            self.db,
            "-v",
            "ON_ERROR_STOP=ON",
            "-f",
            path,
        ]

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        capture_logs(process.stdout, f"transform ({path.name})")

        ret = process.wait()

        if ret != 0:
            raise RuntimeError(f"error while running sql transform {path}")

    def init_journal(self):
        with psycopg.connect(self.conn_str) as conn:
            conn.execute("""
                create table if not exists _makegis_runs (
                    node_id text not null,
                    started timestamp not null,
                    completed timestamp not null,
                    db_user text not null,
                    hostname text not null,
                    mkgs_version text not null,
                    repo_revision text
                );
                """)
            conn.commit()

    def ensure_schema(self, schema: str):
        statement = sql.SQL("create schema if not exists {schema}").format(
            schema=sql.Identifier(schema)
        )
        with psycopg.connect(self.conn_str) as conn:
            log.debug(statement)
            conn.execute(statement)

    def log_event(self, record: RunRecord):
        with psycopg.connect(self.conn_str) as conn:
            conn.execute(
                """
                insert into _makegis_runs(
                    node_id,
                    started,
                    completed,
                    db_user,
                    hostname,
                    mkgs_version,
                    repo_revision
                ) values (%s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    record.node_id,
                    record.started,
                    record.completed,
                    record.db_user,
                    record.hostname,
                    record.mkgs_version,
                    record.repo_hash,
                ),
            )

    def fetch_manifest(self) -> Manifest:
        with psycopg.connect(self.conn_str) as conn:
            rows = conn.execute("""
                select node_id
                    , max(completed) as last_run_utc
                from _makegis_runs
                group by 1;
                """).fetchall()
            # Map node ids to timestamp coverted from utc to local.
            return {
                row[0]: row[1].replace(tzinfo=timezone.utc).astimezone() for row in rows
            }


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
def ddb2pg(conn_str: str, src: DuckDBSource, dst: Destination, launder=True):
    log.info(
        f"postgis - loading duckdb table from {src.path}:{src.table} to {dst.schema}.{dst.table}"
    )
    if src.pk is not None:
        raise NotImplementedError("Explicit PK not implemented for DuckDB sources")

    if launder:
        # Replace non alpha-numeric characters and convert to lowercase
        format_column = lambda col: re.sub(r"[^a-zA-Z0-9]", "_", col).lower()
    else:
        # Do nothing
        format_column = lambda col: col

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
            statement += f"\n    st_ashexwkb({col}) as {format_column(col)},"
        else:
            # Strings need to be quoted in case they're not laundered
            statement += f'\n    "{col}" as "{format_column(col)}",'

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
            col = Column(format_column(col_str))
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
        pks = [
            format_column(col_str)
            for i, col_str, dtype, not_null, default, pk in columns
            if pk
        ]
        pks = [sql.Identifier(k) for k in pks]
        if pks:
            log.info("adding primary key")
            stmt = sql.SQL("alter table {table} add primary key ({key})").format(
                table=table.ident,
                key=sql.SQL(",").join(pks),
            )
            log.debug(f"{stmt.as_string()}")
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
    log.info(f"processing geometry column '{col}' for table '{table}'")
    log.debug(f"src_epsg: {src_epsg}, dst_epsg: {dst_epsg}, geom_index: {geom_index}")
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
    log.debug("_duckdb_cast_geom_without_srid")
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
    log.debug("_duckdb_cast_geom_with_srid")
    conn.execute(sql.SQL("""
            alter table {table}
                alter column {col} type geometry(GEOMETRY, {epsg})
                using {col}::geometry(GEOMETRY, {epsg})
            """).format(table=table.ident, col=col.ident, epsg=sql.Literal(epsg)))


def _duckdb_cast_geom_with_transform(
    conn: psycopg.Connection,
    table: Table,
    col: Column,
    src_epsg: int,
    dst_epsg: int,
):
    """Casts the geo column to a geometry type while transforming between srids"""
    log.debug("_duckdb_cast_geom_with_transform")
    conn.execute(
        sql.SQL("""
            alter table {table}
                alter column {col} type geometry(GEOMETRY, {dst_epsg})
                using st_transform({col}::geometry(GEOMETRY, {src_epsg}), {dst_epsg})
            """).format(
            table=table.ident,
            col=col.ident,
            src_epsg=sql.Literal(src_epsg),
            dst_epsg=sql.Literal(dst_epsg),
        )
    )


def _ensure_gist_index(conn: psycopg.Connection, table: Table, col: Column):
    if _column_has_index(conn, table, col, "gist"):
        return

    log.debug(f"creating index on {col} in {table}")
    conn.execute(sql.SQL("""
            create index on {table} using GIST({col});
            """).format(table=table.ident, col=col.ident))


def _column_has_index(
    conn: psycopg.Connection,
    table: Table,
    col: Column,
    index_type: str,
) -> bool:
    qry = sql.SQL("""
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
    """).format(
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
    db.sql(f"""
        create or replace table pg.{dst.schema}.{dst.table} as
            select * from '{src.path}';
        """)
    db.close()

    with psycopg.connect(conn_str) as conn:
        if src.pk is not None:
            conn.execute(
                f"alter table {dst.schema}.{dst.table} add primary key ({src.pk})"
            )


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
    log.debug(f"return code: {ret}")
    if ret != 0:
        raise FailedNodeRun("loading wfs source failed with code {ret}")


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
    log.debug(f"return code: {ret}")
    if ret != 0:
        raise FailedNodeRun("loading gdb source failed with code {ret}")


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
    log.debug(f"return code: {ret}")
    if ret != 0:
        raise FailedNodeRun("loading shapefile source failed with code {ret}")


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
    log.debug(f"return code: {ret}")
    if ret != 0:
        raise FailedNodeRun(f"loading esri source failed with code ({ret})")


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

    capture_logs(process.stdout, f"load ({label})")

    return process.wait()


def raster2pgsql(
    target: PostgisTarget,
    src: RasterSource,
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

    psql_cmd = [
        psql,
        "-h",
        target.host,
        "-p",
        str(target.port),
        "-d",
        target.db,
        "-U",
        target.user,
    ]

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
    capture_logs(psql_process.stdout, f"load {label}")

    ret = r2p_process.wait()
    log.debug(f"raster2pgsql return code: {ret}")
    if ret != 0:
        raise FailedNodeRun(
            "loading raster source (raster2pgsql) failed with code {ret}"
        )

    ret = psql_process.wait()
    log.debug(f"psql return code: {ret}")
    if ret != 0:
        raise RuntimeError(f"loading raster source (psql) failed with code ({ret})")

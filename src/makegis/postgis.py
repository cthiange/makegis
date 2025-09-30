from pathlib import Path
from typing import Tuple

import duckdb
import psycopg
from psycopg import sql


class Table:

    def __init__(self, schema: str, name: str):
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
    ddb_path: Path,
    conn_str: str,
    src_table,
    dst_table,
    src_epsg: int | None = None,
    dst_epsg: int | None = None,
    geom_index: bool = False,
):
    print(f"postgis - loading duckdb table from {ddb_path}:{src_table} to {dst_table}")
    db = duckdb.connect()
    db.sql("install spatial;")
    db.sql("load spatial;")
    db.sql(f"attach '{ddb_path}' as src (READ_ONLY);")
    db.sql(f"attach '{conn_str}' as pg (TYPE postgres);")

    # https://duckdb.org/docs/configuration/pragmas.html#table-information
    columns = db.sql(f"pragma table_info('src.{src_table}');").fetchall()

    statement = f"create or replace table pg.{dst_table} as select"
    for i, col, dtype, not_null, default, pk in columns:
        if dtype == "GEOMETRY":
            # Convert geometry to hexwkb for PostGIS
            statement += f"\n    st_ashexwkb({col}) as {col},"
        else:
            # Strings need to be quoted
            statement += f'\n    "{col}",'

    # Remove last trailing comma
    statement = statement[:-1]

    statement += f"\nfrom src.{src_table};"

    # print(statement)
    db.sql(statement)
    db.close()

    # Add constraints to new postgres table
    with psycopg.connect(conn_str) as conn:
        table = Table(*split_schema_table_name(dst_table))

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
                    src_epsg=src_epsg,
                    dst_epsg=dst_epsg,
                    geom_index=geom_index,
                )

        # Add primary key if any
        pks = [col for i, col, dtype, not_null, default, pk in columns if pk]
        if pks:
            conn.execute(
                sql.SQL(
                    "alter table {table} add primary key ({', '.join(pks)})"
                ).format(table=table)
            )

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


# TODO: refactor to not need this anymore (i.e. use distinct schema/table args)
def schema_name_identifier(sn: str) -> sql.Identifier:
    schema, table = split_schema_table_name(sn)
    return sql.Identifier(schema, table)


def split_schema_table_name(sn: str) -> Tuple[str, str]:
    """
    Split a 'schema.table_name type string into (schema, table_name)

    Assumes schema to always be present.
    """
    assert "." in sn
    schema = sn.split(".")[0]
    table = sn[len(schema) + 1 :]
    return (schema, table)


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

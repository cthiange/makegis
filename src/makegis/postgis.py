from pathlib import Path

import duckdb
import psycopg
from psycopg import sql


# Helper function to load a single table from DuckDB to Postgres
def ddb2pg(ddb_path: Path, conn_str: str, src_table, dst_table):
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
        table = schema_name_identifier(dst_table)
        for i, col_str, dtype, not_null, default, pk in columns:
            col = sql.Identifier(col_str)
            if not_null:
                conn.execute(
                    sql.SQL(
                        "alter table {table} alter column {col} set not null"
                    ).format(table=table, col=col)
                )
            if default:
                conn.execute(
                    sql.SQL(
                        "alter table {table} alter column {col} set default {default}"
                    ).format(table=table, col=col, default=sql.Literal(default))
                )
            # Process geo columns
            if dtype == "GEOMETRY":
                # logger.info(f"processing geometry column '{col}'")
                conn.execute(
                    sql.SQL(
                        "alter table {table} alter column {col} type geometry using {col}::geometry;"
                    ).format(table=table, col=col)
                )
                conn.execute(
                    sql.SQL(
                        "update {table} set {col} = st_setsrid({col}, 4326);"
                    ).format(table=table, col=col)
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


# TODO: refactor to not need this anymore (i.e. use distinct schema/table args)
def schema_name_identifier(sn: str) -> sql.Identifier:
    if "." in sn:
        schema = sn.split(".")[0]
    table = sn[len(schema) + 1 :]
    return sql.Identifier(schema, table)

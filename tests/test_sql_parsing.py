from makegis.dag.sql import analyze_sql_content
from makegis.dag.sql import DBO


def test_simple():
    sql = """
    create table new.table as
    select * from raw.dataset;
    """
    r = analyze_sql_content(sql)
    assert r.created == {DBO("new", "table", "relation")}
    assert r.dependencies == {DBO("raw", "dataset", "relation")}


def test_trickier():
    sql = """
    --create table not.new_table as select * from raw.fake;
    create table new.table as
        select * from raw.dataset;

    create temp table tmp_tbl
        as select * from raw.thing;

    create temporary table tmp_tbl_orary
        as select * from raw.thing;

    begin;

    create view view_commit as select * from new.table;
    create view view_commit_but_dropped_later as select * from new.table;

    commit;

    begin;

    create view view_rollback as select * from still_a_dep;

    rollback;

    drop view view_commit_but_dropped_later;
    """
    r = analyze_sql_content(sql)
    assert r.created == {
        DBO("new", "table", "relation"),
        DBO("", "view_commit", "relation"),
    }
    assert r.dependencies == {
        DBO("raw", "dataset", "relation"),
        DBO("raw", "thing", "relation"),
        DBO("", "still_a_dep", "relation"),
    }


def test_with_cte():
    sql = """
    create table new.table as
        with some_dep as (
            select * from raw.dep
        )
        select * from some_dep;
    """
    r = analyze_sql_content(sql)
    assert r.created == {DBO("new", "table", "relation")}
    assert r.dependencies == {DBO("raw", "dep", "relation")}


def test_create_function():
    sql = """
    create or replace function sch.foo(_id integer)
    returns table(
        id integer
    ) as
    $$
        select _id + 1
    $$
    language sql
    immutable;
    """
    r = analyze_sql_content(sql)
    assert r.created == {DBO("sch", "foo", "function")}
    assert r.dependencies == set()


def test_drop_then_create():
    sql = """
    drop table foo;
    create table foo as
        select *
        from dep;
    """
    r = analyze_sql_content(sql)
    assert r.created == {DBO("", "foo", "relation")}
    assert r.dependencies == {DBO("", "dep", "relation")}


def test_drop_then_create_commit():
    sql = """
    begin;
    drop table foo;
    create table foo as
        select *
        from dep;
    commit;
    """
    r = analyze_sql_content(sql)
    assert r.created == {DBO("", "foo", "relation")}
    assert r.dependencies == {DBO("", "dep", "relation")}


def test_drop_then_create_rollback():
    sql = """
    begin;
    drop table foo;
    create table foo as
        select *
        from dep;
    rollback;
    """
    r = analyze_sql_content(sql)
    assert r.created == set()
    assert r.dependencies == {DBO("", "dep", "relation")}

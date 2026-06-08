import psycopg as pg
import pytest

from .commands import mkgs_run

pytestmark = [pytest.mark.bench, pytest.mark.mkgs_run]


def test_run_single_load(initialized_db):
    p = mkgs_run("raw.csv_table")
    assert p.returncode == 0
    assert "ERROR" not in p.stdout.decode("utf8")


def test_run_is_logged_in_journal(initialized_db):
    p = mkgs_run("raw.csv_table")
    assert p.returncode == 0
    assert "ERROR" not in p.stdout.decode("utf8")

    db = initialized_db
    r = db.query_one("""
        select node_id
            , started
            , completed
            , db_user
            , hostname
            , mkgs_version
            , repo_revision
            , target_version
        from _makegis_runs
        order by started desc
        limit 1;
    """)
    print(r)
    assert r.node_id == "raw.csv_table"
    assert "postgresql" in r.target_version.lower()
    assert "postgis" in r.target_version.lower()

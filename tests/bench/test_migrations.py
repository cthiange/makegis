import psycopg as pg
import pytest

from .commands import mkgs_migrate
from .commands import mkgs_run
from makegis.journal import JOURNAL_SCHEMA_REVISION

pytestmark = [pytest.mark.bench, pytest.mark.mkgs_migrate]

LATEST_REV = JOURNAL_SCHEMA_REVISION


def test_migrate_from_rev_zero_empty(rev_zero_db):
    db = rev_zero_db
    # Confirm this is a rev 0 db
    r = db.query_one("""
        select to_regclass('_makegis_runs') is not null
            and to_regclass('_makegis_revisions') is null
    """)
    assert r is not None and r[0] is True

    # Apply migrations
    mkgs_migrate()

    # Check revision
    r = db.query_one("""
        select max(revision) as max_rev from _makegis_revisions;
    """)
    assert r is not None and r.max_rev == 1

    # And confirm runs can log just fine
    # Creating target schema first because db was not initialized
    db.execute("create schema raw;")
    mkgs_run("raw.csv_table")


def test_migrate_from_rev_zero_not_empty(rev_zero_db):
    db = rev_zero_db
    # Confirm this is a rev 0 db
    r = db.query_one("""
        select to_regclass('_makegis_runs') is not null
            and to_regclass('_makegis_revisions') is null
    """)
    assert r is not None and r[0] is True

    # Add existing run record
    db.execute("""
        insert into _makegis_runs(
            node_id,
            started,
            completed,
            db_user,
            hostname,
            mkgs_version,
            repo_revision
        ) values (
            'raw.dummy_table',
            now(),
            now(),
            'dummy-user',
            'dummy-host',
            'dummy-version',
            'dummy-revision'
        );
    """)

    # Apply migrations
    mkgs_migrate()

    # Check revision
    r = db.query_one("""
        select max(revision) as max_rev from _makegis_revisions;
    """)
    assert r is not None and r.max_rev == 1

    # And confirm runs can log just fine
    # Creating target schema first because db was not initialized
    db.execute("create schema raw;")
    mkgs_run("raw.csv_table")


def test_migrate_on_latest_rev_does_nothing(initialized_db):
    """calling `mkgs migrate` when there is nothing to migrate should be safe"""
    p = mkgs_migrate()
    msg = "target journal is on latest revsion - no migrations to apply"
    assert msg in p.stdout

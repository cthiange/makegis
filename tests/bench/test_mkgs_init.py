import psycopg as pg
import pytest

from makegis.journal import JOURNAL_SCHEMA_REVISION

from .commands import mkgs_init

pytestmark = [pytest.mark.bench, pytest.mark.mkgs_init]


def test_init(blank_db):
    """Test `mkgs init` creates the journal tables and project schemas"""
    db = blank_db
    # Check journal tables and project schemas do not exists yet
    r = db.query_one("""
        select to_regclass('_makegis_runs') is null
            and to_regclass('_makegis_revisions') is null
            and to_regnamespace('raw') is null
        """)
    assert r is not None and r[0] is True

    mkgs_init()

    # Check journal tables and project schemas have been created
    r = db.query_one("""
        select to_regclass('_makegis_runs') is not null
            and to_regclass('_makegis_revisions') is not null
            and to_regnamespace('raw') is not null
        """)
    assert r is not None and r[0] is True

    # Check journal revision is set
    # Should contain only one and be the latest
    r = db.query_one("select revision from _makegis_revisions order by 1 limit 1")
    assert r is not None
    assert r.revision == JOURNAL_SCHEMA_REVISION

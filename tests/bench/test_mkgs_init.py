import psycopg as pg
import pytest

from .commands import mkgs_init

pytestmark = [pytest.mark.bench, pytest.mark.mkgs_init]


def test_init(blank_db):
    """Test `mkgs init` creates the journal table and project schemas"""
    with pytest.raises(pg.errors.UndefinedTable):
        blank_db.execute("select count(*) from _makegis_runs")

    p = mkgs_init()
    assert p.returncode == 0

    r = blank_db.query_one("select count(*) from _makegis_runs")
    assert r == (0,)

    with pytest.raises(pg.errors.DuplicateSchema):
        blank_db.execute("create schema raw")

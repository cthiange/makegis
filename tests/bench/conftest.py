import os
import logging
from pathlib import Path
import subprocess

import dotenv
import psycopg as pg
from psycopg.rows import namedtuple_row
from psycopg.sql import SQL
import pytest
from rich.console import Console
from rich.logging import RichHandler

from .commands import mkgs_init

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="[%X]",
)


@pytest.fixture(scope="module")
def load_env():
    env_path = Path(__file__).absolute().parent / Path(".env")
    log.debug(f"loading local .env file: {env_path}")
    dotenv.load_dotenv(env_path)


class _TempDB:
    uri: str

    def __init__(self):
        log.debug("preparing temp test db from env")
        host = os.environ["MKGS_TEST_BENCH_PG_HOST"]
        port = os.environ["MKGS_TEST_BENCH_PG_PORT"]
        user = os.environ["MKGS_TEST_BENCH_PG_USER"]
        pw = os.environ.get("MKGS_TEST_BENCH_PG_PASS")
        db = os.environ["MKGS_TEST_BENCH_PG_DEFAULT_DB"]
        admin_uri = f"postgresql://{user}{':' + pw if pw else ''}@{host}:{port}/{db}"

        with pg.connect(admin_uri, autocommit=True) as conn:
            conn.execute(f"drop database if exists tmp_test_will_be_deleted;")
            conn.execute(f"create database tmp_test_will_be_deleted;")

        test_uri = f"postgresql://{user}{':' + pw if pw else ''}@{host}:{port}/tmp_test_will_be_deleted"

        with pg.connect(test_uri, autocommit=True) as conn:
            conn.execute("create extension postgis;")

        self.uri = test_uri

    def execute(self, sql: str):
        with pg.connect(self.uri) as conn:
            conn.execute(sql)  # type: ignore

    def query_one(self, sql: str) -> tuple | None:
        with pg.connect(self.uri, row_factory=namedtuple_row) as conn:
            return conn.execute(SQL(sql)).fetchone()  # type: ignore


class BlankDB(_TempDB):
    """Fresh completely empty test postgis db"""

    pass


class InitializedDB(_TempDB):
    """Test db that has already been initialized by `mkgs init`"""

    def __init__(self):
        super().__init__()
        # run `mkgs init` to create journal and targeted schemas
        assert mkgs_init().returncode == 0


@pytest.fixture
def blank_db(load_env) -> BlankDB:
    """
    Create fresh new completely empty db for a test.
    """
    return BlankDB()


@pytest.fixture
def initialized_db(load_env) -> InitializedDB:
    """
    Create fresh new db for a test with journal and expected schemas.
    """
    return InitializedDB()

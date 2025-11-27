from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
import platform
import subprocess
from typing import Dict
from typing import Self

import psycopg

from .config import TargetConfig
from . import __version__


@dataclass(frozen=True)
class RunRecord:
    node_id: str
    started: datetime
    completed: datetime
    db_user: str
    hostname: str
    mkgs_version: str
    repo_hash: str


class RunEvent:

    def __init__(self, node_id: str):
        self._node_id = node_id
        self._started: datetime | None = None

    def start(self) -> Self:
        """Register run or abort if already running"""
        self._started = datetime.now(timezone.utc)
        return self

    def log(self, target: TargetConfig):
        assert self._started is not None
        record = RunRecord(
            node_id=self._node_id,
            started=self._started,
            completed=datetime.now(timezone.utc),
            db_user=target.user,
            hostname=platform.node(),
            mkgs_version=__version__,
            repo_hash=get_repo_hash()
        )
        log_run(target, record)

def get_repo_hash():
    try:
        desc = subprocess.check_output(
            ["git", "describe", "--always", "--dirty"],
            stderr=subprocess.DEVNULL
        ).decode().strip()
        return desc
    except subprocess.CalledProcessError:
        return None


def init_tables(target: TargetConfig):
    print("initializing event table")
    with psycopg.connect(target.conn_str()) as conn:
        conn.execute(
            """
            create table _makegis_runs (
                node_id text not null,
                started timestamp not null,
                completed timestamp not null,
                db_user text not null,
                hostname text not null,
                mkgs_version text not null,
                repo_revision text
            );
            """
        )
        conn.commit()


def log_run(target: TargetConfig, record: RunRecord):
    with psycopg.connect(target.conn_str()) as conn:
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
            """, (
                record.node_id,
                record.started,
                record.completed,
                record.db_user,
                record.hostname,
                record.mkgs_version,
                record.repo_hash
            )
        )


def fetch_manifest(target: TargetConfig) -> Dict[str,datetime]:
    with psycopg.connect(target.conn_str()) as conn:
        rows = conn.execute(
            """
            select node_id
                , max(completed) as last_run_utc
            from _makegis_runs
            group by 1;
            """
        ).fetchall()
        # Map node ids to timestamp coverted from utc to local.
        return {row[0]: row[1].replace(tzinfo=timezone.utc).astimezone() for row in rows}


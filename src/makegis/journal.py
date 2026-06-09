from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
import logging
import platform
import subprocess
from typing import Dict
from typing import Self

from . import __version__

log = logging.getLogger("makegis")

type Manifest = Dict[str, datetime]

# Current schema revision
JOURNAL_SCHEMA_REVISION = 1


@dataclass(frozen=True)
class RunRecord:
    node_id: str
    started: datetime
    completed: datetime
    db_user: str
    hostname: str
    mkgs_version: str
    repo_hash: str | None
    target_version: str


class RunEvent:

    def __init__(self, node_id: str, target_version: str):
        self._node_id = node_id
        self._started: datetime | None = None
        self._target_version: str = target_version

    def start(self) -> Self:
        """Record start time of event"""
        self._started = datetime.now(timezone.utc)
        return self

    def to_record(self, user: str) -> RunRecord:
        assert self._started is not None
        return RunRecord(
            node_id=self._node_id,
            started=self._started,
            completed=datetime.now(timezone.utc),
            db_user=user,
            hostname=platform.node(),
            mkgs_version=__version__,
            repo_hash=get_repo_hash(),
            target_version=self._target_version,
        )


@dataclass(frozen=True)
class MigrationRecord:
    revision: int
    since: datetime

    @classmethod
    def new(cls, revision: int):
        return cls(revision=revision, since=datetime.now(timezone.utc))


def get_repo_hash() -> str | None:
    try:
        desc = (
            subprocess.check_output(
                ["git", "describe", "--always", "--dirty"], stderr=subprocess.DEVNULL
            )
            .decode()
            .strip()
        )
        return desc
    except subprocess.CalledProcessError:
        return None

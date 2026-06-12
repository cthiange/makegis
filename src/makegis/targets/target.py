import logging
from typing import List

from ..core.load import LoadJob
from ..core.transforms import Transform
from ..config.project import TargetConfig
from ..journal import RunEvent
from ..journal import MigrationRecord
from ..journal import Manifest
from ..journal import JOURNAL_SCHEMA_REVISION
from .postgis import PostgisTarget

from .. import __version__

log = logging.getLogger("makegis")

type _Inner = PostgisTarget


class Target:

    def __init__(self, config: TargetConfig):
        self._cfg = config
        self._inner: _Inner = PostgisTarget(config)

    def load_table(self, job: LoadJob):
        self._inner.load_table(job)

    def run_transform(self, transform: Transform):
        self._inner.run_transform(transform)

    def init_journal(self):
        if self._inner.is_initialized():
            log.info("target has already been initialized")
            return
        log.debug("initializing journal tables")
        self._inner.init_journal()

    def ensure_schemas(self, schemas: List[str]):
        """Ensure given schema names exist on target"""
        for schema in schemas:
            log.debug(f"ensuring schema '{schema}' exists on target")
            self._inner.ensure_schema(schema)

    def fetch_manifest(self) -> Manifest:
        log.debug(f"fetching manifest from target")
        return self._inner.fetch_manifest()

    def migrate_journal(self):
        log.debug(f"migrating journal")
        rev = self._inner.get_journal_revision()
        log.info(f"journal is at revsion {rev}")
        if rev is None:
            log.error("target is not initialized - run `mkgs init`")
            return
        if rev == JOURNAL_SCHEMA_REVISION:
            log.info("target journal is on latest revsion - no migrations to apply")
            return

        log.info(f"migrating journal to revision {JOURNAL_SCHEMA_REVISION}")
        migrations = [
            self._inner.apply_journal_migration_1,
        ]
        for i, mig in enumerate(migrations[rev:]):
            mig_revision = rev + i + 1
            log.info(f"applying migration {mig_revision}")
            mig(MigrationRecord.new(mig_revision))

    def get_version(self) -> str:
        """
        Retrieve version of target database.
        """
        version = self._inner.get_version()
        if version is None:
            log.warning(f"could not retrieve target version")
            return "could not retrieve"
        return version

    def log_event(self, event: RunEvent):
        log.debug("logging run to journal")
        record = event.to_record(self._cfg.user)
        self._inner.log_event(record)

    def add_to_environment(self):
        """
        Sets MKGS_TARGET_* environment variables exposing target config.
        """
        log.debug("adding target to environment")
        self._inner.add_to_environment()

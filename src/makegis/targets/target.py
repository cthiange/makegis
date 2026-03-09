import logging

from ..core.load import LoadJob
from ..core.transforms import Transform
from ..config.root import TargetConfig
from ..journal import RunEvent
from ..journal import Manifest
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
        log.debug("initializing event table")
        self._inner.init_journal()

    def fetch_manifest(self) -> Manifest:
        log.debug(f"fetching manifest from target")
        return self._inner.fetch_manifest()

    def log_event(self, event: RunEvent):
        log.debug("logging run to journal")
        record = event.to_record(self._cfg.user)
        self._inner.log_event(record)

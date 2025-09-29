from typing import Dict
from typing import List
from pathlib import Path

from pydantic import BaseModel
import yaml

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


class Target(BaseModel):
    """A targetted database instance"""

    # yaml key used to id this target
    name: str
    # optional description
    description: str | None = None
    host: str = "localhost"
    port: int = 5432
    user: str = "postgres"
    # database name
    db: str

    def conn_uri(self) -> str:
        s = self
        return f"postgresql://{s.user}@{s.host}:{s.port}/{s.db}"

    def conn_str(self) -> str:
        s = self
        return f"host={s.host} port={s.port} dbname={s.db} user={s.user}"


class Config(BaseModel):
    src_dir: Path
    targets: Dict[str, Target]

    @classmethod
    def from_file(cls, path: Path):
        """Build config from yaml file."""
        print(f"loading profile from '{path}'")
        with open(path) as f:
            d = yaml.load(f, Loader)
        return Config.from_dict(d)

    @classmethod
    def from_dict(cls, d: dict):
        src_dir = d["src_dir"]
        targets = parse_targets(d)
        return cls(src_dir=src_dir, targets=targets)


def parse_targets(d: dict) -> Dict[str, Target]:
    if "targets" not in d:
        raise RuntimeError("config is missing a 'targets' key")

    targets = {}
    for tname, tdict in d["targets"].items():
        target = Target(name=tname, **tdict)
        assert tname not in targets
        targets[tname] = target

    return targets

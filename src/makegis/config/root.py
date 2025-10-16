from pathlib import Path
from typing import Dict

from pydantic import BaseModel
import yaml

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

from .makegis import LoadDefaults
from .utils import expand_dict_strings


class RootDefaults(BaseModel):
    load: LoadDefaults = LoadDefaults()
    target: str | None = None


class TargetConfig(BaseModel):
    """Describes a target database"""

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


class RootConfig(BaseModel):
    src_dir: Path
    defaults: RootDefaults
    targets: Dict[str, TargetConfig]

    @classmethod
    def from_file(cls, path: Path):
        print(f"debug - reading {path}")
        with open(path) as f:
            d = yaml.load(f, Loader)
        rc = cls.from_dict(d)
        # Resolve path of src dir
        if not rc.src_dir.is_absolute():
            rc.src_dir = (path.parent / rc.src_dir).resolve()
        return rc

    @classmethod
    def from_yaml(cls, s: str):
        d = yaml.load(s, Loader)
        return cls.from_dict(d)

    @classmethod
    def from_dict(cls, d: Dict):
        expand_dict_strings(d)
        defaults = RootDefaults(**d.pop("defaults", {}))
        return RootConfig(defaults=defaults, **d)

from pathlib import Path

from pydantic import BaseModel


class Transform(BaseModel):
    sql: Path

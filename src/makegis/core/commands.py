from pathlib import Path

from pydantic import BaseModel


class Command(BaseModel):
    path: Path

from pathlib import Path

from makegis.config import ProjectFile
from makegis.config.makegis import ConfigFile


def test_project_1():
    path = Path(__file__).parent.parent / Path("examples/makegis.project1.yml")
    pf = ProjectFile.from_path(path)
    assert isinstance(pf, ProjectFile)
    assert pf.defaults.target == "pg_dev"


def test_project_2():
    path = Path(__file__).parent.parent / Path("examples/makegis.project2.yml")
    pf = ProjectFile.from_path(path)
    assert isinstance(pf, ProjectFile)


def test_makegis():
    path = Path(__file__).parent.parent / Path("examples/makegis.yml")
    pf = ConfigFile.from_path(path)
    assert isinstance(pf, ConfigFile)

from pathlib import Path

from makegis.config.root import RootConfig


def test_root1():
    path = Path(__file__).parent.parent / Path("examples/makegis.root1.yml")
    c = RootConfig.from_file(path)
    assert isinstance(c, RootConfig)


def test_root2():
    path = Path(__file__).parent.parent / Path("examples/makegis.root2.yml")
    c = RootConfig.from_file(path)
    assert isinstance(c, RootConfig)

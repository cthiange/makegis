from pathlib import Path

from makegis.config.makegis import MakeGISConfig
from makegis.config.makegis import LoadBlock
from makegis.config.makegis import DuckDBSourceBlock
from makegis.config.makegis import FileSourceBlock
from makegis.config.makegis import EsriSourceBlock
from makegis.config.makegis import WFSSourceBlock
from makegis.config.makegis import RasterSourceBlock
from makegis.config.makegis import TransformBlock
from makegis.config.makegis import SQLTransform
from makegis.config.makegis import NodeBlock


def test_load():
    path = Path(__file__).parent.parent / Path("examples/makegis.load.yml")
    m = MakeGISConfig.from_file(path)
    assert m.type == "load"
    assert isinstance(m.block, LoadBlock)
    assert m.block.defaults.epsg == 4326
    assert isinstance(m.block.items[0].src, DuckDBSourceBlock)
    assert isinstance(m.block.items[1].src, DuckDBSourceBlock)
    assert isinstance(m.block.items[2].src, FileSourceBlock)
    assert isinstance(m.block.items[3].src, WFSSourceBlock)
    assert isinstance(m.block.items[4].src, EsriSourceBlock)
    assert isinstance(m.block.items[5].src, RasterSourceBlock)
    assert m.block.items[1].src.table == "not_table_2"
    assert m.block.items[5].src.tile_size == 512


def test_transform():
    path = Path(__file__).parent.parent / Path("examples/makegis.transform.yml")
    m = MakeGISConfig.from_file(path)
    assert m.type == "transform"
    assert isinstance(m.block, TransformBlock)
    assert isinstance(m.block.transforms[0], SQLTransform)
    assert m.block.transforms[0].path == Path("table_a.sql")


def test_node():
    path = Path(__file__).parent.parent / Path("examples/makegis.node.yml")
    m = MakeGISConfig.from_file(path)
    assert isinstance(m.block, NodeBlock)
    assert m.type == "node"
    b = m.block
    assert b.prep is not None
    assert b.prep[1] == "then_this.py"
    assert isinstance(b.do.load, LoadBlock)
    assert b.do.run is not None
    assert b.do.run[0].cmd == "script_1.py"
    assert b.do.run[0].creates[1].type == "table"
    assert b.do.run[0].creates[1].name == "other_table_created_by_script_1"

from pathlib import Path
from typing import Final

import pytest

from makegis.config.project import Context
from makegis.config.project import ProjectError

# A dummy project root (this directory)
ROOT: Final = Path(__file__).parent


def make_context(
    cf_path: Path = Path("schema/makegis.yml"), group: str | None = None
) -> Context:
    project_root = ROOT
    config_path = project_root / cf_path
    return Context(project_root, config_path, group)


def test_derived_attributes():
    ctx = make_context()
    assert ctx.conf_dir == ROOT / Path("schema")
    assert ctx.schema == "schema"
    assert ctx.prefix == ""

    ctx = make_context(cf_path=Path("sch/subdir/makegis.yml"), group="group_b")
    assert ctx.conf_dir == ROOT / Path("sch/subdir")
    assert ctx.schema == "sch"
    assert ctx.prefix == "subdir_group_b"


def test_expand_path_relative():
    """Relative paths should be interpeted relative to their config dir and made absolute"""
    ctx = make_context()
    local = Path("script.sql")
    expected = (ROOT / Path("schema/script.sql")).absolute()
    assert ctx.expand_path(local) == expected


def test_expand_path_absolute():
    """Absolute paths should be unchanged"""
    ctx = make_context()
    abs_path_to_some_script = (ROOT / Path("script.sql")).absolute()
    assert ctx.expand_path(abs_path_to_some_script) == abs_path_to_some_script


def test_expand_name_for_named_group():
    ctx = make_context(group="grp")
    assert ctx.expand_name("table1") == "schema.grp_table1"


def test_expand_name_for_unnamed_group():
    ctx = make_context()
    assert ctx.expand_name("table1") == "schema.table1"


def test_node_name_cannot_be_just_schema_name():
    with pytest.raises(ProjectError, match=r"Unnamed custom node in unnamed group.*"):
        ctx = make_context().for_node(None)


def test_expand_name_for_named_node_context():
    ctx = make_context().for_node("testnode")
    assert ctx.expand_name("table1") == "schema.testnode_table1"

    ctx = make_context(group="grp").for_node("testnode")
    assert ctx.expand_name("table1") == "schema.grp_testnode_table1"


def test_expand_name_for_unnamed_node_context():
    ctx = make_context(group="grp").for_node(None)
    assert ctx.expand_name("table1") == "schema.grp_table1"


def test_node_name_for_named_node_context():
    ctx = make_context().for_node("testnode")
    assert ctx.node_name == "schema.testnode"

    ctx = make_context(group="grp").for_node("testnode")
    assert ctx.node_name == "schema.grp_testnode"


def test_node_name_for_unnamed_node_context():
    ctx = make_context(group="grp").for_node(None)
    assert ctx.node_name == "schema.grp"

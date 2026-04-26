from makegis.config.makegis import DuckDBSource
from makegis.config.makegis import FileSource
from makegis.config.makegis import WFSSource
from pathlib import Path

import pydantic
import pytest

from makegis.config.project import Project
from makegis.config.project import ProjectError
from .project_prepper import ProjectPrepper


def test_load_without_config_files(tmp_path):
    """
    Loading a project not containing any config files.

    Allowed for now. Might raise an exception in the future.
    Should at least log a warning.
    TODO: assert a warning was logged.
    """
    pp = ProjectPrepper(tmp_path)
    project = Project(pp.path)
    project.load()


def test_sources(tmp_path):
    pp = ProjectPrepper(tmp_path)

    pp.add_config(
        Path("./schema1"),
        """
        - name: group_one
          load:
            tbl_a:
              wfs: https://dummy_wfs_url
            tbl_b:
              file: layer.shp
              geom_index: true

        # Unnamed group with group-wide defaults
        - defaults:
            load:
              geom_column: geom
          load:
            tbl_x:
              duckdb: path/to.db
        """,
    )

    project = Project(pp.path)
    project.load()

    src = project.sources[0]
    assert src.name == "schema1.group_one_tbl_a"
    assert isinstance(src.source, WFSSource)
    assert src.source.geom_index is False
    assert src.source.geom_column is None

    src = project.sources[1]
    assert src.name == "schema1.group_one_tbl_b"
    assert isinstance(src.source, FileSource)
    assert src.source.geom_index is True
    assert src.source.geom_column is None

    src = project.sources[2]
    assert src.name == "schema1.tbl_x"
    assert isinstance(src.source, DuckDBSource)
    assert src.source.geom_index is False
    assert src.source.geom_column == "geom"


def test_source_with_conflicting_keys(tmp_path):
    pp = ProjectPrepper(tmp_path)

    pp.add_config(
        Path("./schema1"),
        """
        - name: group_one
          load:
            oops_table:
              wfs: url1
              esri: url2
        """,
    )

    project = Project(pp.path)
    with pytest.raises(pydantic.ValidationError):
        project.load()


def test_transforms(tmp_path):
    pp = ProjectPrepper(tmp_path)

    pp.add_config(
        Path("./schema1"),
        """
        - name: namedgroup
          transform:
            - one.sql
            - renamed_two: two.sql

        - transform:
            - three.sql
        """,
    )

    project = Project(pp.path)
    project.load()

    assert len(project.transforms) == 3

    pt = project.transforms[0]
    assert pt.name == "schema1.namedgroup_one"
    assert pt.script == project.root / Path("schema1/one.sql")

    pt = project.transforms[1]
    assert pt.name == "schema1.namedgroup_renamed_two"
    assert pt.script == project.root / Path("schema1/two.sql")

    pt = project.transforms[2]
    assert pt.name == "schema1.three"
    assert pt.script == project.root / Path("schema1/three.sql")


def test_custom_nodes(tmp_path):
    pp = ProjectPrepper(tmp_path)

    pp.add_config(
        Path("./schema1"),
        """
        - name: testgroup
          custom:
            # Can have 1 nameless node, provided its fully qualified name
            # doesn't conflict with any other node in the project.
            - prep:
                - work.py
              load:
                table_1:
                  file: output.shp
            - name: other_node
              deps:
                - table: upstream.table_x
                - table: upstream.table_y
              prep:
                - do_this_first.sh
                - then_prepare_duckdb.py
              load:
                table_2:
                  duckdb: ~/path/to/prepared.db
              cleanup:
                - remove_temp_files.sh
        """,
    )

    project = Project(pp.path)
    project.load()

    assert len(project.custom) == 2

    # Node 1
    node = project.custom[0]
    assert node.name == "schema1.testgroup"

    # Node 1 - Deps
    assert node.deps is None

    # Node 1 - Prep
    assert node.prep is not None
    assert len(node.prep) == 1
    assert node.prep[0] == project.root / Path("schema1/work.py")

    # Node 1 - Load
    assert node.load is not None
    assert len(node.load) == 1
    src_name = "schema1.testgroup_table_1"
    src = node.load[src_name]

    # Node 2
    node = project.custom[1]
    assert node.name == "schema1.testgroup_other_node"

    # Node 2 - Deps
    assert node.deps is not None
    assert len(node.deps) == 2
    assert node.deps[0].type == "table"
    assert node.deps[0].name == "upstream.table_x"

    # Node 2 - Prep
    assert node.prep is not None
    assert len(node.prep) == 2
    assert node.prep[0] == project.root / Path("schema1/do_this_first.sh")
    assert node.prep[1] == project.root / Path("schema1/then_prepare_duckdb.py")

    # Node 2 - Load
    assert node.load is not None
    assert len(node.load) == 1
    src_name = "schema1.testgroup_other_node_table_2"
    src = node.load[src_name]


def test_context_less_node_is_not_allowed(tmp_path):
    pp = ProjectPrepper(tmp_path)

    pp.add_config(
        Path("./schema1"),
        """
        - custom:
            # Nameless node in a nameless group in top-level dir
            - prep:
                - work.py
              load:
                table_1:
                  file: output.shp
        """,
    )

    project = Project(pp.path)
    with pytest.raises(ProjectError):
        project.load()


def test_config_file_in_project_root_is_not_allowed(tmp_path):
    pp = ProjectPrepper(tmp_path)

    pp.add_config(
        Path(""),
        """
        - name: testgroup
          transform:
            - one.sql
        """,
    )

    project = Project(pp.path)
    with pytest.raises(
        ProjectError, match=r"Found config file in root project directory.+"
    ):
        project.load()


def test_project_with_src_dir(tmp_path):
    pp = ProjectPrepper(
        tmp_path,
        yaml="""
        src_dir: src
        defaults:
          load:
        targets:
          dummy:
            host: dummy
            user: dummy
            db: dummy

    """,
    )

    pp.add_config(
        Path("src/schema1"),
        """
        - load:
            tbl_a:
              wfs: url
        """,
    )

    project = Project(pp.path)
    project.load()

    src = project.sources[0]
    assert src.name == "schema1.tbl_a"
    assert isinstance(src.source, WFSSource)
    assert src.source.geom_index is None
    assert src.source.geom_column is None

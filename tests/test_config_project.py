import os

from makegis.config.makegis import DuckDBSource
from makegis.config.makegis import FileSource
from makegis.config.makegis import WFSSource
from makegis.config.makegis import Command
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
          nodes:
            - load: tbl_a
              wfs: https://dummy_wfs_url
            - load: tbl_b
              file: layer.shp
              geom_index: true

        # Unnamed group with group-wide defaults
        - defaults:
            load:
              geom_column: geom
          nodes:
            - load: tbl_x
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
        nodes:
          - load: oops_table
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
          nodes:
            - transform: one.sql
            - transform: two.sql
              name: renamed_two

        - nodes:
            - transform: three.sql
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


def test_runs(tmp_path):
    pp = ProjectPrepper(tmp_path)

    pp.add_config(
        Path("./schema1"),
        """
        - name: testgroup
          nodes:
            # Can have 1 nameless run node, provided its fully qualified name
            # doesn't conflict with any other node in the project.
            - run:
              steps:
                - cmd: prep.py
                - load: table_1
                  file: output.shp
            - run: other_node
              deps:
                - table: upstream.table_x
                - table: upstream.table_y
              steps:
                - cmd: do_this_first.sh
                - cmd: then_prepare_duckdb.py
                - load: table_2
                  duckdb: ~/path/to/prepared.db
                - cmd: cleanup.sh
        """,
    )

    project = Project(pp.path)
    project.load()

    assert len(project.runs) == 2

    # Node 1
    pr = project.runs[0]
    assert pr.name == "schema1.testgroup"

    # Node 1 - Deps
    assert pr.deps is None

    # Node 1 - Steps
    assert len(pr.steps) == 2
    assert pr.steps[0] is not None
    assert isinstance(pr.steps[0], Command)
    assert pr.steps[0].cmd == project.root / Path("schema1/prep.py")

    assert pr.steps[1].name == "schema1.testgroup_table_1"
    # Relative path is now relative to project root
    assert pr.steps[1].source.file == project.root / Path("schema1/output.shp")

    # Node 2
    pr = project.runs[1]
    assert pr.name == "schema1.testgroup_other_node"

    # Node 2 - Deps
    assert pr.deps is not None
    assert len(pr.deps) == 2
    assert pr.deps[0].type == "table"
    assert pr.deps[0].name == "upstream.table_x"

    # Node 2 - Steps
    assert len(pr.steps) == 4
    assert pr.steps[0].cmd == project.root / Path("schema1/do_this_first.sh")
    assert pr.steps[1].cmd == project.root / Path("schema1/then_prepare_duckdb.py")
    assert pr.steps[2].name == "schema1.testgroup_other_node_table_2"
    assert pr.steps[2].source.duckdb == Path("~/path/to/prepared.db").expanduser()
    assert pr.steps[3].cmd == project.root / Path("schema1/cleanup.sh")


def test_context_less_node_is_not_allowed(tmp_path):
    pp = ProjectPrepper(tmp_path)

    pp.add_config(
        Path("./schema1"),
        """
        - nodes:
            # Nameless node in a nameless group in top-level dir
            - run:
              steps:
                - cmd: prep.py
                - load: table_1
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
        nodes:
          - transform: one.sql
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
        nodes:
          - load: tbl_a
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


def test_env_vars_get_expanded(tmp_path):
    os.environ["DB_USER"] = "test_user"
    os.environ["API_KEY"] = "test_key"

    pp = ProjectPrepper(
        tmp_path,
        yaml="""
        defaults:
          load:
        targets:
          dummy:
            host: dummy
            user: "{{ DB_USER }}"
            db: dummy
    """,
    )

    pp.add_config(
        Path("src/schema1"),
        """
        nodes:
          - load: tbl_a
            wfs: "url?key={{API_KEY}}"
        """,
    )

    project = Project(pp.path)
    project.load()

    assert project.targets["dummy"].user == "test_user"
    assert isinstance(project.sources[0].source, WFSSource)
    assert project.sources[0].source.wfs == "url?key=test_key"

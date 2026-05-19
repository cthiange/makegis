from pathlib import Path

from makegis.config.project import Project
from makegis.dag.builder import Builder
from makegis.dag.dag import SourceNode
from makegis.dag.dag import CustomNode
from makegis.dag.dag import DatabaseObject
from .project_prepper import ProjectPrepper


def test_build_project(tmp_path):
    pp = ProjectPrepper(tmp_path)
    pp.add_config(
        Path("schema1"),
        """
        name: group_one
        nodes:
          - load: tbl_a
            wfs: https://dummy_wfs_url
          - load: tbl_b
            file: layer.shp
            geom_index: true
          - run:
            creates:
              - table: schema1.special_table
            steps:
              - cmd: prep.py
              - cmd: do_the_work.sh
          - run: etl
            steps:
              - cmd: process.py
              - load: tbl_x
                duckdb: path.db
        """,
    )

    project = Project(pp.path)
    project.load()

    dag = Builder.build_project(project)
    assert len(dag._nodes) == 4
    assert "schema1.group_one_tbl_a" in dag._nodes
    assert "schema1.group_one_tbl_b" in dag._nodes
    assert "schema1.group_one" in dag._nodes
    assert "schema1.group_one_etl" in dag._nodes

    node = dag._nodes["schema1.group_one"]
    assert isinstance(node, CustomNode)
    assert len(node.owns) == 1
    assert (
        DatabaseObject(type="relation", schema="schema1", name="special_table")
        in node.owns
    )


def test_defaults_cascade(tmp_path):
    pp = ProjectPrepper(
        tmp_path,
        yaml="""
        defaults:
          load:
            geom_column: geom

        targets:
          dummy:
            host: dummy
            user: dummy
            db: dummy
    """,
    )
    pp.add_config(
        Path("schema1"),
        """
        - defaults:
            load:
              # Overwrite project default for group
              geom_column: null
          nodes:
            - load: tbl_a
              wfs: https://dummy_wfs_url
            - load: tbl_b
              wfs: https://dummy_wfs_url
              # Overwrite group default
              geom_column: geomname

        - name: namedgroup
          nodes:
            - load: tbl_x
              esri: url
              # Overwrite project default with null
              geom_column:
            - load: tbl_y
              esri: url
              # Overwrite project default with different value
              geom_column: the_geom
        """,
    )

    project = Project(pp.path)
    project.load()

    dag = Builder.build_project(project)
    assert len(dag._nodes) == 4

    node = dag._nodes["schema1.tbl_a"]
    assert isinstance(node, SourceNode)
    assert node.job.dst.geom_column is None

    node = dag._nodes["schema1.tbl_b"]
    assert isinstance(node, SourceNode)
    assert node.job.dst.geom_column == "geomname"

    node = dag._nodes["schema1.namedgroup_tbl_x"]
    assert isinstance(node, SourceNode)
    assert node.job.dst.geom_column is None

    node = dag._nodes["schema1.namedgroup_tbl_y"]
    assert isinstance(node, SourceNode)
    assert node.job.dst.geom_column == "the_geom"

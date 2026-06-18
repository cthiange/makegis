from makegis.dag import DAG
from makegis.dag.dag import SourceNode
from makegis.dag.dag import TransformNode
from makegis.dag.dag import DatabaseObject


def test_select_single_node():
    nodes = [
        SourceNode(id="raw.dummy_a", owns=set(), job=None),
        SourceNode(id="raw.dummy_b", owns=set(), job=None),
        SourceNode(id="raw.test_c", owns=set(), job=None),
    ]
    dag = DAG(nodes)

    assert dag.select_nodes("raw.dummy_a") == ["raw.dummy_a"]
    assert dag.select_nodes("raw.dum*") == ["raw.dummy_a", "raw.dummy_b"]
    assert dag.select_nodes("raw.*_c") == ["raw.test_c"]
    assert dag.select_nodes("*.*my_*") == ["raw.dummy_a", "raw.dummy_b"]
    assert dag.select_nodes("*") == ["raw.dummy_a", "raw.dummy_b", "raw.test_c"]


def test_select_downstream():
    # dummy_a ------------------- grandchild
    # dummy_b                   /
    # dummy_c --- c_child -----
    nodes = [
        SourceNode(
            id="raw.dummy_a",
            owns=set([DatabaseObject(schema="raw", name="dummy_a", type="relation")]),
            job=None,
        ),
        SourceNode(
            id="raw.dummy_b",
            owns=set([DatabaseObject(schema="raw", name="dummy_b", type="relation")]),
            job=None,
        ),
        SourceNode(
            id="raw.dummy_c",
            owns=set([DatabaseObject(schema="raw", name="dummy_c", type="relation")]),
            job=None,
        ),
        TransformNode(
            id="core.c_child",
            owns=set([DatabaseObject(schema="core", name="c_child", type="relation")]),
            deps=set([DatabaseObject(schema="raw", name="dummy_c", type="relation")]),
            transform=None,
        ),
        TransformNode(
            id="core.grandchild",
            owns=set([DatabaseObject(schema="core", name="tbl_gc", type="relation")]),
            deps=set(
                [
                    DatabaseObject(schema="raw", name="dummy_a", type="relation"),
                    DatabaseObject(schema="core", name="c_child", type="relation"),
                ]
            ),
            transform=None,
        ),
    ]
    dag = DAG(nodes)

    assert dag.select_nodes("raw.dummy_a+") == ["raw.dummy_a", "core.grandchild"]
    assert dag.select_nodes("raw.dummy_b+") == ["raw.dummy_b"]
    assert dag.select_nodes("raw.dummy_c+") == [
        "raw.dummy_c",
        "core.c_child",
        "core.grandchild",
    ]
    assert dag.select_nodes("raw.dummy*+") == [
        "raw.dummy_a",
        "raw.dummy_b",
        "raw.dummy_c",
        "core.c_child",
        "core.grandchild",
    ]


def test_select_upstream():
    # dummy_a ------------------- grandchild
    # dummy_b                   /
    # dummy_c --- c_child -----
    nodes = [
        SourceNode(
            id="raw.dummy_a",
            owns=set([DatabaseObject(schema="raw", name="dummy_a", type="relation")]),
            job=None,
        ),
        SourceNode(
            id="raw.dummy_b",
            owns=set([DatabaseObject(schema="raw", name="dummy_b", type="relation")]),
            job=None,
        ),
        SourceNode(
            id="raw.dummy_c",
            owns=set([DatabaseObject(schema="raw", name="dummy_c", type="relation")]),
            job=None,
        ),
        TransformNode(
            id="core.c_child",
            owns=set([DatabaseObject(schema="core", name="c_child", type="relation")]),
            deps=set([DatabaseObject(schema="raw", name="dummy_c", type="relation")]),
            transform=None,
        ),
        TransformNode(
            id="core.grandchild",
            owns=set([DatabaseObject(schema="core", name="tbl_gc", type="relation")]),
            deps=set(
                [
                    DatabaseObject(schema="raw", name="dummy_a", type="relation"),
                    DatabaseObject(schema="core", name="c_child", type="relation"),
                ]
            ),
            transform=None,
        ),
    ]
    dag = DAG(nodes)

    assert dag.select_nodes("+raw.dummy_a") == ["raw.dummy_a"]
    assert dag.select_nodes("+core.c_child") == ["raw.dummy_c", "core.c_child"]
    assert dag.select_nodes("+core.grandchild") == [
        "raw.dummy_a",
        "raw.dummy_c",
        "core.c_child",
        "core.grandchild",
    ]
    assert dag.select_nodes("+core.*") == [
        "raw.dummy_a",
        "raw.dummy_c",
        "core.c_child",
        "core.grandchild",
    ]


def test_select_upstream_downstream():
    # dummy_a ------------------- grandchild
    # dummy_b                   /
    # dummy_c --- c_child -----
    nodes = [
        SourceNode(
            id="raw.dummy_a",
            owns=set([DatabaseObject(schema="raw", name="dummy_a", type="relation")]),
            job=None,
        ),
        SourceNode(
            id="raw.dummy_b",
            owns=set([DatabaseObject(schema="raw", name="dummy_b", type="relation")]),
            job=None,
        ),
        SourceNode(
            id="raw.dummy_c",
            owns=set([DatabaseObject(schema="raw", name="dummy_c", type="relation")]),
            job=None,
        ),
        TransformNode(
            id="core.c_child",
            owns=set([DatabaseObject(schema="core", name="c_child", type="relation")]),
            deps=set([DatabaseObject(schema="raw", name="dummy_c", type="relation")]),
            transform=None,
        ),
        TransformNode(
            id="core.grandchild",
            owns=set([DatabaseObject(schema="core", name="tbl_gc", type="relation")]),
            deps=set(
                [
                    DatabaseObject(schema="raw", name="dummy_a", type="relation"),
                    DatabaseObject(schema="core", name="c_child", type="relation"),
                ]
            ),
            transform=None,
        ),
    ]
    dag = DAG(nodes)

    assert dag.select_nodes("+raw.dummy_b+") == ["raw.dummy_b"]
    assert dag.select_nodes("+core.c_child+") == [
        "raw.dummy_c",
        "core.c_child",
        "core.grandchild",
    ]

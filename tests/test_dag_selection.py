from makegis.dag import DAG
from makegis.dag.dag import SourceNode


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

from makegis.config.makegis import LoadBlock
from makegis.config.makegis import LoadDefaults
from makegis.dag.builder import MakeGISFileContext
from makegis.dag.builder import process_load_block


def make_ctx() -> MakeGISFileContext:
    return MakeGISFileContext(
        **{
            "schema": "raw",
            "prefix": None,
            "path": "./dummy/makegis.yml",
        }
    )


def test_process_load_block():
    global_defaults = LoadDefaults(
        **{
            "epsg": 4326,
            "geom_index": False,
            "geom_column": None,
        },
    )
    ctx = make_ctx()
    block = LoadBlock(
        **{
            "defaults": {"geom_index": True},
            "items": [
                {
                    "name": "layer",
                    "src": {
                        "type": "wfs",
                        "url": "https://wfs.example.com",
                        "epsg": 3857,
                    },
                    "meta": {},
                }
            ],
        }
    )
    nodes = process_load_block(ctx, block, global_defaults)
    assert len(nodes) == 1
    job = nodes[0].job
    assert job.src.epsg is None
    assert job.dst.epsg == 3857
    assert job.dst.geom_index is True
    assert job.dst.geom_column is None
    assert job.dst.attributes_only is None


def test_process_load_block__item_with_explicit_none_column():
    """
    Check that if the geom_column is set to None explicitly,
    it doesn't fall back on a default.
    """
    global_defaults = LoadDefaults(
        **{
            "epsg": 4326,
            "geom_index": False,
            "geom_column": "geom",
        },
    )
    ctx = make_ctx()
    block = LoadBlock(
        **{
            "defaults": {},
            "items": [
                {
                    "name": "layer",
                    "src": {
                        "type": "wfs",
                        "url": "https://wfs.example.com",
                        "geom_column": None,
                    },
                    "meta": {},
                }
            ],
        }
    )
    nodes = process_load_block(ctx, block, global_defaults)
    job = nodes[0].job
    assert job.dst.epsg == global_defaults.epsg
    assert job.dst.geom_index == global_defaults.geom_index
    # Should be None as per item's config, not 'geom' from global defaults
    assert job.dst.geom_column is None


def test_process_load_block__local_default_with_explicit_none_column():
    """
    Check that if item has no geom_column set and local defaults are
    set to None explicitly, it doesn't fall back on the global default.
    """
    global_defaults = LoadDefaults(
        **{
            "epsg": 4326,
            "geom_index": False,
            "geom_column": "geom",
        },
    )
    ctx = make_ctx()
    block = LoadBlock(
        **{
            "defaults": {"geom_column": None},
            "items": [
                {
                    "name": "layer",
                    "src": {
                        "type": "wfs",
                        "url": "https://wfs.example.com",
                    },
                    "meta": {},
                }
            ],
        }
    )
    nodes = process_load_block(ctx, block, global_defaults)
    job = nodes[0].job
    assert job.dst.epsg == global_defaults.epsg
    assert job.dst.geom_index == global_defaults.geom_index
    # Should be None as per local defaults, not 'geom' from global defaults
    assert job.dst.geom_column is None

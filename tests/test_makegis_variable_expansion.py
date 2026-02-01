import os

import pytest

from makegis.config.utils import expand_dict_strings


def test_whitespace():
    # Set environment variables
    os.environ["TEST_VAR"] = "ok"

    d = dict()
    d["raw"] = "value"
    d["var1"] = "{{TEST_VAR}}"
    d["var2"] = "{{ TEST_VAR}}"
    d["var3"] = "{{ TEST_VAR }}"
    d["var4"] = "{{TEST_VAR }}"
    d["var5"] = "{{TEST_VAR    }}"
    d["var6"] = "It is {{TEST_VAR}} to have text around variables"

    expand_dict_strings(d)

    assert d["raw"] == "value"
    assert d["var1"] == "ok"
    assert d["var2"] == "ok"
    assert d["var3"] == "ok"
    assert d["var4"] == "ok"
    assert d["var5"] == "ok"
    assert d["var6"] == "It is ok to have text around variables"


def test_unknown_variable():

    d = dict()
    d["oops"] = "{{TEST_NOVAR}}"

    with pytest.raises(RuntimeError):
        expand_dict_strings(d)

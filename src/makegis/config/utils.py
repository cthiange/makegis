import ast
import os
import re


def expand_dict_strings(raw_dict: dict) -> dict:
    """
    Replaces {{variables}} found in strings in place.
    """
    pattern = re.compile(r"\{\{\s*(\w+)\s*\}\}")

    s = str(raw_dict)
    vars = re.findall(pattern, s)
    for var in vars:
        if var not in os.environ:
            raise RuntimeError(f"unmatched env var {var}")
        s = re.sub(rf"\{{\{{\s*{var}\s*\}}\}}", os.environ[var], s)
    return ast.literal_eval(s)


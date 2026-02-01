import os
from typing import Dict
import re


def expand_dict_strings(raw_dict: Dict):
    """
    Replaces {{variables}} found in strings in place.
    """
    pattern = re.compile(r"\{\{\s*(\w+)\s*\}\}")

    def expand_string_values(d: dict):
        for k, v in d.items():
            if isinstance(v, dict):
                expand_string_values(v)
            if isinstance(v, str):
                vars = re.findall(pattern, v)
                for var in vars:
                    if var not in os.environ:
                        print(f"error - unmatched environment variable: {var}")
                        raise RuntimeError(f"unmatched env var {var}")
                    v = re.sub(rf"\{{\{{\s*{var}\s*\}}\}}", os.environ[var], v)
                d[k] = v

    expand_string_values(raw_dict)

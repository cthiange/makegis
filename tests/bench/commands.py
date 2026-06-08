from pathlib import Path
import subprocess


def mkgs_init():
    cmd = ["mkgs", "init"]
    cwd = Path(__file__).absolute().parent / Path("test_project")
    return subprocess.run(cmd, capture_output=True, cwd=cwd)


def mkgs_run(pattern: str):
    cmd = ["mkgs", "run", pattern]
    cwd = Path(__file__).absolute().parent / Path("test_project")
    return subprocess.run(cmd, capture_output=True, cwd=cwd)

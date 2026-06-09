from pathlib import Path
import subprocess


def mkgs_init(assert_ok=True):
    cmd = ["mkgs", "init"]
    return run_test_project_command(cmd, assert_ok)


def mkgs_migrate(assert_ok=True):
    cmd = ["mkgs", "migrate"]
    return run_test_project_command(cmd, assert_ok)


def mkgs_run(pattern: str, assert_ok=True):
    cmd = ["mkgs", "run", pattern]
    return run_test_project_command(cmd, assert_ok)


def run_test_project_command(cmd, assert_ok=True):
    cwd = Path(__file__).absolute().parent / Path("test_project")
    p = subprocess.run(
        cmd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=cwd,
    )
    if assert_ok:
        print(p.stdout)
        assert ("error" in p.stdout.lower()) == False
        assert p.returncode == 0
    return p

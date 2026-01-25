from pathlib import Path
import argparse
import logging

import dotenv

from . import __version__
from .dag.builder import Builder
from .config import RootConfig
from . import log

def cli():
    print(f"makegis {__version__}")
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(
        prog="mkgs", description="Spatial database builder"
    )
    # parser.add_argument("action")
    parser.add_argument("-v", "--verbose", action="store_true")

    subparsers = parser.add_subparsers(help="subcommand help")

    init_parser = subparsers.add_parser("init", help="initialize log tables on target")
    init_parser.add_argument(
        "-t",
        "--target",
        action="store",
        type=str,
        default=None,
        help="db instance to target",
    )
    init_parser.set_defaults(func=init)


    outdated_parser = subparsers.add_parser("outdated", help="list outdated nodes")
    outdated_parser.add_argument(
        "-t",
        "--target",
        action="store",
        type=str,
        default=None,
        help="db instance to target",
    )
    outdated_parser.set_defaults(func=outdated)

    run_parser = subparsers.add_parser("run", help="run help")
    run_parser.add_argument("node", type=str, help="node to run")
    run_parser.add_argument(
        "-t",
        "--target",
        action="store",
        type=str,
        default=None,
        help="db instance to target",
    )
    run_parser.add_argument(
        "--force",
        action="store_true",
        help="also run fresh nodes",
    )
    run_parser.set_defaults(func=run)

    check_parser = subparsers.add_parser("check", help="check help")
    # check_parser.add_argument("node", type=str, help="node help")
    check_parser.set_defaults(func=check)

    show_parser = subparsers.add_parser("show", help="show help")
    show_parser.add_argument("pattern", type=str, help="DAG selection pattern")
    show_parser.set_defaults(func=show)


    # Load .env
    dotenv.load_dotenv(".env")

    # Parse args and call handler
    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()



def check(args):
    print("check...")
    cfg = load_root_config()
    dag = Builder(cfg).build()
    dag.print()

def init(args):
    print("init...")
    cfg = load_root_config()
    target_id = args.target or cfg.defaults.target
    assert target_id is not None
    print(f"debug - using target {target_id}")
    target = cfg.targets[target_id]

    log.init_tables(target)

def outdated(args):
    print("outdated...")
    cfg = load_root_config()
    target_id = args.target or cfg.defaults.target
    assert target_id is not None
    print(f"debug - using target {target_id}")
    target = cfg.targets[target_id]

    dag = Builder(cfg).build()
    dag.show_outdated(target)

def run(args):
    print("run...")
    cfg = load_root_config()

    target_id = args.target or cfg.defaults.target
    assert target_id is not None
    print(f"debug - using target {target_id}")
    target = cfg.targets[target_id]

    dag = Builder(cfg).build()
    dag.run(args.node, target, force=args.force)


def show(args):
    print("show...")
    cfg = load_root_config()
    dag = Builder(cfg).build()
    dag.show(args.pattern)


def load_root_config():
    cfg_path = find_root_config()

    # Load .env in same dir as makegis.root.yml
    cfg_dir = cfg_path.parent
    dotenv.load_dotenv(cfg_dir / ".env")

    return RootConfig.from_file(cfg_path)


def find_root_config(cwd: Path = Path(".").resolve()):
    """
    Returns path to first makegis.root.yml file found in current dir or parents.
    """
    path = cwd / "makegis.root.yml"
    if path.exists():
        return path
    parent = cwd.parent
    if parent == cwd:
        print("Found no makegis root file in current directory or its parents.")
        exit(1)
    return find_root_config(cwd=parent)


if __name__ == "__main__":
    cli()

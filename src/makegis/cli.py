import argparse
import logging
from pathlib import Path
import sys

import dotenv
from rich.console import Console
from rich.logging import RichHandler

from . import __version__
from .dag.builder import Builder
from .config import RootConfig
from . import journal
from . import errors

console = Console()

log = logging.getLogger("makegis")


def cli():

    # Handle general -v and -d options outside of argparse.
    args = sys.argv[1:]
    verbose_flags = ["-v", "--verbose"]
    debug_flags = ["-d", "--debug"]
    debug = any([flag in args for flag in debug_flags])
    verbose = not debug and any([flag in args for flag in verbose_flags])
    args = [a for a in args if a not in verbose_flags + debug_flags]

    # Configure logger
    level = logging.WARN
    format = "%(message)s"
    datefmt = "[%X]"
    show_time = False
    show_path = False
    if debug:
        level = logging.DEBUG
        show_path = True
    elif verbose:
        level = logging.INFO
    logging.basicConfig(
        level=level,
        format=format,
        datefmt=datefmt,
        handlers=[
            RichHandler(
                console=console,
                rich_tracebacks=True,
                show_path=show_path,
                show_time=show_time,
            )
        ],
    )
    log.info(f"makegis {__version__}")

    parser = argparse.ArgumentParser(
        prog="mkgs", description="Spatial database builder"
    )

    # The --verbose and --debug options are parsed outside of argparse but we still declare
    # them here so they show up as general options in the generated help.
    parser.add_argument("-v", "--verbose", action="store_true", help="verbose messages")
    parser.add_argument("-d", "--debug", action="store_true", help="debug messages")

    subparsers = parser.add_subparsers(dest="command", help="subcommand help")

    def add_target_argument(parser):
        parser.add_argument(
            "-t",
            "--target",
            action="store",
            type=str,
            default=None,
            help="db instance to target",
        )

    init_parser = subparsers.add_parser("init", help="initialize log tables on target")
    add_target_argument(init_parser)
    init_parser.set_defaults(func=init)

    outdated_parser = subparsers.add_parser("outdated", help="list outdated nodes")
    add_target_argument(outdated_parser)
    outdated_parser.set_defaults(func=outdated)

    run_parser = subparsers.add_parser("run", help="run nodes")
    run_parser.add_argument("pattern", type=str, help="DAG selection pattern")
    add_target_argument(run_parser)
    run_parser.add_argument(
        "--force",
        action="store_true",
        help="also run fresh nodes",
    )
    run_parser.set_defaults(func=run)

    show_parser = subparsers.add_parser("show", help="show nodes")
    show_parser.add_argument("pattern", type=str, help="DAG selection pattern")
    show_parser.set_defaults(func=show)

    # Load .env
    dotenv.load_dotenv(".env")

    # Parse preprocessed args
    args = parser.parse_args(args)

    # Inject verbose or debug option
    args.verbose = verbose
    args.debug = debug

    # Call handler
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()


def init(args):
    log.info("init...")
    cfg = load_root_config()
    target_id = args.target or cfg.defaults.target
    assert target_id is not None
    log.info(f"using target {target_id}")
    target = cfg.targets[target_id]

    journal.init_tables(target)


def outdated(args):
    cfg = load_root_config()
    target_id = args.target or cfg.defaults.target
    assert target_id is not None
    log.info(f"using target {target_id}")
    target = cfg.targets[target_id]

    dag = Builder(cfg).build()
    dag.show_outdated(target)


def run(args):
    cfg = load_root_config()

    target_id = args.target or cfg.defaults.target
    assert target_id is not None
    log.info(f"using target {target_id}")
    target = cfg.targets[target_id]

    dag = Builder(cfg).build()
    node_ids = dag.select_nodes(args.pattern)
    if not node_ids:
        print("No nodes matching selection pattern.")
        return

    if not args.force:
        outdated = dag.get_outdated(target, limit_to=node_ids)

        if not node_ids:
            print("All selected nodes are up to date. Use --force to run anyways.")
            return

    n = len(node_ids)
    with console.status("") as status:
        for node_id in node_ids:
            status.update(f"Running node 1/{n}: {node_id}")
            try:
                dag.run_node(node_id, target)
            except errors.FailedNodeRun as e:
                log.error(e.message)
                return
            except Exception:
                log.exception(f"node '{node_id}' run failed!")
                return
        print("Running {n} node(s) finished succesfully.")


def show(args):
    cfg = load_root_config()
    dag = Builder(cfg).build()
    node_ids = dag.select_nodes(args.pattern)
    if not node_ids:
        print("No matching nodes.")
        return

    for node_id in node_ids:
        print(dag.render_node(node_id))


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
        log.error("Found no makegis root file in current directory or its parents.")
        exit(1)
    return find_root_config(cwd=parent)


if __name__ == "__main__":
    cli()

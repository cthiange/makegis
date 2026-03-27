import argparse
from datetime import timedelta
import logging
from pathlib import Path
import sys
import time

import dotenv
from rich.console import Console
from rich.logging import RichHandler

from . import __version__
from .config import RootConfig
from .dag.builder import Builder
from .targets import Target
from . import errors

console = Console()

log = logging.getLogger("makegis")


def cli():

    # Handle general -v and -d options outside of argparse.
    args = sys.argv[1:]
    verbose_flags = ["-v", "--verbose"]
    debug_flags = ["--debug"]
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
                rich_tracebacks=False,
                show_path=show_path,
                show_time=show_time,
            )
        ],
    )
    log.info(f"makegis {__version__}")

    parser = argparse.ArgumentParser(prog="mkgs")

    # The --verbose and --debug options are parsed outside of argparse but we still declare
    # them here so they show up as general options in the generated help.
    parser.add_argument("-v", "--verbose", action="store_true", help="verbose messages")
    parser.add_argument("--debug", action="store_true", help="debug messages")

    subparsers = parser.add_subparsers(dest="command", help="commands")

    def add_target_argument(parser):
        parser.add_argument(
            "-t",
            "--target",
            action="store",
            type=str,
            default=None,
            help="db instance to target",
        )

    init_parser = subparsers.add_parser("init", help="create schemas and journal table")
    add_target_argument(init_parser)
    init_parser.set_defaults(func=init)

    list_parser = subparsers.add_parser("ls", help="list nodes")
    list_parser.add_argument("pattern", type=str, help="DAG selection pattern")
    list_parser.set_defaults(func=show)

    outdated_parser = subparsers.add_parser("outdated", help="report outdated nodes")
    add_target_argument(outdated_parser)
    outdated_parser.set_defaults(func=outdated)

    run_parser = subparsers.add_parser("run", help="run nodes")
    run_parser.add_argument("pattern", type=str, help="DAG selection pattern")
    add_target_argument(run_parser)
    run_parser.add_argument(
        "-d",
        "--dry-run",
        action="store_true",
        help="process nodes without actually running them",
    )
    run_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="also run fresh nodes",
    )
    run_parser.set_defaults(func=run)

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
    cfg = load_root_config()
    target_id = args.target or cfg.defaults.target
    assert target_id is not None
    log.info(f"using target {target_id}")
    target = Target(cfg.targets[target_id])

    dag = Builder(cfg).build()
    target.ensure_schemas(dag.list_schemas())
    target.init_journal()


def outdated(args):
    cfg = load_root_config()
    target_id = args.target or cfg.defaults.target
    assert target_id is not None
    log.info(f"using target {target_id}")
    target = Target(cfg.targets[target_id])

    dag = Builder(cfg).build()
    node_ids = dag.get_outdated(target)
    if not node_ids:
        print("All nodes are up to date.")

    for node_id in node_ids:
        print(dag.render_node(node_id))


def run(args):
    cfg = load_root_config()

    target_id = args.target or cfg.defaults.target
    assert target_id is not None
    log.info(f"using target {target_id}")
    target = Target(cfg.targets[target_id])

    target.add_to_environment()

    dry_run = args.dry_run == True
    if args.dry_run:
        log.info("dry run - target will not be modified")

    dag = Builder(cfg).build()
    node_ids = dag.select_nodes(args.pattern)
    if not node_ids:
        print("No nodes matching selection pattern.")
        return

    if not args.force:
        outdated = dag.get_outdated(target, limit_to=node_ids)
        node_ids = outdated

        if not node_ids:
            print("All selected nodes are up to date. Use --force to run anyways.")
            return

    n = len(node_ids)
    with console.status("") as status:
        t_start = time.time()
        for inode, node_id in enumerate(node_ids):
            status.update(f"Running node {inode + 1}/{n}: {node_id}")
            if args.dry_run:
                log.info(f"dry running node '{node_id}'")
                continue
            log.info(f"running node '{node_id}'")
            try:
                dag.run_node(node_id, target)
            except errors.FailedNodeRun as e:
                log.error(e.message)
                return
            except Exception:
                log.exception(f"node '{node_id}' run failed!")
                return
        if args.dry_run:
            print(f"Dry run done. Would've run {n} node(s).")
        else:
            duration = timedelta(seconds=time.time() - t_start)
            print(f"Done. Ran {n} node(s) in {duration}")


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

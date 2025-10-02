from pathlib import Path
import argparse
import logging

import dotenv

from .dag import DAG
from .dag.builder import Builder
from .config import RootConfig


def cli():
    print("makegis 0.1.0")
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(
        prog="mkgs", description="Spatial database builder"
    )
    # parser.add_argument("action")
    parser.add_argument("-v", "--verbose", action="store_true")

    subparsers = parser.add_subparsers(help="subcommand help")

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
    run_parser.set_defaults(func=run)

    check_parser = subparsers.add_parser("check", help="check help")
    # check_parser.add_argument("node", type=str, help="node help")
    check_parser.set_defaults(func=check)

    # Load .env
    dotenv.load_dotenv(".env")

    # Parse args and call handler
    args = parser.parse_args()
    args.func(args)


def check(args):
    print("check...")
    cfg = RootConfig.from_file(Path("./makegis.root.yml"))
    dag = Builder(cfg).build()
    dag.print()


def run(args):
    print("run...")
    cfg = RootConfig.from_file(Path("./makegis.root.yml"))
    target = cfg.targets[args.target]
    dag = Builder(cfg).build()
    dag.run(args.node, target)


if __name__ == "__main__":
    cli()

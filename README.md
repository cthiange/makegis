
# MakeGIS

A lightweight orchestrator for spatial databases.

MakeGIS organizes workflows in a DAG whose nodes can be of three types:
 - source nodes: load a dataset into a target database
 - transform nodes: perform transforms within a target database
 - custom nodes: run arbitrary commands

It comes with a command line tool, `mkgs`, that operates on the resulting DAG.

Key features/choices:

 - Local and standalone: `mkgs` runs locally, no other service involved
 - Easy data loading: describe where the data is, MakeGIS handles the rest
 - Works for both ETL and ELT workflows
 - Automatic dependency discovery for SQL transforms
 - Support arbitrary code
 - Event journal to keep track of database state
 - Build DAG through code or from YAML files.

 > [!Note]
 > MakeGIS is a young project and still exploring different approaches.
 >
 > In particular, the [Configuration](#configuration) docs in this readme reflect a somewhat opinionated way of organizing and declaring a DAG through `makegis.yaml` files.
 > Alternative DAG-building paradigms are being explored.


## Installation

`pip install makegis`

MakeGIS relies on external tools, such as `ogr2ogr`, to be available.

## Concept

A quick overview of the main components underpinning MakeGIS

### DAG

The DAG organizes tasks.
A DAG node owns one or more database objects (i.e. tables, views, functions, ...).
A database object cannot be owned by more than one node.
DAG nodes can depend on other DAG nodes.

DAG nodes come in three types.
*Source* nodes own a single database table and describe the data source of that table.
*Tranfrom* nodes represent SQL to be run against a target database. The SQL statement are parsed to detect any dependencies (database object owned by other nodes).
Finally, *custom* nodes wrap arbitrary commands.

### Targets

Targets handle all interecation with a database instance. This includes running nodes as well as writing to and reading from the journal (see below).

### Journal

MakeGIS keeps an event journal on each target database.
This journal logs which nodes have been run, when, and with what version of MakeGIS.
If the MakegGIS project is in a version control system (only git supported at this stage), then the version of the project is logged for each run too.

The role of the journal is to detect stale or modified nodes that need to be rerun.
See the [`mkgs outdated`](#mkgs-outdated) command.

## Usage

Makegis provides the `mkgs` CLI utility to operate on the DAG.

```
usage: mkgs [-h] [-v] [--debug] {init,ls,outdated,run} ...

positional arguments:
  {init,ls,outdated,run}
                        commands
    init                initialize journal on target
    ls                  list nodes
    outdated            report outdated nodes
    run                 run nodes

options:
  -h, --help            show this help message and exit
  -v, --verbose         verbose messages
  --debug               debug messages
```

### mkgs init

The `init` command prepares a target database to work with MakeGIS. It creates a `_makegis_log` journal table that is used to track which nodes have been run, when and at what version.
It will also create any missing schemas expeced by the DAG.
```
usage: mkgs init [-h] [-t TARGET]

options:
  -h, --help           show this help message and exit
  -t, --target TARGET  db instance to target
```

### mkgs ls

The `ls` command shows DAG nodes matching a selection pattern. At this stage only `*` wildcards are supported but additional operators are planned (e.g. `+<pattern>` or `<pattern>+` for upstream/downstream  propagation).

```
usage: mkgs ls [-h] pattern

positional arguments:
  pattern     DAG selection pattern

options:
  -h, --help  show this help message and exit
```

### mkgs outdated

The `outdated` command reports outdated nodes for the given target.

```
usage: mkgs outdated [-h] [-t TARGET]

options:
  -h, --help           show this help message and exit
  -t, --target TARGET  db instance to target
```

### mkgs run

The `run` command will run the nodes matching a selection pattern (same as `mkgs ls`). Nodes that are fresh (i.e. not outdated) will be skipped. This can be overridden by using the `--force` flag.

```
usage: mkgs run [-h] [-t TARGET] [-d] [-f] pattern

positional arguments:
  pattern              DAG selection pattern

options:
  -h, --help           show this help message and exit
  -t, --target TARGET  db instance to target
  -d, --dry-run        process nodes without actually running them
  -f, --force          also run fresh nodes
```

## Configuration

Makegis is configured through YAML configuration files and environment variables.

A `makegis.root.yml` file defines the root of a MakeGIS project, along with project-wide settings.
MakeGIS will traverse the directory tree and look for any `makegis.yml` files.

An example project may look like this:

```
project/
├─ src/
|  ├─ raw/
|  │  ├─ provider/
|  │  │  └─ makegis.yml
|  |  └─ makegis.yml
|  └─ core/
|     ├─ transform_1.sql
|     ├─ transform_2.sql
|     ├─ transform_3.sql
|     └─ makegis.yml
├─ .env
├─ .gitignore
└─ makegis.root.yml
```

> [!Note]  
> **Environment variables** can be used by enclosing them in double curly brackets: `{{ EXAMPLE }}`. MakeGIS will consider any `.env` files in the project tree.

### makegis.root.yml

A `makegis.root.yml` file defines the root of a MakeGIS project along with project wide settings. Here's an annotated example:

```yaml
# The project's root directory.
src_dir: ./src

# Global defaults
defaults:
  # Global defaults for `load` nodes
  load:
    epsg: 4326
    geom_index: false
  # Optional default target (to use we running mkgs without a `--target` option)
  target: pg_dev

# Databases to target
targets:
  pg_prod:
    host: prod.example.com
    port: 5432
    user: mkgs
    db: postgres
  pg_dev:
    host: 127.0.0.1
    port: 5432
    user: mkgs
    db: postgres
```

### makegis.yml

The path of a `makegis.yml` determines the database relations they manage, whith top-level directories mapping to schemas.

A `makegis.yml` contains one of the following configuration blocks:

- load: defines sources to be loaded to a target
- transform: defines transforms to be applied to a target
- node: custom node to run bespoke commands

#### Load block

Maps tables to external data sources.
Each table becomes a DAG node and can be invoked individually

```yaml
load:
  <table-name>:
    <loader>: <loader-arg>
    <loader-option>: <option-value>
    <loader-option>: <option-value>
    ...
```

```yaml
load:
  countries:
    wfs: https://wfs.example.com/countries?token={{API_KEY}}
    epsg: 4326
    geom_index: true
```

TODO: Document loaders and their options.

##### EPSG option

###### Single value

`epsg: 4326`:

Target SRID.
If source declares a different EPSG, a tranformation is applied.
If source has no SRID, no transformation is applied and srid is set to given value. 

###### Mapping

`epsg: 4326:2193`

Convert from source to dest. Warn or abort if source exposes a different SRID


#### Transform block

Declares sql scripts to be enrolled.
Each script becomes a DAG node.
Dependencies with other DAG nodes are resolved automatically.
The order in which sql scripts are listed does not matter.
There are no constraints on what is in the sql scripts, as long as MakeGIS is aware of all dependencies.

```yaml
transform:
  - create_view_of_awesome_table.sql
  - create_awesome_table.sql
```

#### Node block

A `node` block defines a custom DAG node, for when more flexibility is needed than offered by a `load` or `tranform` block.

The price to pay for more flexibility is that dependencies need to be documented manually. This goes for upstream dependecies as well as objects created on the target db.

```yaml
node:
  # List any relations needed by this node.
  deps:
    - schema.upstream_table
  # Commands that do not change the target db but need to be run before we proceed.
  # Commands are run sequentially, in listing order.
  prep:
    - before.py
  # Main section
  do:
    # List of commands along with any objects they will create on the target.
    run:
      - cmd: script1.py
        # Declare objects owned by this command
        creates:
          - table: new_table
          - function: helper
    # Can also use a load block here, but it won't spawn new DAG nodes
    <load-block>
  # Like prep, but runs after `do`, and only if `do` runs fine.
  post:
    - after.py
  # Like post but always runs, even if something failed prior.
  finally:
    - teardown.py
```

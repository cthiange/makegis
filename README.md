
# MakeGIS

A lightweight orchestrator for spatial databases.

MakeGIS organizes workflows in a DAG whose nodes can be of three types:
 - source nodes: load a dataset into a target database
 - transform nodes: perform transforms within a target database
 - run nodes: run arbitrary commands

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
 > Breaking changes are likely and documention may be sparse.


## Installation

`pip install makegis`

MakeGIS relies on external tools, such as `ogr2ogr` and `psql`, to be available.

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
Finally, *run* nodes wrap arbitrary commands.

### Targets

Targets handle all interaction with a database instance. This includes running nodes as well as writing to and reading from the journal (see below).

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

A MakeGIS project is configured through YAML configuration files and environment variables.

A `makegis.project.yml` file defines the root of a MakeGIS project, along with project-wide settings.
MakeGIS will traverse the directory tree and look for any `makegis.yml` files.

An example project may look like this:

```
project/
├─ src/
│  ├─ raw/
│  │  ├─ provider/
│  │  │  └─ makegis.yml
│  │  └─ makegis.yml
│  └─ core/
│     ├─ transform_1.sql
│     ├─ transform_2.sql
│     ├─ transform_3.sql
│     └─ makegis.yml
├─ .env
├─ .gitignore
└─ makegis.project.yml
```

> [!Note]
> **Environment variables** can be used by enclosing them in double curly brackets: `{{ EXAMPLE }}`. MakeGIS will consider any `.env` files in the project tree.

### makegis.project.yml

A `makegis.project.yml` file defines the root of a MakeGIS project along with project wide settings. Here's an annotated example:

```yaml
# Global defaults
defaults:
  # Global defaults for `load` sources
  load:
    epsg: 4326
    geom_index: false
  # Optional default target (to use when running mkgs without a `--target` option)
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

A `makegis.yml` contains one or more configuration groups.
A configuration group can have the following keys:
- name: optional group name, inherited by all nodes in the group
- defaults: optional group-level defaults, overriding project-level values if needed
- nodes: list of nodes with at least one member

Each item under a `nodes` key must be one of 3 types, distinguished by a specific key:
- load: defines sources to be loaded to a target
- transform: defines SQL transforms to be applied to a target
- run: run node to run one or more bespoke commands

#### Load node

Maps tables to external data sources.
Each table becomes a DAG node and can be invoked individually.

```yaml
nodes:
  - load: <table-name>
    <loader>: <loader-arg>
    <loader-option>: <option-value>
    <loader-option>: <option-value>
    ...
```

```yaml
nodes:
  - load: countries
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


#### Transform node

Declares a sql script to be enrolled.
Dependencies with other DAG nodes are resolved automatically.
The order in which transforms are listed does not matter.
There are no constraints on the content of the sql scripts, as long as MakeGIS can resolve all dependencies.

```yaml
nodes:
  - transform: create_view_of_awesome_table.sql
  - transform: create_awesome_table.sql
```

#### Run node

A `run` node defines one or more actions to be performed as a single node, for when more flexibility is needed than offered by a `load` or `transform` node.

The price to pay for more flexibility is that dependencies need to be documented manually. This goes for upstream dependecies as well as objects created on the target db.

```yaml
nodes:
  - run: optional_node_name
    # List any relations needed by this node.
    deps:
      - table: schema.upstream_table
    # Declare objects owned by this node
    creates:
      - table: new_table
      - function: helper
    # Steps are run sequentially, in listing order.
    steps:
      - cmd: prep.py
      - cmd: load_new_table.py
      - cmd: create_helper_function.sh
      # Can also use a load block here, but it won't spawn new DAG nodes
      - load: table_3
        file: ./output.shp
      - cleanup.py
```

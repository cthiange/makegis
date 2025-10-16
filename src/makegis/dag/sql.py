from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Set
from typing import Literal
from typing import NamedTuple

import sqlglot
from sqlglot import exp


class DBO(NamedTuple):
    schema: str
    name: str
    type: Literal["relation", "function"]


@dataclass
class SQLReport:
    dependencies: Set[DBO]
    created: Set[DBO]


@dataclass
class State:
    tx: bool = False
    tx_news: Set[DBO] = field(default_factory=set)
    tx_dels: Set[DBO] = field(default_factory=set)
    news: Set[DBO] = field(default_factory=set)
    dels: Set[DBO] = field(default_factory=set)
    deps: Set[DBO] = field(default_factory=set)
    tmps: Set[DBO] = field(default_factory=set)

    def begin(self):
        # print("debug - begin tx")
        assert self.tx is False
        self.tx = True
        assert not self.tx_news
        assert not self.tx_dels

    def commit(self):
        # print("debug - commit tx")
        assert self.tx is True
        self.tx = False
        # Relations created in tx
        self.news |= self.tx_news
        self.tx_news = set()
        # Relations dropped in tx
        self.dels |= self.tx_dels
        self.tx_dels = set()

    def rollback(self):
        # print("debug - rollback tx")
        assert self.tx is True
        self.tx = False
        self.tx_news = set()
        self.tx_dels = set()

    def create(self, created: DBO, deps: Set[DBO], temp=False):
        self.deps |= deps
        if temp:
            self.tmps.add(created)
        elif self.tx:
            self.tx_news.add(created)
        else:
            self.news.add(created)

    def insert(self, target: DBO, deps: Set[DBO]):
        # If table wasn't created by this script, add it to the deps
        if target not in self.news and target not in self.tx_news:
            self.deps.add(target)
        self.deps |= deps

    def drop(self, dbo: DBO):
        if self.tx:
            if dbo in self.tx_news:
                self.tx_dels.add(dbo)
        else:
            if dbo in self.news:
                self.dels.add(dbo)

    def summary(self) -> SQLReport:
        assert self.tx is False
        dependencies = (self.deps - self.tmps) - self.news
        created = self.news - self.dels
        return SQLReport(dependencies=dependencies, created=created)


def analyze_sql_file(path: Path) -> SQLReport:
    with open(path) as f:
        return analyze_sql_content(f.read())


def analyze_sql_content(sql: str) -> SQLReport:
    state = State()

    statements = [s for s in sqlglot.parse(sql, read="postgres") if s is not None]

    for ast in statements:
        # for node in ast.walk():
        node = ast
        match node:
            case exp.Transaction():
                state.begin()
            case exp.Commit():
                state.commit()
            case exp.Rollback():
                state.rollback()

            case exp.Create(this=exp.Table()) | exp.Create(this=exp.Schema()):
                assert node.kind != "FUNCTION"
                if isinstance(node.this, exp.Table):
                    created = DBO(
                        node.this.db,
                        node.this.name,
                        "relation",
                    )
                elif isinstance(node.this, exp.Schema):
                    created = DBO(
                        node.this.this.db,
                        node.this.this.name,
                        "relation",
                    )
                else:
                    raise NotImplementedError()
                deps = list(node.find_all(exp.Table))
                deps = [DBO(t.db, t.this.name, "relation") for t in deps]
                deps = [dbo for dbo in deps if dbo != created]
                deps = set(deps)
                deps |= extract_user_defined_functions(node)
                # Collect cte names
                ctes = [c.args["alias"].name for c in node.find_all(exp.CTE)]
                # Drop deps that are CTE's
                # Assuming CTE's have no schema, just a name
                deps = {d for d in deps if d.schema or d.name not in ctes}
                # Find out if this is temporary
                temp = False
                props = node.args.get("properties")
                if props is not None:
                    if exp.TemporaryProperty() in props.args.get("expressions", []):
                        temp = True
                state.create(created, deps, temp=temp)

            case exp.Create(this=exp.UserDefinedFunction()):
                assert node.kind == "FUNCTION"
                created = DBO(
                    node.this.this.db,
                    node.this.this.name,
                    "function",
                )
                # The function expression is stored as a heredoc string.
                # Reparse it to extract dependencies
                assert isinstance(node.expression, exp.Heredoc)
                fn_statements = sqlglot.parse(node.expression.this, read="postgres")
                assert len(fn_statements) == 1
                node = fn_statements[0]
                assert node is not None
                deps = list(node.find_all(exp.Table))
                deps = [DBO(t.db, t.this.name, "relation") for t in deps]
                deps = [dbo for dbo in deps if dbo != created]
                deps = set(deps)
                deps |= extract_user_defined_functions(node)
                # Collect cte names
                ctes = [c.args["alias"].name for c in node.find_all(exp.CTE)]
                # Drop deps that are CTE's
                # Assuming CTE's have no schema, just a name
                deps = {d for d in deps if d.schema or d.name not in ctes}
                # Functions are never temp
                state.create(created, deps, temp=False)

            case exp.Insert():
                # Get target
                assert isinstance(node.this, exp.Schema)
                assert isinstance(node.this.this, exp.Table)
                target_schema = node.this.this.db
                target_table = node.this.this.this.this
                target = DBO(target_schema, target_table, "relation")
                # Get deps
                node = node.expression
                deps = list(node.find_all(exp.Table))
                deps = [DBO(t.db, t.this.name, "relation") for t in deps]
                deps = set(deps)
                deps |= extract_user_defined_functions(node)
                # Collect cte names
                ctes = [c.args["alias"].name for c in node.find_all(exp.CTE)]
                # Drop deps that are CTE's
                # Assuming CTE's have no schema, just a name
                deps = {d for d in deps if d.schema or d.name not in ctes}
                state.insert(target, deps)

            case exp.Drop():
                dbo = DBO(
                    node.this.db,
                    node.this.name,
                    "function" if node.kind == "FUNCTION" else "relation",
                )
                state.drop(dbo)

    return state.summary()


def extract_user_defined_functions(ast) -> Set[DBO]:
    functions = set()
    # Built-in functions have an empty name
    for f in [f for f in ast.find_all(exp.Func) if f.name]:
        name = f.name
        match f.parent:
            case exp.Dot():
                schema = f.parent.this.name
            case _:
                schema = ""
        functions.add(DBO(schema, name, "function"))
    return functions

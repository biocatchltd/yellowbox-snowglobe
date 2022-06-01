from __future__ import annotations

from typing import TYPE_CHECKING, Optional, List, Union

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Transaction, Connection, Row

if TYPE_CHECKING:
    from yellowbox_snowglobe.api import SnowGlobe

QUERY_RESPONSE = Union[None, int, List[Row]]


class SnowGlobeSession:
    next_token = 0

    def __init__(self, owner: SnowGlobe, db: Optional[str], schema: Optional[str]):
        self.owner = owner

        self.token = str(self.next_token)
        self.next_token += 1
        self.schema = schema
        self.db = None
        self.engine: Optional[Engine] = None
        self._connection: Optional[Connection] = None
        self._transaction: Optional[Transaction] = None
        if db:
            self.switch_db(db)

    @property
    def connection(self):
        if not self._connection:
            raise Exception("No connection exists, make sure to use a database first")
        return self._connection

    def switch_db(self, db_name: str):
        if self.db == db_name:
            return
        if self.engine:
            self.engine.dispose()
        self.db = db_name
        conn_string = self.owner.sql_service.database(db_name).local_connection_string()
        self.engine = create_engine(conn_string)
        self._connection = self.engine.connect()
        self._transaction = self._connection.begin()

    def do_query(self, query: str) -> QUERY_RESPONSE:
        # queries are always normalized to be without a semicolon
        query_lower = query.lower()
        exact_handler = self.FUNC_BY_EXACT.get(query_lower)
        if exact_handler:
            return exact_handler(self, query)

        prefix_search_root = self.FUNC_BY_PREFIX
        search_query = query_lower.split()
        # we assume to be always prefix-free, with a default fallback
        for word in search_query:
            if callable(prefix_search_root):
                break
            next_root = prefix_search_root.get(word) or prefix_search_root.get(None)
            while isinstance(next_root, str):
                next_root = prefix_search_root.get(next_root)

            if next_root:
                prefix_search_root = next_root
            else:
                print(f"!!! Unknown query: {query}")
                return None
        return prefix_search_root(self, query)

    def _do_ignore(self, query) -> QUERY_RESPONSE:
        return None

    def _do_commit(self, query) -> QUERY_RESPONSE:
        self._transaction.commit()
        return None

    def _do_rollback(self, query) -> QUERY_RESPONSE:
        self._transaction.rollback()
        return None

    def _do_use_database(self, query) -> QUERY_RESPONSE:
        _, _, db_name = query.rpartition(" ")
        self.switch_db(db_name)
        return None

    def _do_use_schema(self, query) -> QUERY_RESPONSE:
        _, _, schema_name = query.rpartition(" ")
        db_name, _, schema_name = schema_name.rpartition(".")
        if db_name:
            self.switch_db(db_name)
        self.schema = schema_name
        self.connection.execute(f"SET search_path TO {schema_name}")
        return None

    def _do_use_unknown(self, query) -> QUERY_RESPONSE:
        postfix = query[4:]
        if "." in postfix:
            qualified = "use schema " + postfix
        else:
            qualified = "use database " + postfix
        return self.do_query(qualified)

    def _do_select(self, query) -> QUERY_RESPONSE:
        result = self.connection.execute(text(query)).all()
        return result

    def _do_mutating_noresponse(self, query) -> QUERY_RESPONSE:
        self.connection.execute(text(query))
        return None

    def _do_mutating(self, query) -> QUERY_RESPONSE:
        result = self.connection.execute(text(query))
        return result.rowcount

    def _do_show_schemas(self, query) -> QUERY_RESPONSE:
        query = "select null as created_on, schema_name as name, null as is_default, null as is_current," \
                " null as database_name, null as owner, null as comment, null as options," \
                " null as retention_time FROM information_schema.schemata" + query[len("show schemas"):]
        return self.do_query(query)

    def _do_show_tables(self, query) -> QUERY_RESPONSE:
        query = "select null as created_on, table_name as name, table_catalog as database_name," \
                " table_schema as schema_name, 'TABLE' as kind, NULL as comment, NULL as cluster_by, NULL as rows," \
                " NULL as bytes, NULL as owner, NULL as retention_time, NULL as change_tracking," \
                " NULL as search_optimization, NULL as search_optimization_progress," \
                " NULL as search_optimization_bytes, NULL as is_external FROM information_schema.tables" \
                " WHERE table_type = 'BASE TABLE'" + query[len("show tables"):]
        return self.do_query(query)

    def _do_describe_table(self, query) -> QUERY_RESPONSE:
        _, _, table_name = query.rpartition(" ")
        query = "SELECT column_name as name, data_type as type, 'COLUMN' as kind, is_nullable as \"null?\"," \
                " column_default as default, NULL as primary_key, NULL as unique_key, NULL as check," \
                " NULL as expression, NULL as comment, NULL as \"policy name\" FROM information_schema.columns" \
                f" WHERE table_name = '{table_name}'"
        return self.do_query(query)

    FUNC_BY_EXACT = {
        "commit": _do_commit,
        "rollback": _do_rollback,
    }

    FUNC_BY_PREFIX = {  # all prefixes hase an implicit space after them
        "use": {
            "database": _do_use_database,
            "schema": _do_use_schema,
            None: _do_use_unknown,
        },
        "select": _do_select,
        "insert": _do_mutating_noresponse,
        "create": _do_mutating_noresponse,
        "delete": _do_mutating,
        "update": _do_mutating,
        "alter": {
            "table": _do_mutating_noresponse,
        },
        "show": {
            "schemas": _do_show_schemas,
            "tables": _do_show_tables,
        },
        "describe": {
            "table": _do_describe_table,
        },
        "desc": "describe"
    }

    def close(self):
        if self.engine:
            self.connection.close()
            self.engine.dispose()

# todo data types

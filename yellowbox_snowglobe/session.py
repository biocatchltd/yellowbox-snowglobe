from __future__ import annotations

from typing import TYPE_CHECKING, Optional, List, Union

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Transaction, Connection, Row

from yellowbox_snowglobe.schema_init import SCHEMA_INITIALIZE_SCRIPT

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
        self.schema = 'public'
        self._connection = self.engine.connect()
        self._transaction = self._connection.begin()
        self._initialize_schema()

    def _initialize_schema(self):
        """
        create all the necessary snowglobe conversions in the current schema
        """
        with self.connection.begin_nested():
            # right now, the only indicator of initialization is the presence of the metadata table
            exists = self.connection.execute(text(f"SELECT EXISTS (SELECT FROM information_schema.tables"
                                                  f" WHERE  table_schema = '{self.schema}'"
                                                  f" AND table_name = '{self.owner.metadata_table_name}')")).scalar()
            if exists:
                return
            self.connection.execute(f'SET search_path TO {self.schema};')
            self.connection.execute(SCHEMA_INITIALIZE_SCRIPT)
            self.connection.execute(text(f"CREATE TABLE IF NOT EXISTS {self.owner.metadata_table_name}()"))

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
            prefix_search_root = prefix_search_root.get(word) or prefix_search_root.get(None)
            if not prefix_search_root:
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

    def _do_set_schema(self, query) -> QUERY_RESPONSE:
        _, _, schema_name = query.rpartition(" ")
        # todo assert the schema exists
        self.schema = schema_name
        self._initialize_schema()
        return None

    def _do_retrieve(self, query) -> QUERY_RESPONSE:
        _, _, query_id = query.rpartition(" ")
        res = self.owner.query_results.pop(query_id, None)
        if res is None:
            return None  # todo some better handling here?
        return res

    def _do_select(self, query) -> QUERY_RESPONSE:
        result = self.connection.execute(text(query)).all()
        return result

    def _do_mutating_noresponse(self, query) -> QUERY_RESPONSE:
        self.connection.execute(text(query))
        return None

    def _do_mutating(self, query) -> QUERY_RESPONSE:
        result = self.connection.execute(text(query))
        return result.rowcount

    FUNC_BY_EXACT = {
        "!commit": _do_commit,
        "!rollback": _do_rollback,
    }

    FUNC_BY_PREFIX = {  # all prefixes hase an implicit space after them
        "!switch_db": _do_use_database,
        "!set_schema": _do_set_schema,
        "!retrieve": _do_retrieve,
        "select": _do_select,
        "insert": _do_mutating_noresponse,
        "create": {
            'database': _do_ignore,
            None: _do_mutating_noresponse,
        },
        "set": _do_mutating_noresponse,
        "delete": _do_mutating,
        "update": _do_mutating,
        "alter": {
            "table": _do_mutating_noresponse,
        },
    }

    def close(self):
        if self.engine:
            self.connection.close()
            self.engine.dispose()

# todo data types

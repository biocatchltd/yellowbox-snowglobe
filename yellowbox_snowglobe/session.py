from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence, Set

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine, Row, Transaction

from yellowbox_snowglobe.schema_init import SCHEMA_INITIALIZE_SCRIPT

if TYPE_CHECKING:
    from yellowbox_snowglobe.api import SnowGlobeAPI

QUERY_RESPONSE = Optional[Sequence[Row]]


class SnowGlobeSession:
    # a session is an individual connection, containing all the data associated with it.
    next_token = 0  # each session is identified by a token the connector must send on every request

    def __init__(self, owner: SnowGlobeAPI, db: Optional[str], schema: str):
        self.owner = owner

        self.token = str(self.next_token)
        type(self).next_token += 1
        self.schema = schema
        # all these fields are set and replaced together when we switch the DB
        self.db: Optional[str] = None
        self.engine: Optional[Engine] = None
        self._connection: Optional[Connection] = None
        self._transaction: Optional[Transaction] = None

        self.known_columns: Set[str] = set()  # stores all the columns we know about, updates when a create or alter
        # is called
        if db:
            self.switch_db(db, schema)

    @property
    def connection(self) -> Connection:
        if not self._connection:
            raise Exception("No connection exists, make sure to use a database first")
        return self._connection

    @property
    def transaction(self) -> Transaction:
        if not self._transaction:
            raise Exception("No connection exists, make sure to use a database first")
        return self._transaction

    def switch_db(self, db_name: str, schema_name: str = "public"):
        if self.db == db_name:
            return
        if self.engine:
            self.engine.dispose()
        self.db = db_name
        conn_string = self.owner.sql_service.database(db_name).local_connection_string()
        self.engine = create_engine(conn_string)
        self.schema = schema_name
        self._connection = self.engine.connect()
        self._transaction = self._connection.begin()
        self._initialize_schema()

    def _initialize_schema(self):
        """
        create all the necessary snowglobe conversions in the current schema
        """
        with self.connection.begin_nested():
            # right now, the only indicator of initialization is the presence of the metadata table
            exists = self.connection.execute(
                text(
                    f"SELECT EXISTS(SELECT FROM information_schema.tables"
                    f" WHERE  table_schema = '{self.schema}'"
                    f" AND table_name = '{self.owner.metadata_table_name}')"
                )
            ).scalar()
            if exists:
                return
            self.connection.execute(text(f"SET search_path TO {self.schema};"))
            self.connection.execute(SCHEMA_INITIALIZE_SCRIPT)
            self.connection.execute(text(f"CREATE TABLE IF NOT EXISTS {self.owner.metadata_table_name}()"))

    def do_query(self, query: str) -> QUERY_RESPONSE:
        # queries are always normalized to be without a semicolon
        query_lower = query.lower()
        prefix_search_root: Any = self.FUNC_BY_PREFIX
        search_query = query_lower.split()
        # we assume to be always prefix-free, with a default fallback
        for word in search_query:
            if callable(prefix_search_root):
                break
            prefix_search_root = prefix_search_root.get(word) or prefix_search_root.get(None)
            if not prefix_search_root:
                print(f"Unknown query: {query}")
                return None
        return prefix_search_root(self, query)

    # region handlers
    def _do_ignore(self, query: str) -> QUERY_RESPONSE:
        return None

    def _do_commit(self, query: str) -> QUERY_RESPONSE:
        self.transaction.commit()
        return None

    def _do_rollback(self, query: str) -> QUERY_RESPONSE:
        self.transaction.rollback()
        return None

    def _do_use_database(self, query: str) -> QUERY_RESPONSE:
        _, _, db_name = query.rpartition(" ")
        self.switch_db(db_name)
        return None

    def _do_set_schema(self, query: str) -> QUERY_RESPONSE:
        _, _, schema_name = query.rpartition(" ")
        # todo assert the schema exists
        self.schema = schema_name
        self._initialize_schema()
        return None

    def _do_retrieve(self, query: str) -> QUERY_RESPONSE:
        _, _, query_id = query.rpartition(" ")
        res = self.owner.query_results.pop(query_id, None)
        if res is None:
            return None  # todo some better handling here?
        return res

    def _do_select(self, query: str) -> QUERY_RESPONSE:
        result = self.connection.execute(text(query)).all()
        return result

    def _do_mutating_noresponse(self, query: str) -> QUERY_RESPONSE:
        self.connection.execute(text(query))
        # we need to update the known columns here
        result = self.connection.execute(
            text(
                "select column_name from information_schema.columns c where c.table_schema <> 'pg_catalog'"
                "AND c.table_schema <> 'information_schema';"
            )
        )
        self.known_columns = set(result.scalars().fetchall())
        return None

    # endregion

    # here we map SQL keywords to whichever handler we want to use on queries with them
    # think of this like a prefix trie, with NONE as a fallback
    # all keywords beginning with "!" are specially generated by the post_to_snow transpiler.
    FUNC_BY_PREFIX: Dict[Optional[str], Any] = {  # all prefixes have an implicit space after them
        "!commit": _do_commit,
        "!rollback": _do_rollback,
        "!switch_db": _do_use_database,
        "!set_schema": _do_set_schema,
        "!retrieve": _do_retrieve,
        "select": _do_select,
        "insert": _do_mutating_noresponse,
        "create": {
            "database": _do_ignore,
            None: _do_mutating_noresponse,
        },
        "set": _do_mutating_noresponse,
        "delete": _do_mutating_noresponse,
        "update": _do_mutating_noresponse,
        "alter": {
            "table": _do_mutating_noresponse,
        },
    }

    def close(self):
        if self.engine:
            self.connection.close()
            self.engine.dispose()


# todo data types

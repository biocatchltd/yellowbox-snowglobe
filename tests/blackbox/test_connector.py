from snowflake import connector
from snowflake.connector import DictCursor


def test_select_as(connection):
    connection.cursor().execute("create table bar (x int, y text)")
    connection.cursor().execute("insert into bar values (1, 'one'), (2, 'two'), (3, 'three'), (10, 'ten')")
    res = connection.cursor(DictCursor).execute("select x as a, y as b from bar;").fetchall()
    assert res == [
        {"a": 1, "b": "one"},
        {"a": 2, "b": "two"},
        {"a": 3, "b": "three"},
        {"a": 10, "b": "ten"},
    ]


def test_select_as_name(connection):
    connection.cursor().execute("create table bar (x int, y text)")
    connection.cursor().execute("insert into bar values (1, 'one'), (2, 'two'), (3, 'three'), (10, 'ten')")
    res = connection.cursor(DictCursor).execute('select bar.x::string as "bar.x::string" from bar;').fetchall()
    assert res == [
        {"bar.x::string": "1"},
        {"bar.x::string": "2"},
        {"bar.x::string": "3"},
        {"bar.x::string": "10"},
    ]


def test_async(connection):
    with connection.cursor() as cursor:
        cursor.execute("create table bar (x int, y text)")
        cursor.execute("insert into bar values (1, 'one'), (2, 'two'), (3, 'three'), (10, 'ten')")
        cursor.execute_async("select x, y from bar;")
        query_id = cursor.sfqid
        cursor.get_results_from_sfqid(query_id)
        assert cursor.fetchall() == [
            (1, "one"),
            (2, "two"),
            (3, "three"),
            (10, "ten"),
        ]
        cursor.execute_async("select x from bar where x = 4;")  # empty result
        query_id = cursor.sfqid
        cursor.get_results_from_sfqid(query_id)
        assert not cursor.fetchall()


def test_args(connection):
    with connection.cursor() as cursor:
        cursor.execute("create table bar (x int, y text)")
        cursor.execute("insert into bar values (1, 'one'), (2, 'two'), (3, 'three'), (10, 'ten')")
        cursor.execute("select x, y from bar where x = %s", (10,))
        assert cursor.fetchall() == [(10, "ten")]


def test_create_and_switch_db(db, snowglobe):
    new_db_name = db + "_new"
    with (
        connector.connect(**snowglobe.local_connection_kwargs(), database=db) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(f"create database {new_db_name}")
        cursor.execute(f"use database {new_db_name}")
        cursor.execute("create table bar (x int, y text)")
        cursor.execute("insert into bar values (1, 'one'), (2, 'two'), (3, 'three'), (10, 'ten')")
        cursor.execute("select x, y from bar where x = %s", (10,))
        assert cursor.fetchall() == [(10, "ten")]

    assert snowglobe.sql_service.database_exists(new_db_name)


def test_switch_to_same_db(db, snowglobe, connection):
    with connection.cursor() as cursor:
        cursor.execute(f"create database {db}")
        cursor.execute(f"use database {db}")
        cursor.execute("create table bar (x int, y text)")
        cursor.execute("insert into bar values (1, 'one'), (2, 'two'), (3, 'three'), (10, 'ten')")
        cursor.execute("select x, y from bar where x = %s", (10,))
        assert cursor.fetchall() == [(10, "ten")]

    assert snowglobe.sql_service.database_exists(db)

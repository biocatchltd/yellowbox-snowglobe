from datetime import datetime

from pytest import mark


def test_double_dot(connection, db):
    connection.cursor().execute("create table bar (x int, y text)")
    connection.cursor().execute("insert into bar values (1, 'one'), (2, 'two'), (3, 'three'), (10, 'ten')")
    schema_res = connection.cursor().execute(f"select * from {db}..bar;").fetchall()
    names = {row[0] for row in schema_res}
    assert {1, 2, 3, 10} == names


def test_lateral_flatten(connection):
    connection.cursor().execute("create table bar (x int, d int[])")
    connection.cursor().execute(
        "insert into bar values (1, '{}'), (2, '{2}'), (3, '{3}'),"
        " (4, '{2}'), (5, '{5}'), (6, '{2, 3}'), (7, '{7}'), (8, '{2}'), (9, '{3}')"
    )
    res = (
        connection.cursor()
        .execute("select x, root.value from bar, lateral flatten(d) as root" " where root.value*root.value = x;")
        .fetchall()
    )
    assert res == [(4, 2), (9, 3)]


def test_cast(connection):
    connection.cursor().execute("create table bar (x int, y text)")
    connection.cursor().execute("insert into bar values (1, 'one'), (2, 'two'), (3, 'three'), (10, 'ten')")
    res = connection.cursor().execute("select x::text from bar;").fetchall()
    assert res == [("1",), ("2",), ("3",), ("10",)]


@mark.parametrize("schema", ["public", "loolie", None])
def test_date(connection, schema):
    if schema:
        connection.cursor().execute(f"create schema IF NOT EXISTS {schema}")
        connection.cursor().execute(f"use schema {schema}")
    connection.cursor().execute("create table bar (x DATE, y text)")
    connection.cursor().execute(
        "insert into bar values ('2020-01-01', 'one'), ('2020-02-02', 'two'), "
        "('03-MAR-2020', 'three'), ('2020-10-10', 'ten')"
    )
    res = connection.cursor().execute("select year(x), month(x), day(x) from bar;").fetchall()
    assert res == [(2020, 1, 1), (2020, 2, 2), (2020, 3, 3), (2020, 10, 10)]


def test_bools(connection):
    connection.cursor().execute("create table bar (x int, y text)")
    connection.cursor().execute("insert into bar values (1, 'one'), (2, 'two'), (3, 'three'), (10, 'ten')")
    res = connection.cursor().execute("select x, y LIKE 't%' from bar;").fetchall()
    assert res == [(1, False), (2, True), (3, True), (10, True)]


def test_null_bools_and_dates(connection):
    connection.cursor().execute("create table bar (x timestamp, y boolean)")
    connection.cursor().execute(
        "insert into bar values " "('2014-01-01 16:00:00', true)," " (null, false), " "('2023-01-08 17:00:00', null)"
    )
    res = connection.cursor().execute("select * from bar;").fetchall()
    assert res == [(datetime(2014, 1, 1, 16, 0), True), (None, False), (datetime(2023, 1, 8, 17, 0), None)]


@mark.parametrize(("queried_sample", "expected_sample"), [("2", 2), ("2.3", 2), ("2.5", 3), ("2.7", 3)])
def test_sample_query(connection, queried_sample, expected_sample):
    connection.cursor().execute("create table bar (x int, y text)")
    connection.cursor().execute("insert into bar values (1, 'one'), (2, 'two'), (3, 'three'), (10, 'ten')")
    res = connection.cursor().execute(f"select x, y from bar sample ({queried_sample} rows);").fetchall()
    assert len(res) == expected_sample


@mark.parametrize(
    ("query", "expected"),
    [
        ("select x, y:a::string from bar;", [(1, "1"), (2, "2")]),
        ("select x, y:a::number from bar;", [(1, 1), (2, 2)]),
    ],
)
def test_json(connection, db, query, expected):
    connection.cursor().execute("create table bar (x int, y json)")
    connection.cursor().execute("""insert into bar values (1, '{"a":"1", "b":"1"}'), (2, '{"a":"2", "b":"2"}')""")
    res = connection.cursor().execute(query).fetchall()
    assert res == expected

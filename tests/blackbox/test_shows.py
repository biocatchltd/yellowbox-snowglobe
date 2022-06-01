def test_show_schemas(connection):
    connection.cursor().execute('create schema loolie')
    connection.cursor().execute('create schema moonie')

    schema_res = connection.cursor().execute("show schemas;").fetchall()
    names = {row[1] for row in schema_res}
    assert {'loolie', 'moonie', 'public'} <= names


def test_show_tables(connection):
    connection.cursor().execute('create schema loolie')
    connection.cursor().execute('create schema moonie')

    connection.cursor().execute('create table loolie.a ()')
    connection.cursor().execute('create table loolie.b ()')
    connection.cursor().execute('create table moonie.c ()')
    connection.cursor().execute('create table public.a ()')
    connection.cursor().execute('create table public.b ()')

    schema_res = connection.cursor().execute("show tables;").fetchall()
    names = {(row[1], row[3]) for row in schema_res}
    assert {('a', 'loolie'), ('b', 'loolie'), ('c', 'moonie'), ('a', 'public'), ('b', 'public')} <= names


def test_describe_tables(connection):
    connection.cursor().execute('create table bar (x int, y text)')
    schema_res = connection.cursor().execute("describe table bar;").fetchall()
    names = {row[0] for row in schema_res}
    assert {'x', 'y'} == names

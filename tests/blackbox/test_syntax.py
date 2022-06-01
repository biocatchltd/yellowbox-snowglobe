def test_describe_tables(connection, db):
    connection.cursor().execute('create table bar (x int, y text)')
    connection.cursor().execute("insert into bar values (1, 'one'), (2, 'two'), (3, 'three'), (10, 'ten')")
    schema_res = connection.cursor().execute(f"select * from \"{db}..bar\";").fetchall()
    names = {row[0] for row in schema_res}
    assert {1, 2, 3, 10} == names

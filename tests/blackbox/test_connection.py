import snowflake.connector as connector
from pytest import raises


def test_rollback(snowglobe, db):
    with connector.connect(**snowglobe.local_connection_kwargs(), database=db) as conn:
        conn.cursor().execute('create table recs(t text);')

    with raises(RuntimeError):
        with connector.connect(**snowglobe.local_connection_kwargs(), database=db) as conn:
            conn.cursor().execute("insert into recs values ('1'), ('2');")
            assert conn.cursor().execute('select * from recs').fetchall() == [('1',), ('2',)]
            raise RuntimeError('rollback')
    with connector.connect(**snowglobe.local_connection_kwargs(), database=db) as conn:
        conn.cursor().execute("insert into recs values ('3'), ('4');")
        assert conn.cursor().execute('select * from recs').fetchall() == [('3',), ('4',)]
        # commit
    with raises(RuntimeError):
        with connector.connect(**snowglobe.local_connection_kwargs(), database=db) as conn:
            conn.cursor().execute("insert into recs values ('5'), ('6');")
            assert conn.cursor().execute('select * from recs').fetchall() == [('3',), ('4',), ('5',), ('6',)]
            raise RuntimeError('rollback')
    with connector.connect(**snowglobe.local_connection_kwargs(), database=db) as conn:
        conn.cursor().execute("insert into recs values ('7'), ('8');")
        assert conn.cursor().execute('select * from recs').fetchall() == [('3',), ('4',), ('7',), ('8',)]

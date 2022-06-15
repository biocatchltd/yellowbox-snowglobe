import snowflake.connector as connector
from pytest import mark

from yellowbox_snowglobe.service import SnowGlobeService


def test_connect(docker_client):
    with SnowGlobeService.run(docker_client) as service, \
            connector.connect(**service.local_connection_kwargs()) as conn:
        conn.cursor().execute('use database foo;')
        conn.cursor().execute('create schema loolie')
        conn.cursor().execute('use schema loolie')
        assert conn.database == 'foo'
        assert conn.schema == 'loolie'
        conn.cursor().execute('create table bar (x int, y text)')
        conn.cursor().execute("insert into bar values (1, 'one'), (2, 'two'), (3, 'three'), (10, 'ten')")
        conn.cursor().execute('delete from bar where x = 10')
        results = conn.cursor().execute("select x, y from bar where y like 't%'").fetchall()
        assert results == [(2, 'two'), (3, 'three')]


@mark.asyncio
async def test_connect_async(docker_client):
    async with SnowGlobeService.arun(docker_client) as service:
        with connector.connect(**service.local_connection_kwargs()) as conn:
            conn.cursor().execute('use database foo;')
            conn.cursor().execute('create schema loolie')
            conn.cursor().execute('use schema loolie')
            conn.cursor().execute('create table bar (x int, y text)')
            conn.cursor().execute("insert into bar values (1, 'one'), (2, 'two'), (3, 'three'), (10, 'ten')")
            conn.cursor().execute('delete from bar where x = 10')
            results = conn.cursor().execute("select x, y from bar where y like 't%'").fetchall()
            assert results == [(2, 'two'), (3, 'three')]


def test_connect_to_db(docker_client):
    with SnowGlobeService.run(docker_client) as service:
        with connector.connect(**service.local_connection_kwargs(), database='foo') as conn:
            conn.cursor().execute('create schema loolie')
            assert conn.database == 'foo'
            assert conn.schema == 'public'
        with connector.connect(**service.local_connection_kwargs(), database='foo', schema='loolie') as conn:
            assert conn.database == 'foo'
            assert conn.schema == 'loolie'
            conn.cursor().execute('create table bar (x int, y text)')
            conn.cursor().execute("insert into bar values (1, 'one'), (2, 'two'), (3, 'three'), (10, 'ten')")
            conn.cursor().execute('delete from bar where x = 10')
            results = conn.cursor().execute("select x, y from bar where y like 't%'").fetchall()
            assert results == [(2, 'two'), (3, 'three')]

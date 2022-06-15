import snowflake.connector as connector
from pytest import fixture

from yellowbox_snowglobe.service import SnowGlobeService


@fixture(scope="session")
def snowglobe(docker_client) -> SnowGlobeService:
    with SnowGlobeService.run(docker_client) as service:
        yield service


next_db = 0


@fixture
def db():
    global next_db
    next_db += 1
    return f'db_{next_db}'


@fixture
def connection(snowglobe: SnowGlobeService, db):
    with connector.connect(**snowglobe.local_connection_kwargs(), database=db) as conn:
        yield conn

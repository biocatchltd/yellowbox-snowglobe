import pytest
from snowflake import connector

from yellowbox_snowglobe.service import SnowGlobeService


@pytest.fixture(scope="session")
def snowglobe(docker_client) -> SnowGlobeService:
    with SnowGlobeService.run(docker_client) as service:
        yield service


next_db = 0


@pytest.fixture()
def db():
    global next_db
    next_db += 1
    return f"db_{next_db}"


@pytest.fixture()
def connection(snowglobe: SnowGlobeService, db):
    with connector.connect(**snowglobe.local_connection_kwargs(), database=db) as conn:
        yield conn

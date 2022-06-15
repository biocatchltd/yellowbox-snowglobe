# yellowbox-snowglobe: A local snowflake emulator for testing and development

yellowbox-snowglobe is a library to run a local snowflake emulator on your machine. It uses 
[yellowbox](https://github.com/biocatchltd/yellowbox) to run a docker container+webservice to 
emulate a snowflake instance.

```python
from yellowbox.clients import open_docker_client

from yellowbox_snowglobe import SnowGlobeService
import snowflake.connector as connector


with open_docker_client() as dc, \
      SnowGlobeService.run(dc) as service, \
      connector.connect(**service.local_connection_kwargs()) as conn:
    conn.cursor().execute('use database foo;')
    conn.cursor().execute('create table bar (x int, y text)')
    conn.cursor().execute("insert into bar values (1, 'one'), (2, 'two'), (3, 'three'), (10, 'ten')")
    conn.cursor().execute('delete from bar where x = 10')
    results = conn.cursor().execute("select x, y from bar where y like 't%'").fetchall()
    assert results == [(2, 'two'), (3, 'three')]
```

## ⚠ DISCLAIMER ⚠
Snowglobe is in very early development, all the features that snowglobe currently supports were 
implemented on a need-to-have basis for internal development. There are many features and
functionalities that are currently missing from it. However, the infrastructure makes it relatively
easy to add new features. If you need something snowglobe doesn't support, please open an issue on
github, or open a PR that adds this functionality.

Also, many implemented features have some quirks and limitations. See `known_quirks.md` for a full 
list of known differences from a real snowflake instance.

## Under The Hood
Snowglobe is implemented using two yellowbox services that work together: a Webserver (the api) and
a Postgresql container (the database). The snowflake connector sends HTTP requests to the webserver,
which then translates the snowflake-dialect query to one that can be handled by the postgresql 
container, and then forwards the query to the postgresql container. 
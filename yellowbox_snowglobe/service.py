from yellowbox.extras.postgresql import PostgreSQLService
from yellowbox.utils import docker_host_name

from yellowbox_snowglobe.api import SnowGlobeAPI


class SnowGlobeService(PostgreSQLService):
    def __init__(self, *args, metadata_table_name: str = '__snowglobe_md', **kwargs):
        super().__init__(*args, **kwargs)
        self.api = SnowGlobeAPI(sql_service=self, metadata_table_name=metadata_table_name)

    @property
    def api_port(self):
        # the http port snowflake connectors should use to connect
        return self.api.port

    def start(self, *args, **kwargs):
        super().start(*args, **kwargs)
        self.api.start()
        return self

    async def astart(self, *args, **kwargs):
        await super().astart(*args, **kwargs)
        self.api.start()
        return self

    def stop(self, *args):
        self.api.stop()
        super().stop(*args)

    def _base_connection_kwargs(self):
        return dict(
            port=self.api_port,
            user='MyUser',
            password='MyPass',
            account='MyAccount',
            protocol='http',
        )

    def local_connection_kwargs(self):
        return {
            'host': 'localhost',
            **self._base_connection_kwargs(),
        }

    def container_connection_kwargs(self):
        return {
            'host': docker_host_name,
            **self._base_connection_kwargs(),
        }

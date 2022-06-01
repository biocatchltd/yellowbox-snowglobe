from yellowbox.extras.postgresql import PostgreSQLService

from yellowbox_snowglobe.api import SnowGlobe


class SnowGlobeService(PostgreSQLService):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api = SnowGlobe(sql_service=self)

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

    def local_connection_kwargs(self):
        return dict(
            host='localhost',
            port=self.api.port,
            user='MyUser',
            password='MyPass',
            account='MyAccount',
            protocol='http',
        )

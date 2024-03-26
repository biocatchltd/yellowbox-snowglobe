from __future__ import annotations

from typing import Any

from yellowbox import AsyncRunMixin, RunMixin, YellowService
from yellowbox.extras.postgresql import PostgreSQLService
from yellowbox.utils import docker_host_name

from yellowbox_snowglobe.api import SnowGlobeAPI
from yellowbox_snowglobe.case_mode import CaseMode, IgnoreAll


class SnowGlobeService(YellowService, RunMixin, AsyncRunMixin):
    def __init__(self, *args, metadata_table_name: str = "__snowglobe_md", case_mode: CaseMode = IgnoreAll(), **kwargs):
        super().__init__()
        self.sql_service = PostgreSQLService(*args, **kwargs)
        self.api = SnowGlobeAPI(
            sql_service=self.sql_service, metadata_table_name=metadata_table_name, case_mode=case_mode
        )

    @property
    def api_port(self) -> int:
        # the http port snowflake connectors should use to connect
        return self.api.port

    def start(self, *args, **kwargs) -> SnowGlobeService:
        self.sql_service.start(*args, **kwargs)
        self.api.start()
        return self

    async def astart(self, *args, **kwargs) -> Any:
        await self.sql_service.astart(*args, **kwargs)
        self.api.start()
        return self

    def stop(self, *args) -> None:
        self.api.stop()
        self.sql_service.stop(*args)

    def is_alive(self) -> bool:
        return self.api.is_alive() and self.sql_service.is_alive()

    def _base_connection_kwargs(self) -> dict:
        return {
            "port": self.api_port,
            "user": "MyUser",
            "password": "MyPass",
            "account": "MyAccount",
            "protocol": "http",
        }

    def local_connection_kwargs(self) -> dict:
        return {
            "host": "localhost",
            **self._base_connection_kwargs(),
        }

    def container_connection_kwargs(self) -> dict:
        return {
            "host": docker_host_name,
            **self._base_connection_kwargs(),
        }

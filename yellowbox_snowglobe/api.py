from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from datetime import datetime
from traceback import print_exc
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

from sqlalchemy.engine import Row
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from yellowbox.extras.postgresql import PostgreSQLService
from yellowbox.extras.webserver import WebServer, class_http_endpoint

from yellowbox_snowglobe.session import SnowGlobeSession
from yellowbox_snowglobe.snow_to_post import snow_to_post, split_sql_to_statements


async def unpack_request_body(request: Request):
    body = await request.body()
    uz = gzip.decompress(body)
    return json.loads(uz)


@dataclass
class SnowType:
    """
    A column type that can be returned to the connector
    """
    name: str
    connector_converter: Optional[Callable[[Any], Any]] = None
    """
    This converter is used to convert the python value to a value the connector can use (currently only used for bools)
    """


PY_TYPE_TO_SNOW_TYPE = {
    int: SnowType('FIXED'),
    str: SnowType('TEXT'),
    float: SnowType('REAL'),
    bool: SnowType('BOOLEAN', lambda x: str(int(x))),
    datetime: SnowType('DATETIME', str)
}  # todo there are a lot more

VARIANT = SnowType('VARIANT')  # this will be the default snow type for when we can't handle the result type


def sql_alchemy_result_to_snowglobe_result(result: List[Row]) -> Dict[str, Any]:
    col_names = result[0]._fields
    columns = [{'name': col_name.upper(), "length": 0, "precision": 0, "scale": 0, "nullable": False}
               for col_name in col_names]
    rows = [list(row) for row in result]
    for i, col in enumerate(columns):
        t = None
        for row in rows:
            if row[i] is None:
                col['nullable'] = True
            else:
                proposed_type = PY_TYPE_TO_SNOW_TYPE.get(type(row[i]))
                if proposed_type is None:
                    # unrecognized type, call it a variant and be done with it
                    t = VARIANT
                elif t is None:
                    t = proposed_type
                elif t != proposed_type:
                    # type conflict (I don't know if this can even happen), call it a variant
                    t = VARIANT
        if t is None:
            # if no type was found, we default the column type to variant, AFAICT this will only happen for a result
            # without rows
            t = VARIANT
        col['type'] = t.name
        if t.connector_converter is not None:
            for row in rows:
                row[i] = t.connector_converter(row[i])
    return {'rowtype': columns, 'rowset': rows}


class SnowGlobeAPI(WebServer):
    def __init__(self, *args, sql_service: PostgreSQLService, metadata_table_name: str, **kwargs):
        super().__init__('snowglobe', *args, **kwargs)
        self.sql_service = sql_service

        self.sessions: Dict[str, SnowGlobeSession] = {}  # stores all the live sessions
        self.metadata_table_name = metadata_table_name

        self.query_results: Dict[str, List[Row]] = {}  # stores all the async query results

    def session_from_request(self, request):
        """
        Get a request's relevant session
        """
        auth = request.headers.get('Authorization')
        if not auth:
            raise HTTPException(status_code=401, detail='No Authorization header')
        if not auth.startswith('Snowflake Token="'):
            raise HTTPException(status_code=401, detail='Invalid Authorization header')
        token = auth[17:-1]
        if token not in self.sessions:
            raise HTTPException(status_code=401, detail='Invalid Authorization header')
        return self.sessions[token]

    @class_http_endpoint(['POST'], '/session/v1/login-request')  # type: ignore[arg-type]
    async def login_request(self, request: Request):
        db = request.query_params.get('databaseName')
        schema = request.query_params.get('schemaName', 'public')
        session = SnowGlobeSession(self, db, schema)
        self.sessions[session.token] = session
        return JSONResponse({'data': {'token': session.token, 'masterToken': 'SwordFish'}, 'success': True})

    @class_http_endpoint(['POST'], '/session')  # type: ignore[arg-type]
    async def delete_session(self, request: Request):
        if request.query_params.get('delete') == 'true':
            session = self.session_from_request(request)
            del self.sessions[session.token]
            session.close()
            return JSONResponse({'success': True})
        return Response(status_code=404)

    @class_http_endpoint(['POST'], '/queries/v1/query-request')  # type: ignore[arg-type]
    async def query_request(self, request: Request):
        try:
            session = self.session_from_request(request)
            body = await unpack_request_body(request)
            query = body['sqlText']
            if query.endswith(';'):
                query = query[:-1]
            post = snow_to_post(query)  # note that this query might well now have multiple statements, but only the
            # last one counts
            stmts = list(split_sql_to_statements(post))
            if not stmts:
                return JSONResponse({'success': False, 'message': 'no query provided'})
            result = None
            for stmt in stmts:
                result = session.do_query(stmt)
            query_id = str(uuid4())
            data = {
                'finalDatabaseName': session.db,
                'finalSchemaName': session.schema,
                "rowtype": [],
                "rowset": [],
                "queryId": query_id,
            }
            if not result:
                return JSONResponse({'data': data, 'success': True})
            if body.get('asyncExec', False):
                # we should store the result in the server for when it gets retrieved
                self.query_results[query_id] = result
            data.update(sql_alchemy_result_to_snowglobe_result(result))
            return JSONResponse({'data': data, 'success': True})
        except Exception as e:
            print_exc()  # we print exec here because the connector + webservice combo doesn't always do a good job of
            # telling us what the error is (or that it's happening)
            return JSONResponse({'success': False, 'message': str(e)})

    @class_http_endpoint(['GET'], '/monitoring/queries/{query_id:str}')  # type: ignore[arg-type]
    async def query_monitoring_query(self, request: Request):
        query_id = request.path_params['query_id']
        if query_id not in self.query_results:
            return JSONResponse({'success': False, 'message': 'query not found'})
        # note that we always execute synchronously, so we can just return a static success
        return JSONResponse({'data': {
            'queries': [{"status": 'SUCCESS'}]
        }, 'success': True})

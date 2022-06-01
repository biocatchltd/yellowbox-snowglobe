from __future__ import annotations

import gzip
import json
from traceback import print_exc
from typing import Dict

from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from yellowbox.extras.postgresql import PostgreSQLService
from yellowbox.extras.webserver import WebServer, class_http_endpoint

from yellowbox_snowglobe.session import SnowGlobeSession


async def unpack_request_body(request: Request):
    body = await request.body()
    uz = gzip.decompress(body)
    return json.loads(uz)

PY_TYPE_TO_SNOW_TYPE = {
    int: ('FIXED', None),
    str: ('TEXT', None),
    float: ('REAL', None),
    bool: ('BOOLEAN', lambda x: str(int(x))),
}  # todo there are a lot more

class SnowGlobe(WebServer):
    def __init__(self, *args, sql_service: PostgreSQLService, **kwargs):
        super().__init__('snowglobe', *args, **kwargs)
        self.sql_service = sql_service
        self.sessions: Dict[str, SnowGlobeSession] = {}

    def session_from_request(self, request):
        auth = request.headers.get('Authorization')
        if not auth:
            raise HTTPException(status_code=401, detail='No Authorization header')
        if not auth.startswith('Snowflake Token="'):
            raise HTTPException(status_code=401, detail='Invalid Authorization header')
        token = auth[17:-1]
        if token not in self.sessions:
            raise HTTPException(status_code=401, detail='Invalid Authorization header')
        return self.sessions[token]

    @class_http_endpoint(['POST'], '/session/v1/login-request')
    async def login_request(self, request: Request):
        db = request.query_params.get('databaseName')
        schema = request.query_params.get('schemaName', 'public')
        session = SnowGlobeSession(self, db, schema)
        self.sessions[session.token] = session
        return JSONResponse({'data': {'token': session.token, 'masterToken': 'SwordFish'}, 'success': True})

    @class_http_endpoint(['POST'], '/session')
    async def delete_session(self, request: Request):
        if request.query_params.get('delete') == 'true':
            session = self.session_from_request(request)
            del self.sessions[session.token]
            session.close()
            return JSONResponse({'success': True})
        return Response(status_code=404)

    @class_http_endpoint(['POST'], '/queries/v1/query-request')
    async def query_request(self, request: Request):
        try:
            session = self.session_from_request(request)
            query = (await unpack_request_body(request))['sqlText']
            if query.endswith(';'):
                query = query[:-1]
            result = session.do_query(query)
            data = {
                'finalDatabaseName': session.db,
                'finalSchemaName': session.schema,
                "rowtype": [],
                "rowset": [],
            }
            if isinstance(result, int):
                return JSONResponse({'data': data, 'success': True})
            elif not result:
                return JSONResponse({'data': data, 'success': True})
            else:
                # we need to guess the types of the columns
                col_names = result[0]._fields
                columns = [{'name': col_name, "length": 0, "precision": 0, "scale": 0, "nullable": False}
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
                                t = 'VARIANT'
                            elif t is None:
                                t = proposed_type
                            elif t != proposed_type:
                                t = 'VARIANT'
                    col['type'] = 'VARIANT' if t is None else t[0]
                    if t is not None and t[1] is not None:
                        for row in rows:
                            row[i] = t[1](row[i])
                data['rowtype'] = columns
                data['rowset'] = rows
                return JSONResponse({'data': data, 'success': True})
        except Exception as e:
            print_exc()
            return JSONResponse({'success': False, 'message': str(e)})



            

"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""
import json
import uuid
from tornado import gen
from tornado import ioloop
from tornado import websocket

from gremlin_python.structure.io.graphson import GraphSONReader
from gremlin_python.structure.io.graphson import GraphSONWriter
from .remote_connection import RemoteConnection
from .remote_connection import RemoteTraversal
from .remote_connection import RemoteTraversalSideEffects


class GremlinServerError(Exception):
    pass


class DriverRemoteConnection(RemoteConnection):
    def __init__(self, url, traversal_source, loop=None, username="", password=""):
        super(DriverRemoteConnection, self).__init__(url, traversal_source)
        if loop is None:
            self._loop = ioloop.IOLoop.current()
        self._ws = self._loop.run_sync(lambda: websocket.websocket_connect(self.url))
        self._username = username
        self._password = password

    def submit(self,
               bytecode,
               op="bytecode",
               processor="traversal"):
        request_id = str(uuid.uuid4())
        traversers = self._loop.run_sync(lambda: self.submit_traversal_bytecode(bytecode, request_id))
        keys_lambda = lambda: self._loop.run_sync(lambda: self.submit_sideEffect_keys(request_id))
        value_lambda = lambda key: self._loop.run_sync(lambda: self.submit_sideEffect_value(request_id, key))
        return RemoteTraversal(iter(traversers), RemoteTraversalSideEffects(keys_lambda, value_lambda))

    @gen.coroutine
    def submit_traversal_bytecode(self, bytecode, request_id):
        message = self._get_traversal_bytecode_message(bytecode, request_id)
        traversers = yield self._execute_message(message)
        raise gen.Return(traversers)

    @gen.coroutine
    def submit_sideEffect_keys(self, request_id):
        message = self._get_sideEffect_keys_message(request_id)
        keys = yield self._execute_message(message)
        raise gen.Return(set(keys))

    @gen.coroutine
    def submit_sideEffect_value(self, request_id, key):
        message = self._get_sideEffect_value_message(request_id, key)
        side_effects = yield self._execute_message(message)
        raise gen.Return(side_effects)

    @gen.coroutine
    def _execute_message(self, message):
        if self._ws.protocol is None:
            self._ws = yield websocket.websocket_connect(self.url)
        self._ws.write_message(message, binary=True)
        resp = Response(self._ws, self._username, self._password)
        results = None
        while True:
            msg = yield resp.receive()
            if msg is None:
                break
            # on first message, get the right result data structure
            if None == results:
                if "list" == msg[0]:
                    results = []
                elif "set" == msg[0]:
                    results = set()
                elif "map" == msg[0]:
                    results = {}
                else:
                    results = []
            # updating a map is different than a list or a set
            if isinstance(results, dict):
                for item in msg[1]:
                    results.update(item)
            else:
                results += msg[1]
        raise gen.Return([] if None == results else results)

    def close(self):
        self._ws.close()

    def _get_traversal_bytecode_message(self, bytecode, request_id):
        message = {
            "requestId": {
                "@type": "g:UUID",
                "@value": request_id
            },
            "op": "bytecode",
            "processor": "traversal",
            "args": {
                "gremlin": GraphSONWriter.writeObject(bytecode),
                "aliases": {"g": self.traversal_source}
            }
        }
        return DriverRemoteConnection._finalize_message(message)

    def _get_sideEffect_keys_message(self, request_id):
        message = {
            "requestId": {
                "@type": "g:UUID",
                "@value": str(uuid.uuid4())
            },
            "op": "keys",
            "processor": "traversal",
            "args": {
                "sideEffect": {
                    "@type": "g:UUID",
                    "@value": request_id
                }
            }
        }
        return DriverRemoteConnection._finalize_message(message)

    def _get_sideEffect_value_message(self, request_id, key):
        message = {
            "requestId": {
                "@type": "g:UUID",
                "@value": str(uuid.uuid4())
            },
            "op": "gather",
            "processor": "traversal",
            "args": {
                "sideEffect": {
                    "@type": "g:UUID",
                    "@value": request_id
                },
                "sideEffectKey": key,
                "aliases": {"g": self.traversal_source}
            }
        }
        return DriverRemoteConnection._finalize_message(message)

    @staticmethod
    def _finalize_message(message):
        message = json.dumps(message)
        mime_type = b"application/vnd.gremlin-v2.0+json"
        mime_len = b"\x21"
        return b"".join([mime_len, mime_type, message.encode("utf-8")])


class Response:
    def __init__(self, ws, username, password):
        self._ws = ws
        self._closed = False
        self._username = username
        self._password = password

    def _authenticate(self, username, password):
        auth = b"".join([b"\x00", username.encode("utf-8"),
                         b"\x00", password.encode("utf-8")])
        message = {
            "requestId": {
                "@type": "g:UUID",
                "@value": str(uuid.uuid4())
            },
            "op": "authentication",
            "processor": "traversal",
            "args": {
                "sasl": base64.b64encode(auth).decode()
            }
        }
        self._ws.send_message(DriverRemoteConnection._finalize_message(message), binary=True)

    @gen.coroutine
    def receive(self):
        if self._closed:
            return
        data = yield self._ws.read_message()
        message = json.loads(data)
        status_code = message["status"]["code"]
        data = message["result"]["data"]
        msg = message["status"]["message"]
        meta = message["result"]["meta"]
        aggregateTo = "list" if "aggregateTo" not in meta else meta["aggregateTo"]

        if status_code == 407:
            self._authenticate(self._username, self._password)
            yield self.receive()
        elif status_code == 204:
            self._closed = True
            return
        elif status_code in [200, 206]:
            results = []
            for item in data:
                results.append(GraphSONReader._objectify(item))
            if status_code == 200:
                self._closed = True
            raise gen.Return((aggregateTo, results))
        else:
            self._closed = True
            raise GremlinServerError(
                "{0}: {1}".format(status_code, msg))

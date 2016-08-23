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

from .remote_connection import RemoteConnection
from .remote_connection import RemoteTraversal
from ..process.graphson import GraphSONWriter
from ..process.traversal import Traverser


class GremlinServerError(Exception):
    pass


def parse_traverser(result):
    return Traverser(result["@value"], result["@value"]["bulk"]["@value"])


class DriverRemoteConnection(RemoteConnection):
    """Remote connection to the Gremlin Server"""

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
        traversers = self._loop.run_sync(lambda: self.submit_bytecode(
            bytecode, request_id))
        return RemoteTraversal(iter(traversers),
                               SideEffectManager(self, request_id))

    @gen.coroutine
    def submit_bytecode(self, bytecode, request_id):
        """
        Submit bytecode to Gremlin Server

        :param str gremlin: Gremlin script to submit to server.
        :param str lang: Language of scripts submitted to the server.
            "gremlin-groovy" by default
        :param str op: Gremlin Server op argument. "eval" by default.
        :param str processor: Gremlin Server processor argument. "" by default.

        :returns: :py:class:`Response` object
        """
        print(request_id)
        message = self._get_bytecode_message(bytecode, request_id)
        traversers = yield self._execute_message(message, parse_traverser)
        raise gen.Return(traversers)

    @gen.coroutine
    def submit_keys(self, request_id):
        message = self._get_keys_message(request_id)
        resp_parser = lambda result: result
        keys = yield self._execute_message(message, resp_parser)
        raise gen.Return(keys)

    @gen.coroutine
    def submit_gather(self, request_id, key):
        message = self._get_gather_message(request_id, key)
        side_effects = yield self._execute_message(message, parse_traverser)
        raise gen.Return(side_effects)

    @gen.coroutine
    def _execute_message(self, message, resp_parser):
        if self._ws.protocol is None:
            self._ws = yield websocket.websocket_connect(self.url)
        self._ws.write_message(message, binary=True)
        resp = Response(self._ws, self._username, self._password, resp_parser)
        results = []
        while True:
            msg = yield resp.receive()
            if msg is None:
                break
            results += msg
        raise gen.Return(results)

    def close(self):
        """Close underlying connection and mark as closed."""
        self._ws.close()

    def _get_bytecode_message(self, bytecode, request_id):
        message = {
            "requestId": {
                "@type": "gremlin:uuid",
                "@value": request_id
            },
            "op": "bytecode",
            "processor": "traversal",
            "args": {
                "gremlin": GraphSONWriter.writeObject(bytecode),
                "aliases": {"g": self.traversal_source}
            }
        }
        message = self._finalize_message(message)
        return message

    def _get_keys_message(self, request_id):
        message = {
            "requestId": {
                "@type": "gremlin:uuid",
                "@value": str(uuid.uuid4())
            },
            "op": "keys",
            "processor": "traversal",
            "args": {
        		"sideEffect": {
        			"@type": "gremlin:uuid",
        			"@value": request_id
        		}
            }
        }
        message = self._finalize_message(message)
        return message

    def _get_gather_message(self, request_id, key):
        message = {
            "requestId": {
                "@type": "gremlin:uuid",
                "@value": str(uuid.uuid4())
            },
            "op": "gather",
            "processor": "traversal",
            "args": {
        		"sideEffect": {
        			"@type": "gremlin:uuid",
        			"@value": request_id
        		},
                "sideEffectKey": key,
                "aliases": {"g": self.traversal_source}
            }
        }
        message = self._finalize_message(message)
        return message

    def _authenticate(self, username, password, processor):
        auth = b"".join([b"\x00", username.encode("utf-8"),
                         b"\x00", password.encode("utf-8")])
        message = {
            "requestId": str(uuid.uuid4()),
            "op": "authentication",
            "processor": "",
            "args": {
                "sasl": base64.b64encode(auth).decode()
            }
        }
        message = self._finalize_message(message)
        self._ws.send_message(message, binary=True)

    def _finalize_message(self, message):
        message = json.dumps(message)
        return self._set_message_header(message)

    @staticmethod
    def _set_message_header(message):
        mime_type = b"application/vnd.gremlin-v2.0+json"
        mime_len = b"\x21"
        return b"".join([mime_len, mime_type, message.encode("utf-8")])


class Response:
    def __init__(self, ws, username, password, resp_parser):
        self._ws = ws
        self._closed = False
        self._username = username
        self._password = password
        self._resp_parser = resp_parser

    @gen.coroutine
    def receive(self):
        if self._closed:
            return
        data = yield self._ws.read_message()
        message = json.loads(data)
        status_code = message["status"]["code"]
        data = message["result"]["data"]
        msg = message["status"]["message"]
        if status_code == 407:
            self._authenticate(self._username, self._password, self._processor)
            traversers = yield self.receive()
        elif status_code == 204:
            self._closed = True
            return
        elif status_code in [200, 206]:
            results = []
            for result in data:
                results.append(self._resp_parser(result))
            if status_code == 200:
                self._closed = True
        else:
            self._closed = True
            raise GremlinServerError(
                "{0}: {1}".format(status_code, msg))
        raise gen.Return(results)


class SideEffectManager(object):

    def __init__(self, remote_connection, request_id):
        self._remote_connection = remote_connection
        self._request_id = request_id
        self._loop = self._remote_connection._loop

    def keys(self):
        keys = self._loop.run_sync(
            lambda: self._remote_connection.submit_keys(self._request_id))
        return keys

    def get(self, key):
        side_effects = self._loop.run_sync(
            lambda: self._remote_connection.submit_gather(self._request_id, key)
        )
        return side_effects

'''
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
'''
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


class DriverRemoteConnection(RemoteConnection):
    """Remote connection to the Gremlin Server"""

    def __init__(self, url, traversal_source, loop=None, username='', password=''):
        super(DriverRemoteConnection, self).__init__(url, traversal_source)
        if loop is None:
            self._loop = ioloop.IOLoop.current()
        self._ws = self._loop.run_sync(lambda: websocket.websocket_connect(self.url))
        self._username = username
        self._password = password

    def submit(self,
               bytecode,
               op="bytecode",
               processor="traversal",
               session=None):
        traversers = self._loop.run_sync(lambda: self._submit(bytecode, op, processor, session))
        return RemoteTraversal(iter(traversers), {})

    @gen.coroutine
    def _submit(self, bytecode, op, processor, session):
        """
        Submit bytecode to Gremlin Server

        :param str gremlin: Gremlin script to submit to server.
        :param str lang: Language of scripts submitted to the server.
            "gremlin-groovy" by default
        :param str op: Gremlin Server op argument. "eval" by default.
        :param str processor: Gremlin Server processor argument. "" by default.
        :param str session: Session id (optional). Typically a uuid

        :returns: :py:class:`Response` object
        """
        request_id = str(uuid.uuid4())
        message = self._prepare_message(bytecode, op, processor, session, request_id)
        if self._ws.protocol is None:
            self._ws = yield websocket.websocket_connect(self.url)
        self._ws.write_message(message, binary=True)
        resp = Response(self._ws, processor, session, self._username, self._password)
        traversers = []
        while True:
            msg = yield resp.receive()
            if msg is None:
                break
            traversers += msg
        raise gen.Return(traversers)

    def close(self):
        """Close underlying connection and mark as closed."""
        self._ws.close()

    def _prepare_message(self, bytecode, op, processor, session, request_id):
        message = {
            "requestId": request_id,
            "op": op,
            "processor": processor,
            "args": {
                "gremlin": GraphSONWriter.writeObject(bytecode),
                "aliases": {'g': self.traversal_source}
            }
        }
        message = self._finalize_message(message, processor, session)
        return message

    def _authenticate(self, username, password, processor, session):
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
        message = self._finalize_message(message, processor, session)
        self._ws.send_message(message, binary=True)

    def _finalize_message(self, message, processor, session):
        if processor == "session":
            if session is None:
                raise RuntimeError("session processor requires a session id")
            else:
                message["args"].update({"session": session})
        message = json.dumps(message)
        return self._set_message_header(message, "application/json")

    @staticmethod
    def _set_message_header(message, mime_type):
        if mime_type == "application/json":
            mime_len = b"\x10"
            mime_type = b"application/json"
        else:
            raise ValueError("Unknown mime type.")
        return b"".join([mime_len, mime_type, message.encode("utf-8")])


class Response:
    def __init__(self, ws, processor, session, username, password):
        self._ws = ws
        self._closed = False
        self._processor = processor
        self._session = session
        self._username = username
        self._password = password

    @gen.coroutine
    def receive(self):
        if self._closed:
            return
        data = yield self._ws.read_message()
        message = json.loads(data)
        status_code = message['status']['code']
        data = message["result"]["data"]
        msg = message["status"]["message"]
        if status_code == 407:
            self._authenticate(self._username, self._password, self._processor,
                               self._session)
            traversers = yield self.receive()
        elif status_code == 204:
            self._closed = True
            return
        elif status_code in [200, 206]:
            traversers = []
            for result in data:
                traversers.append(Traverser(result['value'], result['bulk']))
            if status_code == 200:
                self._closed = True
        else:
            self._closed = True
            raise GremlinServerError(
                "{0}: {1}".format(status_code, msg))
        raise gen.Return(traversers)

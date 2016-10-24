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
import base64
import json
import uuid
from tornado import gen
from tornado import ioloop
from tornado import websocket

from gremlin_python.structure.io.graphson import GraphSONReader, GraphSONWriter
from .remote_connection import RemoteConnection
from .remote_connection import RemoteTraversal
from .remote_connection import RemoteTraversalSideEffects


class GremlinServerError(Exception):
    pass


class DriverRemoteConnection(RemoteConnection):
    def __init__(self, url, traversal_source, username="", password="", loop=None, graphson_reader=None, graphson_writer=None):
        super(DriverRemoteConnection, self).__init__(url, traversal_source)
        self._url = url
        self._username = username
        self._password = password
        if loop is None: self._loop = ioloop.IOLoop.current()
        self._websocket = self._loop.run_sync(lambda: websocket.websocket_connect(self.url))
        self._graphson_reader = graphson_reader or GraphSONReader()
        self._graphson_writer = graphson_writer or GraphSONWriter()

    def submit(self, bytecode):
        '''
        :param bytecode: the bytecode of a traversal to submit to the RemoteConnection
        :return: a RemoteTraversal with RemoteTraversalSideEffects
        '''
        request_id = str(uuid.uuid4())
        traversers = self._loop.run_sync(lambda: self.submit_traversal_bytecode(request_id, bytecode))
        side_effect_keys = lambda: self._loop.run_sync(lambda: self.submit_sideEffect_keys(request_id))
        side_effect_value = lambda key: self._loop.run_sync(lambda: self.submit_sideEffect_value(request_id, key))
        side_effect_close = lambda: self._loop.run_sync(lambda: self.submit_sideEffect_close(request_id))
        return RemoteTraversal(iter(traversers), RemoteTraversalSideEffects(side_effect_keys, side_effect_value, side_effect_close))

    @gen.coroutine
    def submit_traversal_bytecode(self, request_id, bytecode):
        message = {
            "requestId": {
                "@type": "g:UUID",
                "@value": request_id
            },
            "op": "bytecode",
            "processor": "traversal",
            "args": {
                "gremlin": self._graphson_writer.writeObject(bytecode),
                "aliases": {"g": self.traversal_source}
            }
        }
        traversers = yield self._execute_message(message)
        raise gen.Return(traversers)

    @gen.coroutine
    def submit_sideEffect_keys(self, request_id):
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
        keys = yield self._execute_message(message)
        raise gen.Return(set(keys))

    @gen.coroutine
    def submit_sideEffect_value(self, request_id, key):
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
        try:
            value = yield self._execute_message(message)
        except:
            raise KeyError(key)
        raise gen.Return(value)

    @gen.coroutine
    def submit_sideEffect_close(self, request_id):
        message = {
            "requestId": {
                "@type": "g:UUID",
                "@value": str(uuid.uuid4())
            },
            "op": "close",
            "processor": "traversal",
            "args": {
                "sideEffect": {
                    "@type": "g:UUID",
                    "@value": request_id
                }
            }
        }
        result = yield self._execute_message(message)
        raise gen.Return(result)

    @gen.coroutine
    def _execute_message(self, send_message):
        send_message = b"".join([b"\x21",
                                 b"application/vnd.gremlin-v2.0+json",
                                 json.dumps(send_message, separators=(',', ':')).encode("utf-8")])
        if self._websocket.protocol is None:
            self._websocket = yield websocket.websocket_connect(self.url)
        self._websocket.write_message(send_message, binary=True)
        response = Response(self._websocket, self._username, self._password, self._graphson_reader)
        results = None
        while True:
            recv_message = yield response.receive()
            if recv_message is None:
                break
            aggregateTo = recv_message[0]
            # on first message, get the right result data structure
            if None == results:
                if "list" == aggregateTo:
                    results = []
                elif "set" == aggregateTo:
                    results = set()
                elif aggregateTo in ["map", "bulkset"]:
                    results = {}
                elif "none" == aggregateTo:
                    results = None
                else:
                    results = []

            # if there is no update to a structure, then the item is the result
            if results is None:
                results = recv_message[1][0]
            # updating a map is different than a list or a set
            elif isinstance(results, dict):
                if "map" == aggregateTo:
                    for item in recv_message[1]:
                        results.update(item)
                else:
                    for item in recv_message[1]:
                        results[item.object] = item.bulk
            # flat add list to result list
            else:
                results += recv_message[1]
        raise gen.Return([] if None == results else results)

    def close(self):
        self._websocket.close()


class Response:
    def __init__(self, websocket, username, password, graphson_reader):
        self._websocket = websocket
        self._username = username
        self._password = password
        self._closed = False
        self._graphson_reader = graphson_reader

    @gen.coroutine
    def receive(self):
        if self._closed:
            return
        recv_message = yield self._websocket.read_message()
        recv_message = json.loads(recv_message.decode('utf-8'))
        status_code = recv_message["status"]["code"]
        aggregateTo = recv_message["result"]["meta"].get("aggregateTo", "list")

        # authentification required then
        if status_code == 407:
            self._websocket.write_message(
                b"".join([b"\x21",
                          b"application/vnd.gremlin-v2.0+json",
                          json.dumps({
                              "requestId": {
                                  "@type": "g:UUID",
                                  "@value": str(uuid.uuid4())
                              },
                              "op": "authentication",
                              "processor": "traversal",
                              "args": {
                                  "sasl": base64.b64encode(
                                      b"".join([b"\x00", self._username.encode("utf-8"),
                                                b"\x00", self._password.encode("utf-8")])).decode()
                              }
                          }, separators=(',', ':')).encode("utf-8")]), binary=True)
            results = yield self.receive()
            raise gen.Return(results)
        elif status_code == 204:
            self._closed = True
            return
        elif status_code in [200, 206]:
            results = []
            for item in recv_message["result"]["data"]:
                results.append(self._graphson_reader.toObject(item))
            if status_code == 200:
                self._closed = True
            raise gen.Return((aggregateTo, results))
        else:
            self._closed = True
            raise GremlinServerError(
                "{0}: {1}".format(status_code, recv_message["status"]["message"]))

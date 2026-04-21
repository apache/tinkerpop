# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import queue
from concurrent.futures import Future

from gremlin_python.driver import resultset, useragent
from gremlin_python.driver.aiohttp.transport import AiohttpHTTPTransport
from gremlin_python.structure.io.graphbinaryV4 import GraphBinaryReader, DataType, int32_unpack
from gremlin_python.structure.io.util import Marker

__author__ = 'David M. Brown (davebshow@gmail.com)'


class GremlinServerError(Exception):
    def __init__(self, status):
        super(GremlinServerError, self).__init__('{0}: {1}'.format(status['code'], status['message']))
        self.status_code = status['code']
        self.status_message = status['message']
        self.status_exception = status['exception']


class Connection:

    def __init__(self, url, traversal_source,
                 executor, pool, request_serializer=None,
                 response_serializer=None, auth=None, interceptors=None,
                 headers=None, enable_user_agent_on_connect=True,
                 bulk_results=False, **transport_kwargs):
        if callable(interceptors):
            interceptors = [interceptors]
        elif not (isinstance(interceptors, tuple)
                  or isinstance(interceptors, list)
                  or interceptors is None):
            raise TypeError("interceptors must be a callable, tuple, list or None")

        self._url = url
        self._headers = headers
        self._traversal_source = traversal_source
        self._transport_kwargs = transport_kwargs
        self._executor = executor
        self._transport = None
        self._pool = pool
        self._result_set = None
        self._inited = False
        self._request_serializer = request_serializer
        self._response_serializer = response_serializer
        self._auth = auth
        self._interceptors = interceptors
        self._enable_user_agent_on_connect = enable_user_agent_on_connect
        if self._enable_user_agent_on_connect:
            self.__add_header(useragent.userAgentHeader, useragent.userAgent)
        self._bulk_results = bulk_results
        if self._bulk_results:
            self.__add_header("bulkResults", "true")

    def connect(self):
        if self._transport:
            self._transport.close()
        self._transport = AiohttpHTTPTransport(**self._transport_kwargs)
        self._transport.connect(self._url, self._headers)
        self._inited = True

    def close(self):
        if self._inited:
            self._transport.close()

    def _write_request(self, request_message):
        accept = str(self._response_serializer.version, encoding='utf-8')
        message = {
            'headers': {'accept': accept},
            'payload': self._request_serializer.serialize_message(request_message)
                if self._request_serializer is not None else request_message,
            'auth': self._auth
        }
        if self._request_serializer is not None:
            content_type = str(self._request_serializer.version, encoding='utf-8')
            message['headers']['content-type'] = content_type
        for interceptor in self._interceptors or []:
            message = interceptor(message)
        self._transport.write(message)

    def write(self, request_message):
        if not self._inited:
            self.connect()
        self._result_set = resultset.ResultSet(queue.Queue())
        # Create write task
        future = Future()
        future_write = self._executor.submit(
            self._write_request, request_message)

        def cb(f):
            try:
                f.result()
            except Exception as e:
                future.set_exception(e)
                self._pool.put_nowait(self)
            else:
                # Start receive task
                done = self._executor.submit(self._receive)
                self._result_set.done = done
                future.set_result(self._result_set)

        future_write.add_done_callback(cb)
        return future

    def _receive(self):
        try:
            # Check for non-GraphBinary error responses
            status = getattr(self._transport, 'status_code', None)
            if status is not None and status >= 400:
                content_type = getattr(self._transport, 'content_type', '')
                if 'graphbinary' not in content_type:
                    body = self._transport.read_body().decode('utf-8', errors='replace')
                    raise GremlinServerError({
                        'code': status,
                        'message': body,
                        'exception': ''
                    })

            # 204 No Content
            if status == 204:
                return

            stream = self._transport.get_stream()
            reader = GraphBinaryReader()

            # Read GB response header
            stream.read(1)  # version byte
            flags = stream.read(1)[0]
            bulked = flags == 0x01

            # Deserialize results one at a time into the ResultSet
            while True:
                obj = reader.to_object(stream)
                if obj == Marker.end_of_stream():
                    break
                if bulked:
                    bulk = reader.to_object(stream)
                    for _ in range(bulk):
                        self._result_set.stream.put_nowait(obj)
                else:
                    self._result_set.stream.put_nowait(obj)

            # Read status after EndOfStream
            status_code = int32_unpack(stream.read(4))
            msg_is_null = stream.read(1)[0] == 0x01
            status_message = '' if msg_is_null else reader.to_object(stream, DataType.string, False)
            exc_is_null = stream.read(1)[0] == 0x01
            status_exception = '' if exc_is_null else reader.to_object(stream, DataType.string, False)

            if status_code not in (0, 200, 204):
                raise GremlinServerError({
                    'code': status_code,
                    'message': status_message,
                    'exception': status_exception
                })
        finally:
            self._pool.put_nowait(self)

    def __add_header(self, key, value):
        if self._headers is None:
            self._headers = dict()
        # Headers may be a list of pairs
        if isinstance(self._headers, list):
            for pair in self._headers:
                if pair[0] == key:
                    self._headers.remove(pair)
            self._headers.append((key, value))
        else:
            self._headers[key] = value

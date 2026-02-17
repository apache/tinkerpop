# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import uuid
import queue
from concurrent.futures import Future

from gremlin_python.driver import resultset, useragent

__author__ = 'David M. Brown (davebshow@gmail.com)'


class Connection:

    def __init__(self, url, traversal_source, protocol, transport_factory,
                 executor, pool, headers=None, enable_user_agent_on_connect=True):
        self._url = url
        self._headers = headers
        self._traversal_source = traversal_source
        self._protocol = protocol
        self._transport_factory = transport_factory
        self._executor = executor
        self._transport = None
        self._pool = pool
        self._results = {}
        self._inited = False
        self._enable_user_agent_on_connect = enable_user_agent_on_connect
        if self._enable_user_agent_on_connect:
            self.__add_header(useragent.userAgentHeader, useragent.userAgent)

    def connect(self):
        if self._transport:
            self._transport.close()
        self._transport = self._transport_factory()
        self._transport.connect(self._url, self._headers)
        self._protocol.connection_made(self._transport)
        self._inited = True

    def close(self):
        if self._inited:
            self._transport.close()

    def write(self, request_message):
        if not self._inited:
            self.connect()
        if request_message.args.get("requestId"):
            request_id = str(request_message.args.get("requestId"))
            uuid.UUID(request_id)  # Checks for proper UUID or else server will return an error.
        else:
            request_id = str(uuid.uuid4())
        result_set = resultset.ResultSet(queue.Queue(), request_id)
        self._results[request_id] = result_set
        # Create write task
        future = Future()
        future_write = self._executor.submit(
            self._protocol.write, request_id, request_message)

        def cb(f):
            try:
                f.result()
            except Exception as e:
                future.set_exception(e)
                self._pool.put_nowait(self)
            else:
                # Start receive task
                done = self._executor.submit(self._receive)
                result_set.done = done
                future.set_result(result_set)

        future_write.add_done_callback(cb)
        return future

    def _receive(self):
        try:
            while True:
                data = self._transport.read()
                status_code = self._protocol.data_received(data, self._results)
                if status_code != 206:
                    break
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

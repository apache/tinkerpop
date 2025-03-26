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
import queue
from concurrent.futures import Future

from gremlin_python.driver import resultset, useragent

__author__ = 'David M. Brown (davebshow@gmail.com)'


class Connection:

    def __init__(self, url, traversal_source, protocol, transport_factory,
                 executor, pool, headers=None, enable_user_agent_on_connect=True,
                 bulk_results=False):
        self._url = url
        self._headers = headers
        self._traversal_source = traversal_source
        self._protocol = protocol
        self._transport_factory = transport_factory
        self._executor = executor
        self._transport = None
        self._pool = pool
        self._result_set = None
        self._inited = False
        self._enable_user_agent_on_connect = enable_user_agent_on_connect
        if self._enable_user_agent_on_connect:
            self.__add_header(useragent.userAgentHeader, useragent.userAgent)
        self._bulk_results = bulk_results
        if self._bulk_results:
            self.__add_header("bulkResults", "true")

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
        self._result_set = resultset.ResultSet(queue.Queue())
        # Create write task
        future = Future()
        future_write = self._executor.submit(
            self._protocol.write, request_message)

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
            '''
            GraphSON does not support streaming deserialization, we are aggregating data and bypassing streamed
             deserialization while GraphSON is enabled for testing. Remove after GraphSON is removed.
            '''
            self._protocol.data_received_aggregate(self._transport.read(), self._result_set)
            # re-enable streaming after graphSON removal
            # self._transport.read(self.stream_chunk)
        finally:
            self._pool.put_nowait(self)

    def stream_chunk(self, chunk_data, read_completed=None, http_req_resp=None):
        self._protocol.data_received(chunk_data, self._result_set, read_completed, http_req_resp)

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

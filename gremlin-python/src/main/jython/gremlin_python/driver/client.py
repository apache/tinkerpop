#
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
#
from concurrent.futures import ThreadPoolExecutor

from six.moves import queue

from gremlin_python.driver import connection, protocol, request, serializer
from gremlin_python.process import traversal

# This is until concurrent.futures backport 3.1.0 release
try:
    from multiprocessing import cpu_count
except ImportError:
    # some platforms don't have multiprocessing
    def cpu_count():
        return None

__author__ = 'David M. Brown (davebshow@gmail.com)'


class Client:

    def __init__(self, url, traversal_source, protocol_factory=None,
                 transport_factory=None, pool_size=None, max_workers=None,
                 message_serializer=None, username="", password="",
                 headers=None, session=""):
        self._url = url
        self._headers = headers
        self._traversal_source = traversal_source
        if message_serializer is None:
            message_serializer = serializer.GraphSONSerializersV3d0()
        self._message_serializer = message_serializer
        self._username = username
        self._password = password
        self._session = session
        self._sessionEnabled = (session != "")
        if transport_factory is None:
            try:
                from gremlin_python.driver.tornado.transport import (
                    TornadoTransport)
            except ImportError:
                raise Exception("Please install Tornado or pass"
                                "custom transport factory")
            else:
                transport_factory = lambda: TornadoTransport()
        self._transport_factory = transport_factory
        if protocol_factory is None:
            protocol_factory = lambda: protocol.GremlinServerWSProtocol(
                self._message_serializer,
                username=self._username,
                password=self._password)
        self._protocol_factory = protocol_factory
        if self._sessionEnabled:
            if pool_size is None:
                pool_size = 1
            elif pool_size != 1:
                raise Exception("PoolSize must be 1 on session mode!")
        if pool_size is None:
            pool_size = 4
        self._pool_size = pool_size
        # This is until concurrent.futures backport 3.1.0 release
        if max_workers is None:
            # If your application is overlapping Gremlin I/O on multiple threads
            # consider passing kwarg max_workers = (cpu_count() or 1) * 5
            max_workers = pool_size
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        # Threadsafe queue
        self._pool = queue.Queue()
        self._fill_pool()

    @property
    def available_pool_size(self):
        return self._pool.qsize()

    @property
    def executor(self):
        return self._executor

    @property
    def traversal_source(self):
        return self._traversal_source

    def _fill_pool(self):
        for i in range(self._pool_size):
            conn = self._get_connection()
            self._pool.put_nowait(conn)

    def close(self):
        if self._sessionEnabled:
            self._close_session()
        while not self._pool.empty():
            conn = self._pool.get(True)
            conn.close()
        self._executor.shutdown()

    def _close_session(self):
        message = request.RequestMessage(
            processor='session', op='close',
            args={'session': self._session})
        conn = self._pool.get(True)
        return conn.write(message).result()

    def _get_connection(self):
        protocol = self._protocol_factory()
        return connection.Connection(
            self._url, self._traversal_source, protocol,
            self._transport_factory, self._executor, self._pool,
            headers=self._headers)

    def submit(self, message, bindings=None, request_options=None):
        return self.submitAsync(message, bindings=bindings, request_options=request_options).result()

    def submitAsync(self, message, bindings=None, request_options=None):
        if isinstance(message, traversal.Bytecode):
            message = request.RequestMessage(
                processor='traversal', op='bytecode',
                args={'gremlin': message,
                      'aliases': {'g': self._traversal_source}})
        elif isinstance(message, str):
            message = request.RequestMessage(
                processor='', op='eval',
                args={'gremlin': message,
                      'aliases': {'g': self._traversal_source}})
            if bindings:
                message.args.update({'bindings': bindings})
            if self._sessionEnabled:
                message = message._replace(processor='session')
                message.args.update({'session': self._session})
        conn = self._pool.get(True)
        if request_options:
            message.args.update(request_options)
        return conn.write(message)

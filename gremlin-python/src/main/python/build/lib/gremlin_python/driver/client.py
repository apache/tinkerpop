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
import logging
import warnings
import queue
import re
from concurrent.futures import ThreadPoolExecutor

from gremlin_python.driver import connection, protocol, request, serializer
from gremlin_python.process import traversal

log = logging.getLogger("gremlinpython")

# This is until concurrent.futures backport 3.1.0 release
try:
    from multiprocessing import cpu_count
except ImportError:
    # some platforms don't have multiprocessing
    def cpu_count():
        return None

__author__ = 'David M. Brown (davebshow@gmail.com), Lyndon Bauto (lyndonb@bitquilltech.com)'


class Client:

    def __init__(self, url, traversal_source, protocol_factory=None,
                 transport_factory=None, pool_size=None, max_workers=None,
                 message_serializer=None, username="", password="",
                 kerberized_service="", headers=None, session=None,
                 enable_user_agent_on_connect=True, enable_compression=False,
                 **transport_kwargs):
        log.info("Creating Client with url '%s'", url)

        # check via url that we are using http protocol
        self._use_http = re.search('^http', url)

        self._closed = False
        self._url = url
        self._headers = headers
        self._enable_user_agent_on_connect = enable_user_agent_on_connect
        self._traversal_source = traversal_source
        self._enable_compression = enable_compression
        if not self._use_http and "max_content_length" not in transport_kwargs:
            transport_kwargs["max_content_length"] = 10 * 1024 * 1024
        if message_serializer is None:
            message_serializer = serializer.GraphBinarySerializersV1()

        self._message_serializer = message_serializer
        self._username = username
        self._password = password
        self._session = session
        self._session_enabled = (session is not None and session != "")
        if transport_factory is None:
            try:
                from gremlin_python.driver.aiohttp.transport import (
                    AiohttpTransport, AiohttpHTTPTransport)
            except ImportError:
                raise Exception("Please install AIOHTTP or pass "
                                "custom transport factory")
            else:
                def transport_factory():
                    if self._use_http:
                        return AiohttpHTTPTransport(**transport_kwargs)
                    else:
                        return AiohttpTransport(enable_compression=enable_compression, **transport_kwargs)
        self._transport_factory = transport_factory
        if protocol_factory is None:
            def protocol_factory():
                if self._use_http:
                    return protocol.GremlinServerHTTPProtocol(
                        self._message_serializer,
                        username=self._username,
                        password=self._password)
                else:
                    return protocol.GremlinServerWSProtocol(
                        self._message_serializer,
                        username=self._username,
                        password=self._password,
                        kerberized_service=kerberized_service,
                        max_content_length=transport_kwargs["max_content_length"])
        self._protocol_factory = protocol_factory
        if self._session_enabled:
            if pool_size is None:
                pool_size = 1
            elif pool_size != 1:
                raise Exception("PoolSize must be 1 on session mode!")
        if pool_size is None:
            pool_size = 8
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

    def is_closed(self):
        return self._closed

    def close(self):
        # prevent the Client from being closed more than once. it raises errors if new jobby jobs
        # get submitted to the executor when it is shutdown
        if self._closed:
            return

        if self._session_enabled:
            self._close_session()
        log.info("Closing Client with url '%s'", self._url)
        while not self._pool.empty():
            conn = self._pool.get(True)
            conn.close()
        self._executor.shutdown()
        self._closed = True

    def _close_session(self):
        message = request.RequestMessage(
            processor='session', op='close',
            args={'session': str(self._session)})
        conn = self._pool.get(True)
        try:
            write_result_set = conn.write(message).result()
            return write_result_set.all().result()  # wait for _receive() to finish
        except protocol.GremlinServerError:
            pass

    def _get_connection(self):
        protocol = self._protocol_factory()
        return connection.Connection(
            self._url, self._traversal_source, protocol,
            self._transport_factory, self._executor, self._pool,
            headers=self._headers, enable_user_agent_on_connect=self._enable_user_agent_on_connect)

    def submit(self, message, bindings=None, request_options=None):
        return self.submit_async(message, bindings=bindings, request_options=request_options).result()

    def submitAsync(self, message, bindings=None, request_options=None):
        warnings.warn(
            "gremlin_python.driver.client.Client.submitAsync will be replaced by "
            "gremlin_python.driver.client.Client.submit_async.",
            DeprecationWarning)
        return self.submit_async(message, bindings, request_options)

    def submit_async(self, message, bindings=None, request_options=None):
        if self.is_closed():
            raise Exception("Client is closed")

        log.debug("message '%s'", str(message))
        args = {'gremlin': message, 'aliases': {'g': self._traversal_source}}
        processor = ''
        op = 'eval'
        if isinstance(message, traversal.Bytecode):
            op = 'bytecode'
            processor = 'traversal'

        if isinstance(message, str) and bindings:
            args['bindings'] = bindings

        if self._session_enabled:
            args['session'] = str(self._session)
            processor = 'session'

        if isinstance(message, traversal.Bytecode) or isinstance(message, str):
            log.debug("processor='%s', op='%s', args='%s'", str(processor), str(op), str(args))
            message = request.RequestMessage(processor=processor, op=op, args=args)

        conn = self._pool.get(True)
        if request_options:
            message.args.update(request_options)
        return conn.write(message)

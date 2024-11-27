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
from concurrent.futures import ThreadPoolExecutor

from gremlin_python.driver import connection, protocol, request, serializer

log = logging.getLogger("gremlinpython")

# This is until concurrent.futures backport 3.1.0 release
try:
    from multiprocessing import cpu_count
except ImportError:
    # some platforms don't have multiprocessing
    def cpu_count():
        return None

__author__ = 'David M. Brown (davebshow@gmail.com), Lyndon Bauto (lyndonb@bitquilltech.com)'


# TODO: remove session, update connection pooling, etc.
class Client:

    def __init__(self, url, traversal_source, protocol_factory=None,
                 transport_factory=None, pool_size=None, max_workers=None,
                 request_serializer=serializer.GraphBinarySerializersV4(),
                 response_serializer=None, interceptors=None, auth=None,
                 headers=None, enable_user_agent_on_connect=True,
                 bulk_results=False, **transport_kwargs):
        log.info("Creating Client with url '%s'", url)

        self._closed = False
        self._url = url
        self._headers = headers
        self._enable_user_agent_on_connect = enable_user_agent_on_connect
        self._bulk_results = bulk_results
        self._traversal_source = traversal_source
        if "max_content_length" not in transport_kwargs:
            transport_kwargs["max_content_length"] = 10 * 1024 * 1024
        if response_serializer is None:
            response_serializer = serializer.GraphBinarySerializersV4()

        self._auth = auth
        self._response_serializer = response_serializer

        if transport_factory is None:
            try:
                from gremlin_python.driver.aiohttp.transport import AiohttpHTTPTransport
            except ImportError:
                raise Exception("Please install AIOHTTP or pass "
                                "custom transport factory")
            else:
                def transport_factory():
                    if self._protocol_factory is None:
                        self._protocol_factory = protocol_factory
                    return AiohttpHTTPTransport(**transport_kwargs)
        self._transport_factory = transport_factory

        if protocol_factory is None:
            def protocol_factory():
                return protocol.GremlinServerHTTPProtocol(
                    request_serializer, response_serializer, auth=self._auth,
                    interceptors=interceptors)
        self._protocol_factory = protocol_factory

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
    
    def response_serializer(self):
        return self._response_serializer

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

        log.info("Closing Client with url '%s'", self._url)
        while not self._pool.empty():
            conn = self._pool.get(True)
            conn.close()
        self._executor.shutdown()
        self._closed = True

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
        fields = {'g': self._traversal_source}

        # TODO: bindings is now part of request_options, evaluate the need to keep it separate in python.
        #  Note this bindings parameter only applies to string script submissions
        if isinstance(message, str) and bindings:
            fields['bindings'] = bindings

        if isinstance(message, str):
            log.debug("fields='%s', gremlin='%s'", str(fields), str(message))
            message = request.RequestMessage(fields=fields, gremlin=message)

        conn = self._pool.get(True)
        if request_options:
            message.fields.update({token: request_options[token] for token in request.Tokens
                                   if token in request_options and token != 'bindings'})
            if 'bindings' in request_options:
                if 'bindings' in message.fields:
                    message.fields['bindings'].update(request_options['bindings'])
                else:
                    message.fields['bindings'] = request_options['bindings']
            if 'params' in request_options:
                if 'bindings' in message.fields:
                    message.fields['bindings'].update(request_options['params'])
                else:
                    message.fields['bindings'] = request_options['params']

        return conn.write(message)

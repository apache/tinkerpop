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
import asyncio
import queue
from concurrent.futures import Future

from aiohttp.client_exceptions import (
    ClientOSError,
    ClientPayloadError,
    ServerDisconnectedError,
)

from gremlin_python.driver import resultset, useragent
from gremlin_python.driver.aiohttp.transport import AiohttpHTTPTransport
from gremlin_python.driver.http_request import HttpRequest

__author__ = 'David M. Brown (davebshow@gmail.com)'

_TRANSPORT_ERRORS = (ClientOSError, ClientPayloadError, ServerDisconnectedError, asyncio.IncompleteReadError)
_CONNECTION_ERROR_MSG = (
    "Connection to server closed unexpectedly. "
    "Ensure that the server is still reachable and the connection has not been closed by the server or a network device."
)


class GremlinServerError(Exception):
    def __init__(self, status):
        super(GremlinServerError, self).__init__('{0}: {1}'.format(status['code'], status['message']))
        self.status_code = status['code']
        self.status_message = status['message']
        self.status_exception = status['exception']


class GremlinConnectionError(Exception):
    """Raised when a transport-level failure occurs communicating with the server."""
    pass


class Connection:

    def __init__(self, url, traversal_source,
                 executor, pool,
                 response_serializer=None, auth=None, interceptors=None,
                 enable_user_agent_on_connect=True,
                 bulk_results=False, **transport_kwargs):
        if callable(interceptors):
            interceptors = [interceptors]
        elif isinstance(interceptors, tuple):
            interceptors = list(interceptors)
        elif not (isinstance(interceptors, list) or interceptors is None):
            raise TypeError("interceptors must be a callable, tuple, list or None")

        # Auth is just an interceptor. As a convenience (and for discoverability), the auth
        # interceptor is appended to the end of the interceptor list so it runs last, after
        # any user interceptors have modified the request.
        if auth is not None:
            interceptors = (interceptors or []) + [auth]

        self._url = url
        # Custom request headers are set via interceptors. This internal dict
        # only carries connection-level headers managed by the driver itself
        # (user agent, bulkResults) and is established at construction time.
        self._headers = None
        self._traversal_source = traversal_source
        self._transport_kwargs = transport_kwargs
        self._executor = executor
        self._transport = None
        self._pool = pool
        self._result_set = None
        self._inited = False
        self._response_serializer = response_serializer
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

        headers = {'accept': accept}

        # Promote transactionId to HTTP header before interceptors run.
        # The field remains in the serialized body as well (dual transmission
        # per the HTTP transaction protocol specification).
        if hasattr(request_message, 'fields') and 'transactionId' in request_message.fields:
            headers['X-Transaction-Id'] = request_message.fields['transactionId']

        http_request = HttpRequest(
            method="POST",
            url=self._url,
            headers=headers,
            body=request_message
        )

        for interceptor in self._interceptors or []:
            interceptor(http_request)

        # Auto-serialize if no interceptor already did so
        http_request.serialize_body()

        # Build the transport message in the format the transport expects
        message = {
            'headers': http_request.headers,
            'payload': http_request.body
        }
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
            except _TRANSPORT_ERRORS as e:
                wrapped = GremlinConnectionError(_CONNECTION_ERROR_MSG)
                wrapped.__cause__ = e
                future.set_exception(wrapped)
                self._pool.put_nowait(self)
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
            # Surface error responses whose Content-Type doesn't match the
            # configured response serializer (e.g. plain-text 5xx).
            status = getattr(self._transport, 'status_code', None)
            response_content_type = str(self._response_serializer.version, encoding='utf-8')
            if status is not None and status >= 400:
                content_type = getattr(self._transport, 'content_type', '')
                if response_content_type not in content_type:
                    body = self._transport.read_body().decode('utf-8', errors='replace')
                    raise GremlinServerError({
                        'code': status,
                        'message': body,
                        'exception': ''
                    })

            # 204 No Content
            if status == 204:
                return

            # Stream items through the configured response serializer; it
            # raises GremlinServerError if the response ends with a non-success status.
            stream = self._transport.get_stream()
            for obj in self._response_serializer.deserialize_response_stream(stream):
                self._result_set.stream.put_nowait(obj)
        except _TRANSPORT_ERRORS as err:
            # Evict the dead connection from aiohttp's internal pool, ensuring subsequent
            # requests get a fresh connection.
            self._transport.evict_response()
            msg = 'Server returned an empty response body' if isinstance(err, asyncio.IncompleteReadError) and not err.partial else _CONNECTION_ERROR_MSG
            raise GremlinConnectionError(msg) from err
        except Exception:
            self._transport.evict_response()
            raise
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

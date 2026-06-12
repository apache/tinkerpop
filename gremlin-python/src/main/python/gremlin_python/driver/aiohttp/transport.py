#
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
#
import aiohttp
import asyncio
import sys

if sys.version_info >= (3, 11):
    import asyncio as async_timeout
else:
    import async_timeout

__author__ = 'Lyndon Bauto (lyndonb@bitquilltech.com)'


class AiohttpSyncStream:
    """Wraps aiohttp's async StreamReader as a synchronous file-like object.
    read(n) blocks until exactly n bytes are available from the HTTP response.

    Maintains an internal byte buffer that is refilled one HTTP chunk at a time
    so the deserializer's many small read(n) calls don't each pay the cost of a
    full asyncio event-loop turn."""

    # Max bytes pulled from the response per underlying read. Matches
    # aiohttp.StreamReader's default 64 KB limit, which is the per-connection
    # high-water mark, asking for more in one read() never returns more.
    _FILL_SIZE = 64 * 1024

    def __init__(self, response, loop, read_timeout):
        self._response = response
        self._loop = loop
        self._read_timeout = read_timeout
        self._buf = bytearray()
        self._pos = 0

    def read(self, n):
        if n <= 0:
            return b''
        while len(self._buf) - self._pos < n:
            data = self._read_chunk()
            if not data:
                partial = bytes(self._buf[self._pos:])
                self._buf.clear()
                self._pos = 0
                raise asyncio.IncompleteReadError(partial=partial, expected=n)
            self._buf.extend(data)
        end = self._pos + n
        out = bytes(self._buf[self._pos:end])
        self._pos = end
        # Reclaim memory once the buffer is fully drained
        if self._pos == len(self._buf):
            self._buf.clear()
            self._pos = 0
        return out

    def _read_chunk(self):
        async def _read():
            async with async_timeout.timeout(self._read_timeout):
                return await self._response.content.read(self._FILL_SIZE)
        return self._loop.run_until_complete(_read())


class AiohttpHTTPTransport:
    nest_asyncio_applied = False

    def __init__(self, call_from_event_loop=None, read_timeout=None, write_timeout=None, **kwargs):
        if call_from_event_loop is not None and call_from_event_loop and not AiohttpHTTPTransport.nest_asyncio_applied:
            """ 
                The AiohttpTransport implementation uses the asyncio event loop. Because of this, it cannot be called 
                within an event loop without nest_asyncio. If the code is ever refactored so that it can be called 
                within an event loop this import and call can be removed. Without this, applications which use the 
                event loop to call gremlin-python (such as Jupyter) will not work.
            """
            import nest_asyncio
            nest_asyncio.apply()
            AiohttpHTTPTransport.nest_asyncio_applied = True

        # Start event loop and initialize client session and response to None
        self._loop = asyncio.new_event_loop()
        self._client_session = None
        self._http_req_resp = None
        self._enable_ssl = False
        self._url = None

        # Set all inner variables to parameters passed in.
        self._aiohttp_kwargs = kwargs
        self._write_timeout = write_timeout
        self._read_timeout = read_timeout
        # max_content_length is no longer enforced with streaming deserialization, but pop it
        # to prevent it from leaking to aiohttp as an unknown kwarg
        self._aiohttp_kwargs.pop("max_content_length", None)
        if "ssl_options" in self._aiohttp_kwargs:
            self._ssl_context = self._aiohttp_kwargs.pop("ssl_options")
            self._enable_ssl = True

    def __del__(self):
        # Close will only actually close if things are left open, so this is safe to call.
        # Clean up any connection resources and close the event loop.
        self.close()

    def connect(self, url, headers=None):
        self._url = url
        # Inner function to perform async connect.
        async def async_connect():
            # Start client session and use it to send all HTTP requests. Headers can be set here.
            if self._enable_ssl:
                # ssl context is established through tcp connector
                tcp_conn = aiohttp.TCPConnector(ssl_context=self._ssl_context)
                self._client_session = aiohttp.ClientSession(connector=tcp_conn,
                                                             headers=headers, loop=self._loop)
            else:
                self._client_session = aiohttp.ClientSession(headers=headers, loop=self._loop)

        # Execute the async connect synchronously.
        self._loop.run_until_complete(async_connect())

    def write(self, message):
        # Inner function to perform async write.
        async def async_write():
            async with async_timeout.timeout(self._write_timeout):
                self._http_req_resp = await self._client_session.post(url=self._url,
                                                                      data=message['payload'],
                                                                      headers=message['headers'],
                                                                      **self._aiohttp_kwargs)

        # Execute the async write synchronously.
        self._loop.run_until_complete(async_write())

    def get_stream(self):
        """Returns a synchronous file-like object for the HTTP response body."""
        return AiohttpSyncStream(self._http_req_resp, self._loop, self._read_timeout)

    @property
    def content_type(self):
        """Returns the Content-Type header of the HTTP response."""
        if self._http_req_resp is not None:
            return self._http_req_resp.headers.get('content-type', '')
        return ''

    @property
    def status_code(self):
        """Returns the HTTP status code of the response."""
        if self._http_req_resp is not None:
            return self._http_req_resp.status
        return None

    def read_body(self):
        """Read the entire HTTP response body as bytes."""
        async def _read():
            async with async_timeout.timeout(self._read_timeout):
                return await self._http_req_resp.read()
        return self._loop.run_until_complete(_read())

    def close(self):
        # Inner function to perform async close.
        async def async_close():
            if self._client_session is not None and not self._client_session.closed:
                await self._client_session.close()
                self._client_session = None

        # If the loop is not closed (connection hasn't already been closed)
        if not self._loop.is_closed():
            # Execute the async close synchronously.
            self._loop.run_until_complete(async_close())

            # Close the event loop.
            self._loop.close()

    @property
    def closed(self):
        # Connection is closed when client session is closed.
        return self._client_session.closed

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
import json

import aiohttp
import asyncio
import sys

if sys.version_info >= (3, 11):
    import asyncio as async_timeout
else:
    import async_timeout
from aiohttp import ClientPayloadError
from gremlin_python.driver.protocol import GremlinServerError
from gremlin_python.driver.transport import AbstractBaseTransport

__author__ = 'Lyndon Bauto (lyndonb@bitquilltech.com)'


class AiohttpHTTPTransport(AbstractBaseTransport):
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
        if "max_content_length" in self._aiohttp_kwargs:
            self._max_content_len = self._aiohttp_kwargs.pop("max_content_length")
        else:
            self._max_content_len = 10 * 1024 * 1024
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
            # To pass url into message for request authentication processing
            message.update({'url': self._url})
            if message['auth']:
                message['auth'](message)

            async with async_timeout.timeout(self._write_timeout):
                self._http_req_resp = await self._client_session.post(url=self._url,
                                                                      data=message['payload'],
                                                                      headers=message['headers'],
                                                                      **self._aiohttp_kwargs)

        # Execute the async write synchronously.
        self._loop.run_until_complete(async_write())

    def read(self, stream_chunk=None):
        if not stream_chunk:
            '''
            GraphSON does not support streaming deserialization, we are aggregating data and bypassing streamed
             deserialization while GraphSON is enabled for testing. Remove after GraphSON is removed.
            '''
            async def async_read():
                async with async_timeout.timeout(self._read_timeout):
                    data_buffer = b""
                    async for data, end_of_http_chunk in self._http_req_resp.content.iter_chunks():
                        try:
                            data_buffer += data
                        except ClientPayloadError:
                            # server disconnect during streaming will cause ClientPayLoadError from aiohttp
                            raise GremlinServerError({'code': 500,
                                                      'message': 'Server disconnected - please try to reconnect',
                                                      'exception': ClientPayloadError})
                    if self._max_content_len and len(
                            data_buffer) > self._max_content_len:
                        raise Exception(f'Response size {len(data_buffer)} exceeds limit {self._max_content_len} bytes')
                    if self._http_req_resp.headers.get('content-type') == 'application/json':
                        message = json.loads(data_buffer.decode('utf-8'))
                        err = message.get('message')
                        raise Exception(f'Server disconnected with error message: "{err}" - please try to reconnect')
                    return data_buffer
            return self._loop.run_until_complete(async_read())
            # raise Exception('missing handling of streamed responses to protocol')

        # Inner function to perform async read.
        async def async_read():
            # TODO: potentially refactor to just use streaming and remove transport/protocol
            async with async_timeout.timeout(self._read_timeout):
                read_completed = False
                # aiohttp streaming may not iterate through one whole chunk if it's too large, need to buffer it
                data_buffer = b""
                async for data, end_of_http_chunk in self._http_req_resp.content.iter_chunks():
                    try:
                        data_buffer += data
                        if end_of_http_chunk:
                            if self._max_content_len and len(
                                    data_buffer) > self._max_content_len:
                                raise Exception(  # TODO: do we need proper exception class for this?
                                    f'Response size {len(data_buffer)} exceeds limit {self._max_content_len} bytes')
                            stream_chunk(data_buffer, read_completed, self._http_req_resp.ok)
                            data_buffer = b""
                    except ClientPayloadError:
                        # server disconnect during streaming will cause ClientPayLoadError from aiohttp
                        # TODO: double check during refactoring
                        raise GremlinServerError({'code': 500,
                                                  'message': 'Server disconnected - please try to reconnect',
                                                  'exception': ClientPayloadError})
                read_completed = True
                stream_chunk(data_buffer, read_completed, self._http_req_resp.ok)

        return self._loop.run_until_complete(async_read())

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

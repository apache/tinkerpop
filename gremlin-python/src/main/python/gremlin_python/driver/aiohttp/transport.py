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


import aiohttp
import asyncio
import async_timeout

from gremlin_python.driver.transport import AbstractBaseTransport

__author__ = 'Lyndon Bauto (lyndonb@bitquilltech.com)'


class AiohttpTransport(AbstractBaseTransport):

    # TODO: Default heartbeat value?
    # TODO: Default compression
    def __init__(self, read_timeout=None, write_timeout=None,
                 compression_options=None,
                 ssl_options=None,
                 heartbeat=None):
        if compression_options is None:
            compression_options = {'compression_level': 5, 'mem_level': 5}
        self._loop = asyncio.new_event_loop()
        self._ws = None
        self._read_timeout = read_timeout
        self._write_timeout = write_timeout
        self._compression_options = compression_options
        self._ssl_options = ssl_options
        self._heartbeat = heartbeat

    def connect(self, url, headers=None):
        self._loop.run_until_complete(self._async_connect(url, headers))

    def write(self, message):
        self._loop.run_until_complete(self._async_write(message))

    def read(self):
        return self._loop.run_until_complete(self._async_read())

    def close(self):
        if self._loop.is_running():
            # TODO: review and see if this is the correct way of closing
            # self._loop.run_until_complete(self._async_close())
            self._loop.close()

    def closed(self):
        return self._ws.closed

    async def _async_connect(self, url, headers=None):
        self._client = aiohttp.ClientSession()
        self._ws = await self._client.ws_connect(
            url, ssl=self._ssl_options, headers=headers, heartbeat=self._heartbeat)

    async def _async_write(self, message):
        async with async_timeout.timeout(self._write_timeout):
            await self._ws.send_bytes(message)

    async def _async_read(self):
        async with async_timeout.timeout(self._read_timeout):
            return (await self._ws.receive()).data

    async def _async_close(self):
        if not self._ws.closed:
            await self._ws.close()
        if not self._client.closed:
            await self._client.close()

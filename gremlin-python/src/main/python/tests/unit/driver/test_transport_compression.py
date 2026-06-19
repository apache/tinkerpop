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

"""Loopback round-trip tests for the aiohttp transport's compression handling.

These spin up a tiny in-process aiohttp server on an ephemeral port (no
external network) to verify the *actual* over-the-wire behavior that the
unit-level mocks cannot:

  - M1: the outgoing Accept-Encoding header the server receives for the
    'none' vs 'deflate' compression options.
  - M2: a real deflate-compressed response is transparently decompressed when
    read back through the streaming get_stream() path.
"""

import asyncio
import threading
import time
import zlib

import pytest

from gremlin_python.driver.aiohttp.transport import AiohttpHTTPTransport


class _LoopbackServer:
    """Runs an aiohttp app on its own event loop in a background thread.

    The handler records the Accept-Encoding header it received and replies with
    a deflate-compressed body (advertising Content-Encoding: deflate) so the
    client must decompress to recover the original payload.
    """

    def __init__(self, payload):
        self._payload = payload
        self.received_accept_encoding = "<unset>"
        self._loop = None
        self._runner = None
        self._thread = None
        self.port = None
        self._started = threading.Event()

    async def _handler(self, request):
        from aiohttp import web
        self.received_accept_encoding = request.headers.get('Accept-Encoding', None)
        body = zlib.compress(self._payload)
        return web.Response(body=body,
                            headers={'Content-Encoding': 'deflate',
                                     'Content-Type': 'application/octet-stream'})

    def _run(self):
        from aiohttp import web
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        async def _setup():
            app = web.Application()
            app.router.add_post('/', self._handler)
            self._runner = web.AppRunner(app)
            await self._runner.setup()
            site = web.TCPSite(self._runner, '127.0.0.1', 0)
            await site.start()
            self.port = site._server.sockets[0].getsockname()[1]
            self._started.set()

        self._loop.run_until_complete(_setup())
        self._loop.run_forever()

    def __enter__(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        assert self._started.wait(timeout=10), "loopback server failed to start"
        return self

    def __exit__(self, *exc):
        async def _cleanup():
            await self._runner.cleanup()
        fut = asyncio.run_coroutine_threadsafe(_cleanup(), self._loop)
        try:
            fut.result(timeout=10)
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=10)

    @property
    def url(self):
        return 'http://127.0.0.1:%d/' % self.port


def _read_all(stream):
    """Drain an AiohttpSyncStream, tolerating the IncompleteReadError raised at
    EOF (the deserializer normally stops on the end-of-stream marker before
    hitting EOF, but here we read raw bytes to completion)."""
    out = bytearray()
    while True:
        try:
            chunk = stream.read(4096)
        except asyncio.IncompleteReadError as e:
            out.extend(e.partial)
            break
        if not chunk:
            break
        out.extend(chunk)
    return bytes(out)


class TestCompressionWireBehavior:

    def test_none_does_not_negotiate_compression(self):
        # M1: with compression='none', the server must NOT see an
        # Accept-Encoding that advertises gzip/deflate (aiohttp would otherwise
        # auto-inject one). Either absent or 'identity' is acceptable.
        payload = b'hello world ' * 100
        with _LoopbackServer(payload) as server:
            t = AiohttpHTTPTransport(compression='none')
            t.connect(server.url)
            t.write({'headers': {'accept': 'x'}, 'payload': b'q'})
            t.read_body()
            t.close()
        ae = server.received_accept_encoding
        assert ae in (None, 'identity'), \
            "compression='none' must not negotiate compression, got %r" % ae

    def test_deflate_negotiates_deflate(self):
        # M1: with compression='deflate', the server must see deflate offered.
        payload = b'hello world ' * 100
        with _LoopbackServer(payload) as server:
            t = AiohttpHTTPTransport(compression='deflate')
            t.connect(server.url)
            t.write({'headers': {'accept': 'x'}, 'payload': b'q'})
            t.read_body()
            t.close()
        ae = server.received_accept_encoding
        assert ae is not None and 'deflate' in ae, \
            "compression='deflate' must offer deflate, got %r" % ae

    def test_deflate_response_decompressed_via_get_stream(self):
        # M2: a deflate-compressed response read back through the streaming
        # get_stream() path must be transparently decompressed to the original
        # bytes. Use a payload large enough to span multiple buffer fills.
        payload = b'GRAPHBINARY-PAYLOAD-' * 2000
        with _LoopbackServer(payload) as server:
            t = AiohttpHTTPTransport(compression='deflate')
            t.connect(server.url)
            t.write({'headers': {'accept': 'x'}, 'payload': b'q'})
            stream = t.get_stream()
            # Reading exactly len(payload) decompressed bytes must succeed.
            data = stream.read(len(payload))
            t.close()
        assert data == payload

    def test_deflate_response_decompressed_full_drain(self):
        # M2: draining the whole stream yields exactly the decompressed payload.
        payload = b'abc-' * 5000
        with _LoopbackServer(payload) as server:
            t = AiohttpHTTPTransport(compression='deflate')
            t.connect(server.url)
            t.write({'headers': {'accept': 'x'}, 'payload': b'q'})
            stream = t.get_stream()
            data = _read_all(stream)
            t.close()
        assert data == payload

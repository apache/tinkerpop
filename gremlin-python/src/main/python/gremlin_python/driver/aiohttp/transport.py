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
import socket
import sys

from gremlin_python.driver.exceptions import ReadTimeoutError

if sys.version_info >= (3, 11):
    import asyncio as async_timeout
else:
    import async_timeout

__author__ = 'Lyndon Bauto (lyndonb@bitquilltech.com)'

# Default connection option values (canonical TinkerPop 4.x GLV defaults). The millisecond-suffixed
# options are the primary form (mirroring the other GLVs); aiohttp itself works in seconds, so the
# values are converted internally.
DEFAULT_CONNECT_TIMEOUT_MILLIS = 5000
DEFAULT_IDLE_TIMEOUT_MILLIS = 180000
DEFAULT_KEEP_ALIVE_TIME_MILLIS = 30000
# Seconds equivalents retained for internal use / backwards reference.
DEFAULT_CONNECT_TIMEOUT = DEFAULT_CONNECT_TIMEOUT_MILLIS / 1000
DEFAULT_IDLE_TIMEOUT = DEFAULT_IDLE_TIMEOUT_MILLIS / 1000
DEFAULT_KEEP_ALIVE_TIME = DEFAULT_KEEP_ALIVE_TIME_MILLIS / 1000
DEFAULT_COMPRESSION = 'deflate'

def _resolve_timeout_seconds(millis, seconds, default_millis):
    """Resolve a timeout to seconds (aiohttp's unit) from the ``*_millis`` number or the idiomatic
    unsuffixed seconds number (``None`` means not supplied for either). Supplying both raises
    ``ValueError``; if neither is given, ``default_millis`` is used (``None`` leaves it unset).
    """
    if millis is not None and seconds is not None:
        raise ValueError("provide only one of the milliseconds option or the seconds option, not both")
    if seconds is not None:
        return seconds
    if millis is not None:
        return millis / 1000
    if default_millis is None:
        return None
    return default_millis / 1000


def _normalize_compression(compression):
    """Normalize the compression option to a canonical string ('none' or 'deflate').

    Accepts the string forms 'none'/'deflate'.
    """
    if compression is None:
        return DEFAULT_COMPRESSION
    if isinstance(compression, str):
        normalized = compression.lower()
        if normalized in ('none', 'deflate'):
            return normalized
        raise ValueError("compression must be one of 'none', 'deflate', got '%s'" % compression)
    raise TypeError("compression must be a str ('none'|'deflate'), got %s" % type(compression).__name__)


def _run_read(loop, read_timeout, coro):
    """Run a response-read coroutine on ``loop``, normalizing aiohttp's read timeout
    (SocketTimeoutError / ServerTimeoutError) into a ``ReadTimeoutError`` so a read
    timeout always surfaces as one deterministic, transport-agnostic type. It subclasses
    the builtin ``TimeoutError`` so callers can still ``except TimeoutError``."""
    try:
        return loop.run_until_complete(coro)
    except aiohttp.ServerTimeoutError as e:
        raise ReadTimeoutError(
            f"Read timed out after {read_timeout}s waiting for response data.") from e


def _keep_alive_socket_options(keep_alive_time):
    """Build the list of socket options that enable TCP keep-alive with the
    given idle time before probes begin. TCP_KEEPIDLE is platform dependent
    (Linux); macOS exposes the equivalent as TCP_KEEPALIVE. Both are guarded so
    platforms lacking the option (e.g. Windows) simply enable SO_KEEPALIVE."""
    options = [(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)]
    idle_opt = getattr(socket, 'TCP_KEEPIDLE', None)
    if idle_opt is None:
        # macOS names the idle-before-probe option TCP_KEEPALIVE
        idle_opt = getattr(socket, 'TCP_KEEPALIVE', None)
    if idle_opt is not None:
        options.append((socket.IPPROTO_TCP, idle_opt, int(keep_alive_time)))
    return options


def _keep_alive_socket_factory(keep_alive_time):
    """Return a socket_factory (aiohttp >= 3.11) that applies the keep-alive
    socket options when each connection socket is created."""
    options = _keep_alive_socket_options(keep_alive_time)

    def factory(addr_info):
        family, type_, proto, _, _ = addr_info
        sock = socket.socket(family=family, type=type_, proto=proto)
        for level, optname, value in options:
            try:
                sock.setsockopt(level, optname, value)
            except (OSError, AttributeError):
                pass
        return sock

    return factory


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
        return _run_read(self._loop, self._read_timeout,
                         self._response.content.read(self._FILL_SIZE))


class AiohttpHTTPTransport:
    nest_asyncio_applied = False

    def __init__(self, call_from_event_loop=None, write_timeout=None,
                 connect_timeout_millis=None, connect_timeout=None,
                 idle_timeout_millis=None, idle_timeout=None,
                 read_timeout_millis=None, read_timeout=None,
                 keep_alive_time_millis=None, keep_alive_time=None,
                 compression=DEFAULT_COMPRESSION, proxy=None, trust_env=False,
                 max_connections=None, **kwargs):
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
        self._ssl_context = None
        self._url = None

        # Set all inner variables to parameters passed in.
        self._aiohttp_kwargs = kwargs
        self._write_timeout = write_timeout

        # Timeouts accept a millisecond number (*_millis, primary form) or the idiomatic seconds number
        # (unsuffixed). read_timeout defaults off; the others use the canonical millisecond defaults.
        self._read_timeout = _resolve_timeout_seconds(read_timeout_millis, read_timeout, None)

        # Connection-level pooling / lifecycle options.
        self._connect_timeout = _resolve_timeout_seconds(connect_timeout_millis, connect_timeout, DEFAULT_CONNECT_TIMEOUT_MILLIS)
        self._idle_timeout = _resolve_timeout_seconds(idle_timeout_millis, idle_timeout, DEFAULT_IDLE_TIMEOUT_MILLIS)
        self._keep_alive_time = _resolve_timeout_seconds(keep_alive_time_millis, keep_alive_time, DEFAULT_KEEP_ALIVE_TIME_MILLIS)
        # Caps the aiohttp connector's simultaneous connections per Connection
        # (the Client also sizes its Connection pool by this value).
        self._max_connections = max_connections

        # Compression negotiation. Default 'deflate' (on); advertises
        # Accept-Encoding: deflate. Set 'none' to opt out.
        self._compression = _normalize_compression(compression)

        # HTTP proxy support routed through the ClientSession.
        self._proxy = proxy
        self._trust_env = trust_env

        # ssl: canonical name accepting an ssl.SSLContext.
        if "ssl" in self._aiohttp_kwargs:
            self._ssl_context = self._aiohttp_kwargs.pop("ssl")
            self._enable_ssl = True

    def __del__(self):
        # Close will only actually close if things are left open, so this is safe to call.
        # Clean up any connection resources and close the event loop.
        self.close()

    def connect(self, url, headers=None):
        self._url = url
        # Inner function to perform async connect.
        async def async_connect():
            # Build the TCP connector with the standardized pooling / lifecycle
            # options. keepalive_timeout maps to the idle connection timeout;
            # the socket factory enables TCP keep-alive probes after
            # keep_alive_time idle.
            connector_kwargs = {}
            if self._enable_ssl:
                # ssl context is established through the tcp connector
                connector_kwargs['ssl_context'] = self._ssl_context
            if self._idle_timeout is not None:
                connector_kwargs['keepalive_timeout'] = self._idle_timeout
            if self._keep_alive_time is not None:
                self._apply_keep_alive(connector_kwargs)
            # Reflect max_connections at the aiohttp layer so the connector's
            # simultaneous-connection limit matches the driver option.
            if self._max_connections is not None:
                connector_kwargs['limit'] = self._max_connections

            session_kwargs = {'headers': headers, 'loop': self._loop,
                              'trust_env': self._trust_env}
            # Use the per-socket timeouts (sock_connect/sock_read) rather than a
            # whole-request total, which would abort long but legitimate streaming
            # responses. sock_read bounds idle time between chunks so a stalled
            # server cannot hang forever.
            timeout_kwargs = {}
            if self._connect_timeout is not None:
                timeout_kwargs['sock_connect'] = self._connect_timeout
            if self._read_timeout is not None:
                timeout_kwargs['sock_read'] = self._read_timeout
            if timeout_kwargs:
                session_kwargs['timeout'] = aiohttp.ClientTimeout(**timeout_kwargs)
            if connector_kwargs:
                session_kwargs['connector'] = aiohttp.TCPConnector(**connector_kwargs)

            self._client_session = aiohttp.ClientSession(**session_kwargs)

        # Execute the async connect synchronously.
        self._loop.run_until_complete(async_connect())

    def _apply_keep_alive(self, connector_kwargs):
        """Wire TCP keep-alive into the connector via the aiohttp socket_factory.
        The factory sets SO_KEEPALIVE plus the per-socket idle time; unsupported
        platforms degrade gracefully inside the factory."""
        connector_kwargs['socket_factory'] = _keep_alive_socket_factory(self._keep_alive_time)

    def write(self, message):
        # Negotiate compression unless the caller already set Accept-Encoding:
        # deflate advertises Accept-Encoding: deflate; none suppresses aiohttp's
        # auto-injected Accept-Encoding so compression is not silently negotiated.
        headers = message['headers']
        has_accept_encoding = any(k.lower() == 'accept-encoding' for k in headers)
        skip_auto_headers = None
        if self._compression == 'deflate':
            if not has_accept_encoding:
                headers['accept-encoding'] = 'deflate'
        elif not has_accept_encoding:
            skip_auto_headers = ['Accept-Encoding']

        # Inner function to perform async write.
        async def async_write():
            post_kwargs = dict(self._aiohttp_kwargs)
            if self._proxy is not None:
                post_kwargs['proxy'] = self._proxy
            if skip_auto_headers is not None:
                post_kwargs['skip_auto_headers'] = skip_auto_headers
            async with async_timeout.timeout(self._write_timeout):
                self._http_req_resp = await self._client_session.post(url=self._url,
                                                                      data=message['payload'],
                                                                      headers=headers,
                                                                      **post_kwargs)

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
        return _run_read(self._loop, self._read_timeout, self._http_req_resp.read())

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
        # Connection is closed when client session is closed (or not yet created).
        return self._client_session is None or self._client_session.closed

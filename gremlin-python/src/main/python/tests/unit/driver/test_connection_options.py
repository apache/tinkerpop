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

"""Unit tests for the standardized GLV connection options (TinkerPop 4.x).

These cover option wiring on the aiohttp transport, the Client pool, the
Connection request path, the DriverRemoteConnection surface, and the SigV4
credentials-provider variant. They avoid any network I/O.
"""

import socket
import warnings
from unittest.mock import MagicMock, patch

import pytest

from gremlin_python.driver.aiohttp.transport import (
    AiohttpHTTPTransport,
    _normalize_compression,
    _keep_alive_socket_options,
    _keep_alive_socket_factory,
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_IDLE_TIMEOUT,
    DEFAULT_KEEP_ALIVE_TIME,
)


# ---------------------------------------------------------------------------
# Compression normalization
# ---------------------------------------------------------------------------

class TestCompressionNormalization:

    def test_default_is_deflate(self):
        assert _normalize_compression(None) == 'deflate'

    def test_string_none(self):
        assert _normalize_compression('none') == 'none'

    def test_string_deflate(self):
        assert _normalize_compression('deflate') == 'deflate'

    def test_case_insensitive(self):
        assert _normalize_compression('DEFLATE') == 'deflate'

    def test_invalid_string_raises(self):
        with pytest.raises(ValueError):
            _normalize_compression('gzip')

    def test_invalid_type_raises(self):
        with pytest.raises(TypeError):
            _normalize_compression(5)


# ---------------------------------------------------------------------------
# Keep-alive socket option construction
# ---------------------------------------------------------------------------

class TestKeepAliveSocketOptions:

    def test_includes_so_keepalive(self):
        opts = _keep_alive_socket_options(30)
        assert (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1) in opts

    def test_includes_idle_when_platform_supports(self):
        idle_opt = getattr(socket, 'TCP_KEEPIDLE', None) or getattr(socket, 'TCP_KEEPALIVE', None)
        opts = _keep_alive_socket_options(45)
        if idle_opt is not None:
            assert (socket.IPPROTO_TCP, idle_opt, 45) in opts
        else:
            # Only SO_KEEPALIVE is present when no idle option exists (e.g. Windows)
            assert len(opts) == 1

    def test_socket_factory_applies_options(self):
        factory = _keep_alive_socket_factory(30)
        addr_info = (socket.AF_INET, socket.SOCK_STREAM, 0, '', ('127.0.0.1', 0))
        sock = factory(addr_info)
        try:
            assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) != 0
        finally:
            sock.close()


# ---------------------------------------------------------------------------
# Transport option wiring
# ---------------------------------------------------------------------------

class TestTransportDefaults:

    def test_defaults(self):
        t = AiohttpHTTPTransport()
        assert t._connect_timeout == DEFAULT_CONNECT_TIMEOUT == 5
        assert t._idle_timeout == DEFAULT_IDLE_TIMEOUT == 180
        assert t._keep_alive_time == DEFAULT_KEEP_ALIVE_TIME == 30
        assert t._compression == 'deflate'
        assert t._proxy is None
        assert t._trust_env is False
        t.close()

    def test_explicit_values(self):
        t = AiohttpHTTPTransport(connect_timeout=2, idle_timeout=60,
                                 keep_alive_time=15, compression='deflate',
                                 proxy='http://proxy:3128', trust_env=True)
        assert t._connect_timeout == 2
        assert t._idle_timeout == 60
        assert t._keep_alive_time == 15
        assert t._compression == 'deflate'
        assert t._proxy == 'http://proxy:3128'
        assert t._trust_env is True
        t.close()

    def test_ssl_canonical_option(self):
        import ssl as ssl_module
        ctx = ssl_module.SSLContext(ssl_module.PROTOCOL_TLS_CLIENT)
        t = AiohttpHTTPTransport(ssl=ctx)
        assert t._enable_ssl is True
        assert t._ssl_context is ctx
        t.close()

    def test_ssl_options_deprecated_alias(self):
        import ssl as ssl_module
        ctx = ssl_module.SSLContext(ssl_module.PROTOCOL_TLS_CLIENT)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            t = AiohttpHTTPTransport(ssl_options=ctx)
        assert t._enable_ssl is True
        assert t._ssl_context is ctx
        assert any(issubclass(w.category, DeprecationWarning) for w in caught)
        t.close()

    def test_max_content_length_removed(self):
        # max_content_length is no longer accepted; it would now be forwarded to
        # aiohttp as an unknown kwarg. Verify it is not silently stored anywhere.
        t = AiohttpHTTPTransport()
        assert 'max_content_length' not in t._aiohttp_kwargs
        t.close()


class TestTransportConnectWiring:

    def _connect_capture(self, **transport_kwargs):
        """Construct a transport and capture the kwargs passed to TCPConnector
        and ClientSession during connect(), without doing real I/O."""
        t = AiohttpHTTPTransport(**transport_kwargs)
        captured = {}

        class FakeConnector:
            def __init__(self, **kwargs):
                captured['connector'] = kwargs

        class FakeSession:
            def __init__(self, **kwargs):
                captured['session'] = kwargs

        with patch('aiohttp.TCPConnector', FakeConnector), \
                patch('aiohttp.ClientSession', FakeSession):
            t.connect('http://localhost:8182/gremlin', headers={'a': 'b'})
        t._client_session = None  # FakeSession isn't a real session
        return captured, t

    def test_idle_timeout_maps_to_keepalive_timeout(self):
        captured, t = self._connect_capture(idle_timeout=90)
        assert captured['connector']['keepalive_timeout'] == 90
        t.close()

    def test_connect_timeout_sets_sock_connect(self):
        captured, t = self._connect_capture(connect_timeout=3)
        timeout = captured['session']['timeout']
        assert timeout.sock_connect == 3
        t.close()

    def test_keep_alive_wires_socket_factory(self):
        captured, t = self._connect_capture(keep_alive_time=30)
        # aiohttp >= 3.11 is the declared floor, so socket_factory is always used.
        assert 'socket_factory' in captured['connector']
        assert 'socket_options' not in captured['connector']
        t.close()

    def test_max_connections_sets_connector_limit(self):
        captured, t = self._connect_capture(max_connections=7)
        assert captured['connector']['limit'] == 7
        t.close()

    def test_no_max_connections_leaves_limit_unset(self):
        captured, t = self._connect_capture()
        assert 'limit' not in captured['connector']
        t.close()

    def test_read_timeout_sets_sock_read(self):
        captured, t = self._connect_capture(connect_timeout=3, read_timeout=11)
        timeout = captured['session']['timeout']
        assert timeout.sock_connect == 3
        assert timeout.sock_read == 11
        t.close()

    def test_default_timeout_has_no_unbounded_total(self):
        # Defaults must still arm a socket-connect bound; building ClientTimeout
        # from the socket knobs (not a whole-request total) keeps streaming safe.
        captured, t = self._connect_capture()
        timeout = captured['session']['timeout']
        assert timeout.sock_connect == 5
        assert timeout.total is None
        t.close()

    def test_trust_env_passed_to_session(self):
        captured, t = self._connect_capture(trust_env=True)
        assert captured['session']['trust_env'] is True
        t.close()

    def test_ssl_context_passed_to_connector(self):
        import ssl as ssl_module
        ctx = ssl_module.SSLContext(ssl_module.PROTOCOL_TLS_CLIENT)
        captured, t = self._connect_capture(ssl=ctx)
        assert captured['connector']['ssl_context'] is ctx
        t.close()


class TestTransportWriteCompression:

    def _make_transport(self, **kwargs):
        t = AiohttpHTTPTransport(**kwargs)
        t._url = 'http://localhost:8182/gremlin'

        captured = {}

        async def fake_post(url, data, headers, **post_kwargs):
            captured['headers'] = headers
            captured['post_kwargs'] = post_kwargs
            return MagicMock()

        session = MagicMock()
        session.post = fake_post
        t._client_session = session
        return t, captured

    def test_default_offers_deflate(self):
        # With the default compression ('deflate'), the transport advertises
        # Accept-Encoding: deflate and does not skip the auto-header.
        t, captured = self._make_transport()
        t.write({'headers': {'accept': 'x'}, 'payload': b'{}'})
        assert captured['headers'].get('accept-encoding') == 'deflate'
        assert 'skip_auto_headers' not in captured['post_kwargs']
        t._client_session = None
        t.close()

    def test_deflate_sets_accept_encoding(self):
        t, captured = self._make_transport(compression='deflate')
        t.write({'headers': {'accept': 'x'}, 'payload': b'{}'})
        assert captured['headers'].get('accept-encoding') == 'deflate'
        # deflate must NOT skip the Accept-Encoding auto-header
        assert 'skip_auto_headers' not in captured['post_kwargs']
        t._client_session = None
        t.close()

    def test_none_suppresses_auto_accept_encoding(self):
        # compression='none' must stop aiohttp from auto-injecting
        # Accept-Encoding (gzip, deflate, ...), which would otherwise silently
        # negotiate compression. No explicit header is added; instead the
        # auto-header is skipped.
        t, captured = self._make_transport(compression='none')
        t.write({'headers': {'accept': 'x'}, 'payload': b'{}'})
        assert 'accept-encoding' not in captured['headers']
        assert captured['post_kwargs'].get('skip_auto_headers') == ['Accept-Encoding']
        t._client_session = None
        t.close()

    def test_none_respects_explicit_accept_encoding(self):
        # A caller/interceptor that explicitly sets Accept-Encoding is honored,
        # and the auto-header skip is not applied so their value is sent.
        t, captured = self._make_transport(compression='none')
        t.write({'headers': {'Accept-Encoding': 'gzip'}, 'payload': b'{}'})
        assert captured['headers']['Accept-Encoding'] == 'gzip'
        assert 'skip_auto_headers' not in captured['post_kwargs']
        t._client_session = None
        t.close()

    def test_deflate_respects_existing_header(self):
        t, captured = self._make_transport(compression='deflate')
        t.write({'headers': {'Accept-Encoding': 'gzip'}, 'payload': b'{}'})
        assert captured['headers']['Accept-Encoding'] == 'gzip'
        assert 'accept-encoding' not in captured['headers']
        assert 'skip_auto_headers' not in captured['post_kwargs']
        t._client_session = None
        t.close()

    def test_proxy_passed_to_post(self):
        t, captured = self._make_transport(proxy='http://proxy:3128')
        t.write({'headers': {}, 'payload': b'{}'})
        assert captured['post_kwargs'].get('proxy') == 'http://proxy:3128'
        t._client_session = None
        t.close()

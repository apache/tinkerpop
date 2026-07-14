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

"""Unit tests for connection-level options surfaced on Client,
DriverRemoteConnection, and the SigV4 credentials-provider variant."""

import warnings
from unittest.mock import MagicMock, patch

import pytest

from gremlin_python.driver.client import Client
from gremlin_python.driver.connection import Connection
from gremlin_python.driver.request import RequestMessage


# Patch Connection so Client._fill_pool does not attempt any real connections.
def _make_client(**kwargs):
    with patch('gremlin_python.driver.client.connection.Connection', MagicMock()):
        return Client('http://localhost:8182/gremlin', 'g', **kwargs)


class TestMaxConnections:

    def test_default_is_128(self):
        client = _make_client()
        assert client._max_connections == 128

    def test_explicit_max_connections(self):
        client = _make_client(max_connections=4)
        assert client._max_connections == 4


class TestBatchSize:

    def test_default_is_64(self):
        client = _make_client()
        assert client._batch_size == 64

    def test_explicit_value(self):
        client = _make_client(batch_size=200)
        assert client._batch_size == 200

    def test_fills_batch_size_when_unset(self):
        client = _make_client(batch_size=64)
        conn = MagicMock()
        client._pool.get = MagicMock(return_value=conn)
        client.submit_async('g.V()')
        sent = conn.write.call_args[0][0]
        assert sent.fields['batchSize'] == 64

    def test_does_not_override_per_request_batch_size(self):
        client = _make_client(batch_size=64)
        conn = MagicMock()
        client._pool.get = MagicMock(return_value=conn)
        client.submit_async('g.V()', request_options={'batchSize': 10})
        sent = conn.write.call_args[0][0]
        assert sent.fields['batchSize'] == 10


class TestMaxConnectionsThreadedToTransport:

    def test_max_connections_forwarded_to_connection(self):
        # Client must pass max_connections through to each Connection so the
        # aiohttp connector limit can be set, in addition to sizing the pool.
        captured = {}

        def fake_connection(*args, **kwargs):
            captured.update(kwargs)
            return MagicMock()

        with patch('gremlin_python.driver.client.connection.Connection',
                   side_effect=fake_connection):
            Client('http://localhost:8182/gremlin', 'g', max_connections=5)
        assert captured.get('max_connections') == 5

    def test_default_max_connections_forwarded_to_connection(self):
        captured = {}

        def fake_connection(*args, **kwargs):
            captured.update(kwargs)
            return MagicMock()

        with patch('gremlin_python.driver.client.connection.Connection',
                   side_effect=fake_connection):
            Client('http://localhost:8182/gremlin', 'g')
        assert captured.get('max_connections') == 128


class TestRequestMessageNoMutation:

    def test_resubmit_does_not_accumulate_fields(self):
        # A caller-supplied RequestMessage must not be mutated in place: the
        # second submit with different options must not see the first submit's
        # batchSize/request_options leak in.
        client = _make_client(batch_size=64)
        conn = MagicMock()
        client._pool.get = MagicMock(return_value=conn)

        original = RequestMessage(fields={'g': 'g'}, gremlin='g.V()')

        client.submit_async(original, request_options={'timeoutMillis': 1000})
        # The caller's original message must be untouched.
        assert 'batchSize' not in original.fields
        assert 'timeoutMillis' not in original.fields
        assert original.fields == {'g': 'g'}

        # Resubmit the same message with different options; it must not carry
        # over state from the first submit.
        client.submit_async(original, request_options={'batchSize': 5})
        sent = conn.write.call_args[0][0]
        assert sent.fields['batchSize'] == 5
        assert 'timeoutMillis' not in sent.fields
        # And the original is still pristine.
        assert original.fields == {'g': 'g'}

    def test_batch_size_not_written_to_caller_message(self):
        # Use a non-default value so the assertion proves the configured
        # batch_size flowed through, not the library default (64).
        client = _make_client(batch_size=32)
        conn = MagicMock()
        client._pool.get = MagicMock(return_value=conn)
        original = RequestMessage(fields={'g': 'g'}, gremlin='g.V()')
        client.submit_async(original)
        # default batchSize was applied to the sent clone, not the caller's msg
        sent = conn.write.call_args[0][0]
        assert sent.fields['batchSize'] == 32
        assert 'batchSize' not in original.fields


class TestHeadersKwargRemoved:

    def test_client_rejects_headers_kwarg(self):
        # headers is no longer a named parameter; it lands in transport_kwargs
        # and would be forwarded to the transport. Verify Client has no _headers.
        client = _make_client()
        assert not hasattr(client, '_headers')

    def test_connection_has_no_headers_param(self):
        import inspect
        params = inspect.signature(Connection.__init__).parameters
        assert 'headers' not in params

    def test_connection_internal_headers_default_none(self):
        conn = Connection(
            url='http://localhost:8182/gremlin',
            traversal_source='g',
            executor=MagicMock(),
            pool=MagicMock(),
            enable_user_agent_on_connect=False,
        )
        assert conn._headers is None


class TestDriverRemoteConnectionOptions:

    def test_max_connections_forwarded(self):
        from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
        with patch('gremlin_python.driver.driver_remote_connection.client.Client') as MockClient:
            instance = MockClient.return_value
            instance._url = 'http://localhost:8182/gremlin'
            instance._traversal_source = 'g'
            DriverRemoteConnection('http://localhost:8182/gremlin', 'g',
                                   max_connections=16, batch_size=32)
        _, kwargs = MockClient.call_args
        assert kwargs['max_connections'] == 16
        assert kwargs['batch_size'] == 32

    def test_no_headers_param(self):
        import inspect
        from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
        params = inspect.signature(DriverRemoteConnection.__init__).parameters
        assert 'headers' not in params
        assert 'pool_size' not in params
        assert 'max_connections' in params

    def test_submit_async_signature_single_arg(self):
        import inspect
        from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
        params = list(inspect.signature(DriverRemoteConnection.submit_async).parameters)
        # self + gremlin_lang only
        assert params == ['self', 'gremlin_lang']



class TestSigV4CredentialsProvider:

    def _fake_request(self):
        req = MagicMock()
        req.method = 'POST'
        req.url = 'http://localhost:8182/gremlin'
        req.serialize_body.return_value = b'{}'
        req.headers = {}
        return req

    def test_callable_credentials_provider_used(self):
        from gremlin_python.driver.auth import sigv4

        sentinel_creds = object()
        provider = MagicMock(return_value=sentinel_creds)

        with patch('botocore.auth.SigV4Auth') as MockAuth:
            signer = MockAuth.return_value
            signer.add_auth = MagicMock()
            interceptor = sigv4('us-east-1', 'neptune-db', credentials=provider)
            req = self._fake_request()
            interceptor(req)

        provider.assert_called_once()
        # SigV4Auth must be constructed with the credentials returned by the provider
        args, _ = MockAuth.call_args
        assert args[0] is sentinel_creds

    def test_credentials_object_with_get_credentials(self):
        from gremlin_python.driver.auth import sigv4

        resolved = object()

        class FakeSession:  # not callable; mimics a botocore Session
            def get_credentials(self_inner):
                return resolved

        with patch('botocore.auth.SigV4Auth') as MockAuth:
            MockAuth.return_value.add_auth = MagicMock()
            interceptor = sigv4('us-east-1', 'neptune-db', credentials=FakeSession())
            interceptor(self._fake_request())

        args, _ = MockAuth.call_args
        assert args[0] is resolved

    def test_env_fallback_when_no_provider(self):
        from gremlin_python.driver.auth import sigv4

        resolved = object()
        with patch('boto3.Session') as MockSession, \
                patch('botocore.auth.SigV4Auth') as MockAuth:
            MockSession.return_value.get_credentials.return_value = resolved
            MockAuth.return_value.add_auth = MagicMock()
            interceptor = sigv4('us-east-1', 'neptune-db')
            interceptor(self._fake_request())

        # With no provider supplied, credentials must be resolved from the AWS
        # environment via a boto3 Session, and those resolved credentials must be
        # the ones handed to the SigV4 signer.
        MockSession.assert_called_once()
        MockSession.return_value.get_credentials.assert_called_once()
        args, _ = MockAuth.call_args
        assert args[0] is resolved

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
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

import pytest

from gremlin_python.driver.client import Client
from gremlin_python.driver.connection import Connection
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.driver.remote_connection import RemoteStrategy, RemoteTraversal
from gremlin_python.driver.request import RequestMessage
from gremlin_python.process.traversal import GremlinLang, GValue


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

    def test_submit_does_not_send_traversal_source_as_parameter(self):
        from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
        with patch('gremlin_python.driver.driver_remote_connection.client.Client') as MockClient:
            instance = MockClient.return_value
            instance._url = 'http://localhost:8182/gremlin'
            instance._traversal_source = 'mySource'
            instance.submit.return_value = []
            connection = DriverRemoteConnection('http://localhost:8182/gremlin', 'mySource')
            gremlin_lang = GremlinLang()
            gremlin_lang.add_step('V')

            connection.submit(gremlin_lang)

        request_options = instance.submit.call_args.kwargs['request_options']
        assert "'g':" not in request_options.get('parameters', '')

    def test_submit_sends_only_explicit_parameters(self):
        from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
        with patch('gremlin_python.driver.driver_remote_connection.client.Client') as MockClient:
            instance = MockClient.return_value
            instance._url = 'http://localhost:8182/gremlin'
            instance._traversal_source = 'mySource'
            instance.submit.return_value = []
            connection = DriverRemoteConnection('http://localhost:8182/gremlin', 'mySource')
            gremlin_lang = GremlinLang()
            gremlin_lang.add_step('V', GValue('x', 42))

            connection.submit(gremlin_lang)

        request_options = instance.submit.call_args.kwargs['request_options']
        assert request_options['parameters'] == "['x':42]"
        assert "'g':" not in request_options['parameters']

    def test_submit_async_does_not_send_traversal_source_as_parameter(self):
        from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
        with patch('gremlin_python.driver.driver_remote_connection.client.Client') as MockClient:
            instance = MockClient.return_value
            instance._url = 'http://localhost:8182/gremlin'
            instance._traversal_source = 'mySource'
            result = Future()
            result.set_result([])
            instance.submit_async.return_value = result
            connection = DriverRemoteConnection('http://localhost:8182/gremlin', 'mySource')
            gremlin_lang = GremlinLang()
            gremlin_lang.add_step('V')

            connection.submit_async(gremlin_lang).result()

        request_options = instance.submit_async.call_args.kwargs['request_options']
        assert "'g':" not in request_options.get('parameters', '')

    def test_transaction_submit_does_not_send_traversal_source_as_parameter(self):
        from gremlin_python.driver.transaction import TransactionRemoteConnection
        client = MagicMock()
        client._url = 'http://localhost:8182/gremlin'
        client._traversal_source = 'mySource'
        client.submit.return_value = []
        transaction = MagicMock()
        transaction._client = client
        transaction.is_open = True
        transaction.transaction_id = 'tx-1'
        connection = TransactionRemoteConnection(transaction)
        gremlin_lang = GremlinLang()
        gremlin_lang.add_step('V')

        connection.submit(gremlin_lang)

        request_options = client.submit.call_args.kwargs['request_options']
        assert "'g':" not in request_options.get('parameters', '')

    def test_transaction_submit_sends_only_explicit_parameters(self):
        from gremlin_python.driver.transaction import TransactionRemoteConnection
        client = MagicMock()
        client._url = 'http://localhost:8182/gremlin'
        client._traversal_source = 'mySource'
        client.submit.return_value = []
        transaction = MagicMock()
        transaction._client = client
        transaction.is_open = True
        transaction.transaction_id = 'tx-1'
        connection = TransactionRemoteConnection(transaction)
        gremlin_lang = GremlinLang()
        gremlin_lang.add_step('V', GValue('x', 42))

        connection.submit(gremlin_lang)

        request_options = client.submit.call_args.kwargs['request_options']
        assert request_options['transactionId'] == 'tx-1'
        assert request_options['parameters'] == "['x':42]"
        assert "'g':" not in request_options['parameters']



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


# ===========================================================================
# --- DriverRemoteConnection / RemoteStrategy ---


# ---------------------------------------------------------------------------
# Lightweight stubs
# ---------------------------------------------------------------------------

class _OptionsStrategy:
    """Mimics an options strategy that exposes a ``configuration`` dict.

    extract_request_options only pulls keys from configuration that are present
    in gremlin_python.driver.request.Tokens.
    """

    def __init__(self, configuration):
        self.configuration = configuration


class _GremlinLangStub:
    """Minimal stand-in for a GremlinLang object.

    Exposes only the attributes/methods that DriverRemoteConnection touches:
    get_gremlin, options_strategies, get_parameters_as_string.
    """

    def __init__(self, gremlin='g.V()', options_strategies=None,
                 parameters_string='[:]'):
        self._gremlin = gremlin
        self.options_strategies = options_strategies or []
        self._parameters_string = parameters_string

    def get_gremlin(self):
        return self._gremlin

    def get_parameters_as_string(self):
        return self._parameters_string


class _TraversalStub:
    """Lightweight traversal for exercising RemoteStrategy.apply/apply_async.

    RemoteStrategy only reads .traversers and .gremlin_lang and writes
    .remote_results and .traversers.
    """

    def __init__(self, traversers=None, gremlin_lang=None):
        self.traversers = traversers
        self.gremlin_lang = gremlin_lang or _GremlinLangStub()
        self.remote_results = None


def _make_drc():
    """Build a DriverRemoteConnection with a fully mocked Client.

    Follows the pattern in TestDriverRemoteConnectionOptions:
    patch the Client symbol, then set _url/_traversal_source on the instance the
    constructor reads back from the client.
    """
    with patch('gremlin_python.driver.driver_remote_connection.client.Client') as MockClient:
        instance = MockClient.return_value
        instance._url = 'http://localhost:8182/gremlin'
        instance._traversal_source = 'g'
        drc = DriverRemoteConnection('http://localhost:8182/gremlin', 'g')
    # drc._client is the MockClient.return_value MagicMock; return both.
    return drc, instance


# ---------------------------------------------------------------------------
# 1. extract_request_options -- PURE, no client involved
# ---------------------------------------------------------------------------

class TestExtractRequestOptions:

    def test_bulk_results_defaults_to_true_when_omitted(self):
        gl = _GremlinLangStub(options_strategies=[])
        opts = DriverRemoteConnection.extract_request_options(gl)
        assert opts['bulkResults'] is True

    def test_bulk_results_from_configuration_is_preserved(self):
        # When an options strategy already supplies bulkResults, the default
        # must not clobber it.
        gl = _GremlinLangStub(
            options_strategies=[_OptionsStrategy({'bulkResults': False})])
        opts = DriverRemoteConnection.extract_request_options(gl)
        assert opts['bulkResults'] is False

    def test_tokens_merged_from_each_strategy(self):
        # Two strategies each contribute recognized Tokens; both are merged.
        gl = _GremlinLangStub(options_strategies=[
            _OptionsStrategy({'timeoutMillis': 1000, 'batchSize': 10}),
            _OptionsStrategy({'userAgent': 'ua', 'materializeProperties': 'tokens'}),
        ])
        opts = DriverRemoteConnection.extract_request_options(gl)
        assert opts['timeoutMillis'] == 1000
        assert opts['batchSize'] == 10
        assert opts['userAgent'] == 'ua'
        assert opts['materializeProperties'] == 'tokens'

    def test_non_token_configuration_keys_are_ignored(self):
        # Keys that are not in request.Tokens must not be copied through.
        gl = _GremlinLangStub(
            options_strategies=[_OptionsStrategy({'notAToken': 'x', 'g': 'g'})])
        opts = DriverRemoteConnection.extract_request_options(gl)
        assert 'notAToken' not in opts
        assert opts['g'] == 'g'

    def test_parameters_omitted_when_empty_map(self):
        gl = _GremlinLangStub(parameters_string='[:]')
        opts = DriverRemoteConnection.extract_request_options(gl)
        assert 'parameters' not in opts

    def test_parameters_added_when_non_empty(self):
        gl = _GremlinLangStub(parameters_string='[a:1]')
        opts = DriverRemoteConnection.extract_request_options(gl)
        assert opts['parameters'] == '[a:1]'


# ---------------------------------------------------------------------------
# 2. submit
# ---------------------------------------------------------------------------

class TestSubmit:

    def test_submit_calls_client_and_wraps_result(self):
        drc, client = _make_drc()
        result_set = MagicMock(name='result_set')
        client.submit.return_value = result_set
        gl = _GremlinLangStub(gremlin='g.V().count()')

        remote_traversal = drc.submit(gl)

        # client.submit called with the gremlin string and the request options
        # produced by extract_request_options (bulkResults default present).
        client.submit.assert_called_once()
        args, kwargs = client.submit.call_args
        assert args[0] == 'g.V().count()'
        assert kwargs['request_options'] == \
            DriverRemoteConnection.extract_request_options(gl)
        assert kwargs['request_options']['bulkResults'] is True

        # A RemoteTraversal wrapping the client's result_set is returned.
        assert isinstance(remote_traversal, RemoteTraversal)
        assert remote_traversal.traversers is result_set


# ---------------------------------------------------------------------------
# 3. submit_async
# ---------------------------------------------------------------------------

class TestSubmitAsync:

    def test_client_submit_async_called(self):
        drc, client = _make_drc()
        client_future = Future()
        client.submit_async.return_value = client_future
        gl = _GremlinLangStub(gremlin='g.V()')

        drc.submit_async(gl)

        client.submit_async.assert_called_once()
        args, kwargs = client.submit_async.call_args
        assert args[0] == 'g.V()'
        assert kwargs['request_options'] == \
            DriverRemoteConnection.extract_request_options(gl)

    def test_success_callback_resolves_to_remote_traversal(self):
        drc, client = _make_drc()
        client_future = Future()
        client.submit_async.return_value = client_future
        gl = _GremlinLangStub()

        returned_future = drc.submit_async(gl)
        assert isinstance(returned_future, Future)
        assert not returned_future.done()

        # Completing the client's future should drive the done-callback and
        # resolve the returned future to a RemoteTraversal wrapping the result.
        result_set = MagicMock(name='result_set')
        client_future.set_result(result_set)

        remote_traversal = returned_future.result(timeout=1)
        assert isinstance(remote_traversal, RemoteTraversal)
        assert remote_traversal.traversers is result_set

    def test_error_callback_propagates_exception(self):
        drc, client = _make_drc()
        client_future = Future()
        client.submit_async.return_value = client_future
        gl = _GremlinLangStub()

        returned_future = drc.submit_async(gl)

        boom = RuntimeError('submit failed')
        client_future.set_exception(boom)

        with pytest.raises(RuntimeError, match='submit failed'):
            returned_future.result(timeout=1)


# ---------------------------------------------------------------------------
# 4. RemoteStrategy.apply / apply_async
# ---------------------------------------------------------------------------

class TestRemoteStrategyApply:

    def test_apply_submits_and_sets_results_when_traversers_none(self):
        remote_connection = MagicMock()
        remote_traversal = RemoteTraversal(MagicMock(name='traversers'))
        remote_connection.submit.return_value = remote_traversal

        strategy = RemoteStrategy(remote_connection)
        traversal = _TraversalStub(traversers=None)

        strategy.apply(traversal)

        remote_connection.submit.assert_called_once_with(traversal.gremlin_lang)
        assert traversal.remote_results is remote_traversal
        assert traversal.traversers is remote_traversal.traversers

    def test_apply_is_idempotent_when_traversers_already_set(self):
        remote_connection = MagicMock()
        strategy = RemoteStrategy(remote_connection)
        existing = MagicMock(name='existing_traversers')
        traversal = _TraversalStub(traversers=existing)

        strategy.apply(traversal)

        remote_connection.submit.assert_not_called()
        assert traversal.traversers is existing
        assert traversal.remote_results is None


class TestRemoteStrategyApplyAsync:

    def test_apply_async_submits_and_sets_remote_results_when_none(self):
        remote_connection = MagicMock()
        async_result = MagicMock(name='future')
        remote_connection.submit_async.return_value = async_result

        strategy = RemoteStrategy(remote_connection)
        traversal = _TraversalStub(traversers=None)

        strategy.apply_async(traversal)

        remote_connection.submit_async.assert_called_once_with(traversal.gremlin_lang)
        assert traversal.remote_results is async_result
        # apply_async does NOT populate traversers (only remote_results); this
        # asserts the ACTUAL current behavior of remote_connection.py.
        assert traversal.traversers is None

    def test_apply_async_is_idempotent_when_traversers_already_set(self):
        remote_connection = MagicMock()
        strategy = RemoteStrategy(remote_connection)
        existing = MagicMock(name='existing_traversers')
        traversal = _TraversalStub(traversers=existing)

        strategy.apply_async(traversal)

        remote_connection.submit_async.assert_not_called()
        assert traversal.remote_results is None

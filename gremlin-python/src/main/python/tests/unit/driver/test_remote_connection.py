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

"""Server-free unit tests for DriverRemoteConnection, focused on the
deprecated submitAsync alias forwarding to submit_async."""

from concurrent.futures import Future
from unittest.mock import MagicMock, patch

import pytest

from gremlin_python.driver.remote_connection import RemoteTraversal
from gremlin_python.process.anonymous_traversal import traversal


def _make_drc():
    with patch('gremlin_python.driver.driver_remote_connection.client.Client') as MockClient:
        instance = MockClient.return_value
        instance._url = 'http://localhost:8182/gremlin'
        instance._traversal_source = 'g'
        from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
        drc = DriverRemoteConnection('http://localhost:8182/gremlin', 'g')
    return drc, instance


class TestSubmitAsyncAlias:

    def test_submit_async_emits_deprecation_warning(self):
        drc, _ = _make_drc()
        gremlin_lang = traversal().with_(None).V().count().gremlin_lang
        drc.submit_async = MagicMock(return_value=Future())
        with pytest.warns(DeprecationWarning):
            drc.submitAsync(gremlin_lang)

    def test_submit_async_forwards_single_arg(self):
        drc, _ = _make_drc()
        gremlin_lang = traversal().with_(None).V().count().gremlin_lang
        sentinel = Future()
        drc.submit_async = MagicMock(return_value=sentinel)
        with pytest.warns(DeprecationWarning):
            result = drc.submitAsync(gremlin_lang)
        drc.submit_async.assert_called_once_with(gremlin_lang)
        # submitAsync must return whatever submit_async returns
        assert result is sentinel

    def test_submit_async_returns_future_resolving_to_remote_traversal(self):
        drc, instance = _make_drc()
        gremlin_lang = traversal().with_(None).V().count().gremlin_lang

        # Drive a real Future out of the mocked client.submit_async.
        client_future = Future()
        instance.submit_async = MagicMock(return_value=client_future)

        with pytest.warns(DeprecationWarning):
            returned = drc.submitAsync(gremlin_lang)

        # A Future is returned before the underlying result is available.
        assert isinstance(returned, Future)
        assert not returned.done()

        # Resolve the client-side future; the done callback should populate
        # the returned future with a RemoteTraversal wrapping the result set.
        result_set = MagicMock()
        client_future.set_result(result_set)

        assert returned.done()
        assert isinstance(returned.result(), RemoteTraversal)

    def test_submit_async_does_not_raise_type_error(self):
        drc, instance = _make_drc()
        gremlin_lang = traversal().with_(None).V().count().gremlin_lang
        instance.submit_async = MagicMock(return_value=Future())
        # Previously submitAsync forwarded 3 positional args to the single-arg
        # submit_async, raising TypeError. It must not raise now.
        with pytest.warns(DeprecationWarning):
            drc.submitAsync(gremlin_lang)

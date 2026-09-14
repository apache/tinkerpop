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

"""Explicit-transaction lifecycle
(gremlin_python.driver.transaction.Transaction and
GraphTraversalSource.execute_in_tx). A MagicMock stands in for the Client; assertions target the actual scripts
submitted and the tracked open/closed state."""

from unittest.mock import MagicMock

import pytest

from gremlin_python.driver.transaction import Transaction


def _make_client(transaction_id='tx-123'):
    """A mock Client whose submit(...).all().result() yields the server's
    begin response (a list with a transactionId dict). The same mock serves
    commit/rollback submits, whose result value is ignored by the code."""
    client = MagicMock()
    client._url = 'http://localhost:8182/gremlin'
    client._traversal_source = 'g'
    result = MagicMock()
    result.all.return_value.result.return_value = [{'transactionId': transaction_id}]
    client.submit.return_value = result
    return client


def _scripts_submitted(client):
    """The first positional arg (the gremlin-lang script) of each submit call."""
    return [c.args[0] for c in client.submit.call_args_list if c.args]


class TestBegin:

    def test_begin_opens_transaction_and_returns_bound_gts(self):
        client = _make_client()
        tx = Transaction(client)
        gts = tx.begin()
        assert tx.is_open
        assert tx.transaction_id == 'tx-123'
        # begin returns a GraphTraversalSource bound to the transaction
        assert gts is not None
        client.track_transaction.assert_called_once_with(tx)

    def test_begin_is_idempotent_when_already_open(self):
        # Calling begin() again must not re-issue "g.tx().begin()" nor raise;
        # it reuses the existing transactionId.
        client = _make_client()
        tx = Transaction(client)
        tx.begin()
        tx.begin()
        begin_calls = [s for s in _scripts_submitted(client) if s == 'g.tx().begin()']
        assert len(begin_calls) == 1
        assert tx.is_open
        # track_transaction is only invoked on the initial open
        client.track_transaction.assert_called_once_with(tx)

    def test_begin_after_close_raises_single_use(self):
        # A transaction is single-use: after it is closed, begin() must raise.
        client = _make_client()
        tx = Transaction(client)
        tx.begin()
        tx.commit()
        with pytest.raises(Exception, match="closed and cannot be reused"):
            tx.begin()


class TestCommitRollback:

    def test_commit_submits_commit_script_with_transaction_id(self):
        client = _make_client()
        tx = Transaction(client)
        tx.begin()
        tx.commit()
        assert 'g.tx().commit()' in _scripts_submitted(client)
        # commit is a terminal state
        assert not tx.is_open
        client.untrack_transaction.assert_called_once_with(tx)
        # the transactionId is attached to the commit submit
        commit_call = next(c for c in client.submit.call_args_list
                           if c.args and c.args[0] == 'g.tx().commit()')
        assert commit_call.kwargs['request_options'] == {'transactionId': 'tx-123'}

    def test_rollback_submits_rollback_script_with_transaction_id(self):
        client = _make_client()
        tx = Transaction(client)
        tx.begin()
        tx.rollback()
        assert 'g.tx().rollback()' in _scripts_submitted(client)
        assert not tx.is_open
        client.untrack_transaction.assert_called_once_with(tx)
        rollback_call = next(c for c in client.submit.call_args_list
                             if c.args and c.args[0] == 'g.tx().rollback()')
        assert rollback_call.kwargs['request_options'] == {'transactionId': 'tx-123'}

    def test_commit_when_not_open_raises(self):
        client = _make_client()
        tx = Transaction(client)
        with pytest.raises(Exception, match="Transaction is not open"):
            tx.commit()


class TestClose:

    def test_close_defaults_to_rollback_not_commit(self):
        # close() on an open transaction rolls back (it does not commit).
        client = _make_client()
        tx = Transaction(client)
        tx.begin()
        tx.close()
        scripts = _scripts_submitted(client)
        assert 'g.tx().rollback()' in scripts
        assert 'g.tx().commit()' not in scripts
        assert not tx.is_open

    def test_close_is_noop_when_never_opened(self):
        # close() on a transaction that was never begun must not raise and
        # must not submit anything.
        client = _make_client()
        tx = Transaction(client)
        tx.close()
        assert client.submit.call_count == 0

    def test_close_is_idempotent_after_already_closed(self):
        # After commit the transaction is closed; a follow-up close() must not
        # raise and must not issue a second rollback/commit.
        client = _make_client()
        tx = Transaction(client)
        tx.begin()
        tx.commit()
        submit_count_after_commit = client.submit.call_count
        tx.close()  # no throw
        assert client.submit.call_count == submit_count_after_commit


class TestContextManager:

    def test_enter_returns_transaction(self):
        client = _make_client()
        tx = Transaction(client)
        assert tx.__enter__() is tx

    def test_exit_rolls_back_open_transaction_on_clean_exit(self):
        # ACTUAL semantics: __exit__ rolls back an open transaction even on a
        # clean (exception-free) exit; it does NOT commit.
        client = _make_client()
        tx = Transaction(client)
        with tx as t:
            t.begin()
            assert t.is_open
        assert not tx.is_open
        scripts = _scripts_submitted(client)
        assert 'g.tx().rollback()' in scripts
        assert 'g.tx().commit()' not in scripts

    def test_exit_returns_false_to_not_suppress_exceptions(self):
        client = _make_client()
        tx = Transaction(client)
        tx.begin()
        assert tx.__exit__(None, None, None) is False

    def test_exit_is_noop_when_not_open(self):
        client = _make_client()
        tx = Transaction(client)
        with tx:
            pass  # never begun
        assert client.submit.call_count == 0


def _make_gts(client):
    """Build a real GraphTraversalSource whose RemoteStrategy wraps a mock
    RemoteConnection backed by the given Client, so g.tx()/execute_in_tx work
    without a network."""
    from gremlin_python.process.graph_traversal import GraphTraversalSource
    from gremlin_python.process.traversal import TraversalStrategies, GremlinLang
    from gremlin_python.driver.remote_connection import RemoteStrategy

    remote_connection = MagicMock()
    remote_connection._client = client
    strategies = TraversalStrategies()
    strategies.add_strategies([RemoteStrategy(remote_connection)])
    return GraphTraversalSource(None, strategies, GremlinLang())


class TestExecuteInTx:

    def test_commits_and_returns_body_value_on_success(self):
        client = _make_client()
        g = _make_gts(client)

        seen = {}

        def body(gtx):
            seen['gtx'] = gtx
            return 42

        result = g.execute_in_tx(body)

        assert result == 42
        # the body was handed a (transaction-bound) GraphTraversalSource
        assert seen['gtx'] is not None
        scripts = _scripts_submitted(client)
        assert 'g.tx().begin()' in scripts
        assert 'g.tx().commit()' in scripts
        assert 'g.tx().rollback()' not in scripts

    def test_rolls_back_and_reraises_when_body_raises(self):
        client = _make_client()
        g = _make_gts(client)

        def body(gtx):
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            g.execute_in_tx(body)

        scripts = _scripts_submitted(client)
        assert 'g.tx().begin()' in scripts
        assert 'g.tx().rollback()' in scripts
        assert 'g.tx().commit()' not in scripts


class TestBeginErrorPaths:

    def test_begin_reraises_when_submit_raises_and_marks_failed(self):
        # If the client submit for "g.tx().begin()" raises, begin() marks the
        # transaction _failed and re-raises the original error. Because _failed
        # is terminal, a subsequent begin() takes the guard branch and reports
        # the single-use transaction as unusable rather than retrying.
        client = _make_client()
        client.submit.side_effect = RuntimeError("connection lost")
        tx = Transaction(client)
        with pytest.raises(RuntimeError, match="connection lost"):
            tx.begin()
        assert not tx.is_open
        with pytest.raises(Exception, match="closed and cannot be reused"):
            tx.begin()

    def test_begin_raises_when_server_returns_empty_result(self):
        # An empty result list means the server sent no transaction ID.
        client = _make_client()
        client.submit.return_value.all.return_value.result.return_value = []
        tx = Transaction(client)
        with pytest.raises(Exception, match="Server did not return transaction ID"):
            tx.begin()
        assert not tx.is_open

    def test_begin_raises_when_result_is_not_a_dict(self):
        # results[0] must be a dict; a non-dict payload is an unexpected format.
        client = _make_client()
        client.submit.return_value.all.return_value.result.return_value = ["not-a-dict"]
        tx = Transaction(client)
        with pytest.raises(Exception, match="expected format"):
            tx.begin()
        assert not tx.is_open

    def test_begin_raises_when_transaction_id_is_empty(self):
        # A dict that carries a falsy transactionId is rejected as empty.
        client = _make_client()
        client.submit.return_value.all.return_value.result.return_value = [{'transactionId': None}]
        tx = Transaction(client)
        with pytest.raises(Exception, match="Server returned empty transaction ID"):
            tx.begin()
        assert not tx.is_open


class TestSubmitDirect:

    def test_submit_merges_transaction_id_into_request_options(self):
        # Transaction.submit() injects the transactionId and merges any caller
        # supplied request_options on top of it.
        client = _make_client()
        tx = Transaction(client)
        tx.begin()
        client.submit.reset_mock()

        tx.submit('g.V()', request_options={'k': 1})

        client.submit.assert_called_once()
        call = client.submit.call_args
        assert call.args[0] == 'g.V()'
        assert call.kwargs['parameters'] is None
        assert call.kwargs['request_options'] == {'transactionId': 'tx-123', 'k': 1}

    def test_submit_when_not_open_raises(self):
        client = _make_client()
        tx = Transaction(client)
        with pytest.raises(Exception, match="Transaction is not open"):
            tx.submit('g.V()')

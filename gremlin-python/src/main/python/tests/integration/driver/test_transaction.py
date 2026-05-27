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
import os

import pytest

from gremlin_python.driver.client import Client
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal

gremlin_server_url = os.environ.get('GREMLIN_SERVER_URL', 'http://localhost:{}/gremlin')
test_no_auth_url = gremlin_server_url.format(45940)


@pytest.fixture
def client():
    c = Client(test_no_auth_url, 'gtx')
    yield c
    c.close()


@pytest.fixture
def remote_connection():
    rc = DriverRemoteConnection(test_no_auth_url, 'gtx')
    yield rc
    rc.close()


@pytest.fixture(autouse=True)
def clean_graph():
    """Drop all vertices before each test to prevent cross-test contamination."""
    c = Client(test_no_auth_url, 'gtx')
    c.submit("g.V().drop()").all().result()
    c.close()
    yield


class TestTransaction(object):

    def test_should_commit_transaction(self, client):
        tx = client.transact()
        tx.begin()
        assert tx.is_open

        tx.submit("g.addV('person').property('name','alice')")

        # Uncommitted data not visible outside the transaction
        result = client.submit("g.V().hasLabel('person').count()").all().result()
        assert result[0] == 0

        tx.commit()
        assert not tx.is_open

        # Committed data visible
        result = client.submit("g.V().hasLabel('person').count()").all().result()
        assert result[0] == 1

    def test_should_rollback_transaction(self, client):
        tx = client.transact()
        tx.begin()
        assert tx.is_open

        tx.submit("g.addV('person').property('name','bob')")

        tx.rollback()
        assert not tx.is_open

        # Data discarded after rollback
        result = client.submit("g.V().hasLabel('person').count()").all().result()
        assert result[0] == 0

    def test_should_support_intra_transaction_consistency(self, client):
        tx = client.transact()
        tx.begin()

        tx.submit("g.addV('test').property('name','A')")
        # Read-your-own-writes
        result = tx.submit("g.V().hasLabel('test').count()").all().result()
        assert result[0] == 1

        tx.submit("g.addV('test').property('name','B')")
        tx.submit("g.V().has('name','A').addE('knows').to(V().has('name','B'))")

        result = tx.submit("g.V().hasLabel('test').count()").all().result()
        assert result[0] == 2
        result = tx.submit("g.V().outE('knows').count()").all().result()
        assert result[0] == 1

        tx.commit()

        result = client.submit("g.V().hasLabel('test').count()").all().result()
        assert result[0] == 2

    def test_should_throw_on_submit_after_commit(self, client):
        tx = client.transact()
        tx.begin()
        tx.submit("g.addV()")
        tx.commit()

        with pytest.raises(Exception, match="Transaction is not open"):
            tx.submit("g.V().count()")

    def test_should_throw_on_submit_after_rollback(self, client):
        tx = client.transact()
        tx.begin()
        tx.submit("g.addV()")
        tx.rollback()

        with pytest.raises(Exception, match="Transaction is not open"):
            tx.submit("g.V().count()")

    def test_should_throw_on_double_begin(self, client):
        tx = client.transact()
        tx.begin()

        with pytest.raises(Exception, match="Transaction already started"):
            tx.begin()

    def test_should_throw_on_commit_when_not_open(self, client):
        tx = client.transact()

        with pytest.raises(Exception, match="Transaction is not open"):
            tx.commit()

    def test_should_throw_on_rollback_when_not_open(self, client):
        tx = client.transact()

        with pytest.raises(Exception, match="Transaction is not open"):
            tx.rollback()

    def test_should_return_none_transaction_id_before_begin(self, client):
        tx = client.transact()
        assert tx.transaction_id is None

        tx.begin()
        assert tx.transaction_id is not None
        assert len(tx.transaction_id) > 0

    def test_should_rollback_on_close_by_default(self, client):
        tx = client.transact()
        tx.begin()
        tx.submit("g.addV('person').property('name','close_test')")
        tx.close()
        assert not tx.is_open

        # Data should NOT persist (rollback is default)
        result = client.submit("g.V().hasLabel('person').count()").all().result()
        assert result[0] == 0

    def test_should_isolate_concurrent_transactions(self, client):
        tx1 = client.transact()
        tx1.begin()
        tx2 = client.transact()
        tx2.begin()

        tx1.submit("g.addV('tx1')")
        tx2.submit("g.addV('tx2')")

        # tx1 should not see tx2's data and vice versa
        result = tx1.submit("g.V().hasLabel('tx2').count()").all().result()
        assert result[0] == 0
        result = tx2.submit("g.V().hasLabel('tx1').count()").all().result()
        assert result[0] == 0

        tx1.commit()
        tx2.commit()

        # Both should be visible after commit
        result = client.submit("g.V().hasLabel('tx1').count()").all().result()
        assert result[0] == 1
        result = client.submit("g.V().hasLabel('tx2').count()").all().result()
        assert result[0] == 1

    def test_should_isolate_transactional_and_non_transactional_requests(self, client):
        tx = client.transact()
        tx.begin()
        tx.submit("g.addV('tx_data')")

        # Non-transactional read should not see uncommitted data
        result = client.submit("g.V().hasLabel('tx_data').count()").all().result()
        assert result[0] == 0

        tx.commit()

        result = client.submit("g.V().hasLabel('tx_data').count()").all().result()
        assert result[0] == 1

    def test_should_open_and_close_many_transactions_sequentially(self, client):
        num_transactions = 50
        for i in range(num_transactions):
            tx = client.transact()
            tx.begin()
            tx.submit("g.addV('stress')")
            tx.commit()

        result = client.submit("g.V().hasLabel('stress').count()").all().result()
        assert result[0] == num_transactions

    def test_should_keep_transaction_open_after_traversal_error(self, client):
        tx = client.transact()
        tx.begin()
        tx.submit("g.addV('good_vertex')")

        # Submit a bad traversal that should fail
        try:
            tx.submit("g.V().fail()")
        except Exception:
            pass

        # Transaction should still be open
        assert tx.is_open
        tx.rollback()

        assert not tx.is_open
        result = client.submit("g.V().hasLabel('good_vertex').count()").all().result()
        assert result[0] == 0

    def test_should_work_with_traversal_api(self, remote_connection):
        g = traversal().with_(remote_connection)

        tx = g.tx()
        gtx = tx.begin()
        gtx.addV("val").iterate()
        tx.commit()

        assert g.V().hasLabel("val").count().next() == 1

    def test_context_manager_rollback_on_exception(self, client):
        try:
            with client.transact() as tx:
                tx.begin()
                tx.submit("g.addV('ctx_test')")
                raise RuntimeError("simulated error")
        except RuntimeError:
            pass

        # Data should NOT persist (context manager rolls back)
        result = client.submit("g.V().hasLabel('ctx_test').count()").all().result()
        assert result[0] == 0


    def test_should_reject_begin_on_non_transactional_graph(self):
        # gclassic is a non-transactional graph alias
        c = Client(test_no_auth_url, 'gclassic')
        try:
            tx = c.transact()
            with pytest.raises(Exception, match="Graph does not support transactions"):
                tx.begin()
        finally:
            c.close()

    def test_should_clean_up_on_begin_failure(self):
        c = Client(test_no_auth_url, 'gclassic')
        try:
            tx = c.transact()
            try:
                tx.begin()
            except Exception:
                pass

            # Transaction should not be open after failed begin
            assert not tx.is_open
            assert tx.transaction_id is None

            # Cannot begin again on a failed transaction
            with pytest.raises(Exception):
                tx.begin()
        finally:
            c.close()


    def test_should_return_same_transaction_from_gtx_tx(self, client):
        tx = client.transact()
        gtx = tx.begin()
        same_tx = gtx.tx()
        assert same_tx is tx

    def test_should_throw_on_begin_from_gtx_tx(self, client):
        tx = client.transact()
        gtx = tx.begin()
        same_tx = gtx.tx()

        with pytest.raises(Exception, match="Transaction already started"):
            same_tx.begin()

        tx.rollback()

    def test_should_commit_via_gtx_tx(self, client):
        tx = client.transact()
        gtx = tx.begin()
        gtx.addV("gtx_commit_test").iterate()
        gtx.tx().commit()

        result = client.submit("g.V().hasLabel('gtx_commit_test').count()").all().result()
        assert result[0] == 1


    def test_should_throw_on_double_commit(self, client):
        tx = client.transact()
        tx.begin()
        tx.commit()

        with pytest.raises(Exception, match="Transaction is not open"):
            tx.commit()

    def test_should_throw_on_double_rollback(self, client):
        tx = client.transact()
        tx.begin()
        tx.rollback()

        with pytest.raises(Exception, match="Transaction is not open"):
            tx.rollback()


    def test_should_not_allow_begin_after_commit(self, client):
        tx = client.transact()
        tx.begin()
        tx.commit()

        with pytest.raises(Exception, match="Transaction already started"):
            tx.begin()

    def test_should_not_allow_begin_after_rollback(self, client):
        tx = client.transact()
        tx.begin()
        tx.rollback()

        with pytest.raises(Exception, match="Transaction already started"):
            tx.begin()


    def test_should_rollback_on_client_close(self):
        c = Client(test_no_auth_url, 'gtx')
        tx = c.transact()
        tx.begin()
        tx.submit("g.addV('client_close_test')")
        c.close()

        assert not tx.is_open

        c2 = Client(test_no_auth_url, 'gtx')
        result = c2.submit("g.V().hasLabel('client_close_test').count()").all().result()
        assert result[0] == 0
        c2.close()

    def test_should_rollback_on_drc_close(self, remote_connection):
        g = traversal().with_(remote_connection)
        tx = g.tx()
        gtx = tx.begin()
        gtx.addV("drc_close_test").iterate()

        remote_connection.close()

        assert not tx.is_open

        c2 = Client(test_no_auth_url, 'gtx')
        result = c2.submit("g.V().hasLabel('drc_close_test').count()").all().result()
        assert result[0] == 0
        c2.close()


    def test_should_multi_rollback_transactions(self, client):
        tx1 = client.transact()
        tx1.begin()
        tx2 = client.transact()
        tx2.begin()

        tx1.submit("g.addV('multi_rb1')")
        tx2.submit("g.addV('multi_rb2')")

        tx1.rollback()
        assert not tx1.is_open
        result = client.submit("g.V().hasLabel('multi_rb1').count()").all().result()
        assert result[0] == 0

        tx2.rollback()
        assert not tx2.is_open
        result = client.submit("g.V().hasLabel('multi_rb2').count()").all().result()
        assert result[0] == 0

    def test_should_multi_commit_and_rollback(self, client):
        tx1 = client.transact()
        tx1.begin()
        tx2 = client.transact()
        tx2.begin()

        tx1.submit("g.addV('multi_cr1')")
        tx2.submit("g.addV('multi_cr2')")

        tx1.commit()
        result = client.submit("g.V().hasLabel('multi_cr1').count()").all().result()
        assert result[0] == 1

        tx2.rollback()
        result = client.submit("g.V().hasLabel('multi_cr2').count()").all().result()
        assert result[0] == 0

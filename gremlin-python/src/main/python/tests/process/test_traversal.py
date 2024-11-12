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

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com), Lyndon Bauto (lyndonb@bitquilltech.com)'

import os
import pytest
from pytest import fail

from gremlin_python.driver import serializer
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.structure.graph import Graph
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.traversal import P, Direction
from gremlin_python.process.graph_traversal import __

gremlin_server_url = os.environ.get('GREMLIN_SERVER_URL', 'ws://localhost:{}/gremlin')
anonymous_url = gremlin_server_url.format(45940)


class TestTraversal(object):

    def test_P(self):
        # verify that the order of operations is respected
        assert "and(eq(a),lt(b))" == str(P.eq("a").and_(P.lt("b")))
        assert "and(or(lt(b),gt(c)),neq(d))" == str(P.lt("b").or_(P.gt("c")).and_(P.neq("d")))
        assert "and(or(lt(b),gt(c)),or(neq(d),gte(e)))" == str(
            P.lt("b").or_(P.gt("c")).and_(P.neq("d").or_(P.gte("e"))))

    def test_anonymous_traversal(self):
        gremlin = __.__(1).gremlin_lang
        assert "__.inject(1)" == gremlin.get_gremlin('__')

        ##
        gremlin = __.start().gremlin_lang
        assert "" == gremlin.get_gremlin('')

    def test_clone_traversal(self):
        g = traversal().with_(None)
        original = g.V().out("created")
        clone = original.clone().out("knows")
        cloneClone = clone.clone().out("created")

        assert "g.V().out('created')" == original.gremlin_lang.get_gremlin()
        assert "g.V().out('created').out('knows')" == clone.gremlin_lang.get_gremlin()
        assert "g.V().out('created').out('knows').out('created')" == cloneClone.gremlin_lang.get_gremlin()

        original.has("person", "name", "marko")
        clone.V().out()

        assert "g.V().out('created').has('person','name','marko')" == original.gremlin_lang.get_gremlin()
        assert "g.V().out('created').out('knows').V().out()" == clone.gremlin_lang.get_gremlin()
        assert "g.V().out('created').out('knows').out('created')" == cloneClone.gremlin_lang.get_gremlin()


    def test_no_sugar_for_magic_methods(self):
        g = traversal().with_(None)

        t = g.V().age
        assert "g.V().values('age')" == t.gremlin_lang.get_gremlin()

        try:
            t = g.V().__len__
            fail("can't do sugar with magic")
        except AttributeError as err:
            assert str(
                err) == 'Python magic methods or keys starting with double underscore cannot be used for Gremlin sugar - prefer values(__len__)'

    def test_enforce_anonymous_child_traversal(self):
        g = traversal().with_(None)
        g.V(0).add_e("self").to(__.V(1))

        try:
            g.V(0).add_e("self").to(g.V(1))
            assert False
        except TypeError:
            pass

    @pytest.mark.skip(reason="enable after transaction is implemented in HTTP")
    def test_transaction_commit(self, remote_transaction_connection):
        # Start a transaction traversal.
        g = traversal().with_(remote_transaction_connection)
        start_count = g.V().count().next()
        tx = g.tx()

        # Except transaction to not be open until begin is called.
        assert not tx.isOpen()
        gtx = tx.begin()
        assert tx.isOpen()

        add_node_validate_transaction_state(g, gtx, start_count, start_count, [tx])
        add_node_validate_transaction_state(gtx, g, start_count + 2, start_count, [tx])

        # Commit the transaction, this should close it and add our transaction only vertex to the graph,
        # so our vertex count outside the transaction should be 2 + the start count.
        tx.commit()
        assert not tx.isOpen()
        assert g.V().count().next() == start_count + 2

        drop_graph_check_count(g)
        verify_gtx_closed(gtx)

    @pytest.mark.skip(reason="enable after transaction is implemented in HTTP")
    def test_transaction_rollback(self, remote_transaction_connection):
        # Start a transaction traversal.
        g = traversal().with_(remote_transaction_connection)
        start_count = g.V().count().next()
        tx = g.tx()

        # Except transaction to not be open until begin is called.
        assert not tx.isOpen()
        gtx = tx.begin()
        assert tx.isOpen()

        add_node_validate_transaction_state(g, gtx, start_count, start_count, [tx])
        add_node_validate_transaction_state(gtx, g, start_count + 2, start_count, [tx])

        # Commit the transaction, this should close it and not add our transaction only vertex to the graph,
        # so our vertex count outside the transaction should be the start count.
        tx.rollback()
        assert not tx.isOpen()
        assert g.V().count().next() == start_count + 1

        drop_graph_check_count(g)
        verify_gtx_closed(gtx)

    @pytest.mark.skip(reason="enable after transaction is implemented in HTTP")
    def test_transaction_no_begin(self, remote_transaction_connection):
        # Start a transaction traversal.
        g = traversal().with_(remote_transaction_connection)
        tx = g.tx()

        # Except transaction to not be open until begin is called.
        assert not tx.isOpen()

        try:
            # Attempt to commit even though no tx is started.
            tx().commit()
            assert False
        except Exception as e:
            assert not tx.isOpen()

        try:
            # Attempt to rollback even though no tx is started.
            tx().rollback()
            assert False
        except Exception as e:
            assert not tx.isOpen()

        try:
            # Attempt to invoke tx().tx() which is illegal.
            tx().tx()
            assert False
        except Exception as e:
            assert not tx.isOpen()

        # Create transaction and verify it is open.
        gtx = tx.begin()
        assert tx.isOpen()
        try:
            # Attempt to begin gtx which is illegal.
            gtx().begin()
            assert False
        except Exception as e:
            assert tx.isOpen()

        # Commit this unused transaction and verify it is no longer open.
        tx.commit()
        assert not tx.isOpen()

        # Create another transaction and verify it is open.
        gtx = tx.begin()
        assert tx.isOpen()

        # Rollback this unused transaction and verify it is no longer open.
        tx.rollback()
        assert not tx.isOpen()

    @pytest.mark.skip(reason="enable after transaction is implemented in HTTP")
    def test_multi_commit_transaction(self, remote_transaction_connection):
        # Start a transaction traversal.
        g = traversal().with_(remote_transaction_connection)
        start_count = g.V().count().next()

        # Create two transactions.
        tx1, tx2 = g.tx(), g.tx()

        # Generate two GraphTraversalSource's for each transaction with begin.
        gtx1, gtx2 = tx1.begin(), tx2.begin()
        verify_tx_state([tx1, tx2], True)

        # Add node to gtx1, which should be visible to gtx1, not gtx2.
        add_node_validate_transaction_state(g, gtx1, start_count, start_count, [tx1, tx2])

        # Add node to gtx2, which should be visible to gtx2, not gtx2
        add_node_validate_transaction_state(g, gtx2, start_count, start_count, [tx1, tx2])

        # Add node to gtx1, which should be visible to gtx1, not gtx2. Note previous node also added.
        add_node_validate_transaction_state(g, gtx1, start_count, start_count + 1, [tx1, tx2])

        tx1.commit()
        verify_tx_state([tx1], False)
        verify_tx_state([tx2], True)
        assert g.V().count().next() == start_count + 2

        tx2.commit()
        verify_tx_state([tx1, tx2], False)
        assert g.V().count().next() == start_count + 3

    @pytest.mark.skip(reason="enable after transaction is implemented in HTTP")
    def test_multi_rollback_transaction(self, remote_transaction_connection):
        # Start a transaction traversal.
        g = traversal().with_(remote_transaction_connection)
        start_count = g.V().count().next()

        # Create two transactions.
        tx1, tx2 = g.tx(), g.tx()

        # Generate two GraphTraversalSource's for each transaction with begin.
        gtx1, gtx2 = tx1.begin(), tx2.begin()
        verify_tx_state([tx1, tx2], True)

        # Add node to gtx1, which should be visible to gtx1, not gtx2.
        add_node_validate_transaction_state(g, gtx1, start_count, start_count, [tx1, tx2])

        # Add node to gtx2, which should be visible to gtx2, not gtx2
        add_node_validate_transaction_state(g, gtx2, start_count, start_count, [tx1, tx2])

        # Add node to gtx1, which should be visible to gtx1, not gtx2. Note previous node also added.
        add_node_validate_transaction_state(g, gtx1, start_count, start_count + 1, [tx1, tx2])

        tx1.rollback()
        verify_tx_state([tx1], False)
        verify_tx_state([tx2], True)
        assert g.V().count().next() == start_count

        tx2.rollback()
        verify_tx_state([tx1, tx2], False)
        assert g.V().count().next() == start_count

    @pytest.mark.skip(reason="enable after transaction is implemented in HTTP")
    def test_multi_commit_and_rollback(self, remote_transaction_connection):
        # Start a transaction traversal.
        g = traversal().with_(remote_transaction_connection)
        start_count = g.V().count().next()

        # Create two transactions.
        tx1, tx2 = g.tx(), g.tx()

        # Generate two GraphTraversalSource's for each transaction with begin.
        gtx1, gtx2 = tx1.begin(), tx2.begin()
        verify_tx_state([tx1, tx2], True)

        # Add node to gtx1, which should be visible to gtx1, not gtx2.
        add_node_validate_transaction_state(g, gtx1, start_count, start_count, [tx1, tx2])

        # Add node to gtx2, which should be visible to gtx2, not gtx2
        add_node_validate_transaction_state(g, gtx2, start_count, start_count, [tx1, tx2])

        # Add node to gtx1, which should be visible to gtx1, not gtx2. Note previous node also added.
        add_node_validate_transaction_state(g, gtx1, start_count, start_count + 1, [tx1, tx2])

        tx1.commit()
        verify_tx_state([tx1], False)
        verify_tx_state([tx2], True)
        assert g.V().count().next() == start_count + 2

        tx2.rollback()
        verify_tx_state([tx1, tx2], False)
        assert g.V().count().next() == start_count + 2

    @pytest.mark.skip(reason="enable after transaction is implemented in HTTP")
    def test_transaction_close_tx(self):
        remote_conn = create_connection_to_gtx()
        g = traversal().with_(remote_conn)

        drop_graph_check_count(g)

        tx1 = g.tx()
        tx2 = g.tx()

        # open up two sessions and create stuff
        gtx1 = tx1.begin()
        gtx2 = tx2.begin()

        add_node_validate_transaction_state(g, gtx1, 0, 0, [tx1, tx2])
        add_node_validate_transaction_state(g, gtx2, 0, 0, [tx1, tx2])
        add_node_validate_transaction_state(g, gtx2, 0, 1, [tx1, tx2])
        add_node_validate_transaction_state(g, gtx2, 0, 2, [tx1, tx2])

        # someone gets lazy and doesn't commit/rollback and just calls close() - the graph
        # will decide how to treat the transaction, but for neo4j/gremlin server in this
        # test configuration it should rollback
        tx1.close()
        tx2.close()

        assert not tx1.isOpen()
        assert not tx2.isOpen()
        verify_gtx_closed(gtx1)
        verify_gtx_closed(gtx2)

        remote_conn = create_connection_to_gtx()
        g = traversal().with_(remote_conn)
        assert g.V().count().next() == 0

        drop_graph_check_count(g)

    @pytest.mark.skip(reason="enable after transaction is implemented in HTTP")
    def test_transaction_close_tx_from_parent(self):
        remote_conn = create_connection_to_gtx()
        g = traversal().with_(remote_conn)

        drop_graph_check_count(g)

        tx1 = g.tx()
        tx2 = g.tx()

        # open up two sessions and create stuff
        gtx1 = tx1.begin()
        gtx2 = tx2.begin()

        add_node_validate_transaction_state(g, gtx1, 0, 0, [tx1, tx2])
        add_node_validate_transaction_state(g, gtx2, 0, 0, [tx1, tx2])
        add_node_validate_transaction_state(g, gtx2, 0, 1, [tx1, tx2])
        add_node_validate_transaction_state(g, gtx2, 0, 2, [tx1, tx2])

        # someone gets lazy and doesn't commit/rollback and just calls close() but on the parent
        # DriverRemoteConnection for all the session that were created via tx() - the graph
        # will decide how to treat the transaction, but for neo4j/gremlin server in this
        # test configuration it should rollback
        remote_conn.close()

        assert not tx1.isOpen()
        assert not tx2.isOpen()
        verify_gtx_closed(gtx1)
        verify_gtx_closed(gtx2)

        remote_conn = create_connection_to_gtx()
        g = traversal().with_(remote_conn)
        assert g.V().count().next() == 0

        drop_graph_check_count(g)


def create_connection_to_gtx():
    return DriverRemoteConnection(anonymous_url, 'gtx')


def add_node_validate_transaction_state(g, g_add_to, g_start_count, g_add_to_start_count, tx_verify_list):
    # Add a single node to g_add_to, but not g.
    # Check that vertex count in g is g_start_count and vertex count in g_add_to is g_add_to_start_count + 1.
    g_add_to.add_v("person").property("name", "lyndon").iterate()
    assert g_add_to.V().count().next() == g_add_to_start_count + 1
    assert g.V().count().next() == g_start_count
    verify_tx_state(tx_verify_list, True)


def verify_tx_state(gtx_list, value):
    for tx in gtx_list:
        assert tx.isOpen() == value


def drop_graph_check_count(g):
    g.V().drop().iterate()
    assert g.V().count().next() == 0


def verify_gtx_closed(gtx):
    try:
        # Attempt to add an additional vertex to the transaction. This should throw an exception since it
        # has been rolled back.
        gtx().add_v("failure").iterate()
        assert False
    except Exception as e:
        pass

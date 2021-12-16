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

from gremlin_python.structure.graph import Graph
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.traversal import P
from gremlin_python.process.traversal import Binding, Bindings
from gremlin_python.process.graph_traversal import __


def transactions_disabled():
    return (os.environ['TEST_TRANSACTIONS'] != 'true') if 'TEST_TRANSACTIONS' in os.environ else False


class TestTraversal(object):
    def test_bytecode(self):
        g = traversal().withGraph(Graph())
        bytecode = g.V().out("created").bytecode
        assert 0 == len(bytecode.bindings.keys())
        assert 0 == len(bytecode.source_instructions)
        assert 2 == len(bytecode.step_instructions)
        assert "V" == bytecode.step_instructions[0][0]
        assert "out" == bytecode.step_instructions[1][0]
        assert "created" == bytecode.step_instructions[1][1]
        assert 1 == len(bytecode.step_instructions[0])
        assert 2 == len(bytecode.step_instructions[1])
        ##
        bytecode = g.withSack(1).E().groupCount().by("weight").bytecode
        assert 0 == len(bytecode.bindings.keys())
        assert 1 == len(bytecode.source_instructions)
        assert "withSack" == bytecode.source_instructions[0][0]
        assert 1 == bytecode.source_instructions[0][1]
        assert 3 == len(bytecode.step_instructions)
        assert "E" == bytecode.step_instructions[0][0]
        assert "groupCount" == bytecode.step_instructions[1][0]
        assert "by" == bytecode.step_instructions[2][0]
        assert "weight" == bytecode.step_instructions[2][1]
        assert 1 == len(bytecode.step_instructions[0])
        assert 1 == len(bytecode.step_instructions[1])
        assert 2 == len(bytecode.step_instructions[2])
        ##
        bytecode = g.V(Bindings.of('a', [1, 2, 3])) \
            .out(Bindings.of('b', 'created')) \
            .where(__.in_(Bindings.of('c', 'created'), Bindings.of('d', 'knows')) \
                   .count().is_(Bindings.of('e', P.gt(2)))).bytecode
        assert 5 == len(bytecode.bindings.keys())
        assert [1, 2, 3] == bytecode.bindings['a']
        assert 'created' == bytecode.bindings['b']
        assert 'created' == bytecode.bindings['c']
        assert 'knows' == bytecode.bindings['d']
        assert P.gt(2) == bytecode.bindings['e']
        assert Binding('b', 'created') == bytecode.step_instructions[1][1]
        assert 'binding[b=created]' == str(bytecode.step_instructions[1][1])
        assert isinstance(hash(bytecode.step_instructions[1][1]), int)

    def test_P(self):
        # verify that the order of operations is respected
        assert "and(eq(a),lt(b))" == str(P.eq("a").and_(P.lt("b")))
        assert "and(or(lt(b),gt(c)),neq(d))" == str(P.lt("b").or_(P.gt("c")).and_(P.neq("d")))
        assert "and(or(lt(b),gt(c)),or(neq(d),gte(e)))" == str(
            P.lt("b").or_(P.gt("c")).and_(P.neq("d").or_(P.gte("e"))))

    def test_anonymous_traversal(self):
        bytecode = __.__(1).bytecode
        assert 0 == len(bytecode.bindings.keys())
        assert 0 == len(bytecode.source_instructions)
        assert 1 == len(bytecode.step_instructions)
        assert "inject" == bytecode.step_instructions[0][0]
        assert 1 == bytecode.step_instructions[0][1]
        ##
        bytecode = __.start().bytecode
        assert 0 == len(bytecode.bindings.keys())
        assert 0 == len(bytecode.source_instructions)
        assert 0 == len(bytecode.step_instructions)

    def test_clone_traversal(self):
        g = traversal().withGraph(Graph())
        original = g.V().out("created")
        clone = original.clone().out("knows")
        cloneClone = clone.clone().out("created")

        assert 2 == len(original.bytecode.step_instructions)
        assert 3 == len(clone.bytecode.step_instructions)
        assert 4 == len(cloneClone.bytecode.step_instructions)

        original.has("person", "name", "marko")
        clone.V().out()

        assert 3 == len(original.bytecode.step_instructions)
        assert 5 == len(clone.bytecode.step_instructions)
        assert 4 == len(cloneClone.bytecode.step_instructions)

    def test_no_sugar_for_magic_methods(self):
        g = traversal().withGraph(Graph())

        t = g.V().age
        assert 2 == len(t.bytecode.step_instructions)

        try:
            t = g.V().__len__
            fail("can't do sugar with magic")
        except AttributeError as err:
            assert str(
                err) == 'Python magic methods or keys starting with double underscore cannot be used for Gremlin sugar - prefer values(__len__)'

    def test_enforce_anonymous_child_traversal(self):
        g = traversal().withGraph(Graph())
        g.V(0).addE("self").to(__.V(1))

        try:
            g.V(0).addE("self").to(g.V(1))
            assert False
        except TypeError:
            pass

    @pytest.mark.skipif(transactions_disabled(), reason="Transactions are not enabled.")
    def test_transaction_commit(self, remote_transaction_connection):
        # Start a transaction traversal.
        g = traversal().withRemote(remote_transaction_connection)
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

    @pytest.mark.skipif(transactions_disabled(), reason="Transactions are not enabled.")
    def test_transaction_rollback(self, remote_transaction_connection):
        # Start a transaction traversal.
        g = traversal().withRemote(remote_transaction_connection)
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

    @pytest.mark.skipif(transactions_disabled(), reason="Transactions are not enabled.")
    def test_transaction_no_begin(self, remote_transaction_connection):
        # Start a transaction traversal.
        g = traversal().withRemote(remote_transaction_connection)
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

    @pytest.mark.skipif(transactions_disabled(), reason="Transactions are not enabled.")
    def test_multi_commit_transaction(self, remote_transaction_connection):
        # Start a transaction traversal.
        g = traversal().withRemote(remote_transaction_connection)
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

    @pytest.mark.skipif(transactions_disabled(), reason="Transactions are not enabled.")
    def test_multi_rollback_transaction(self, remote_transaction_connection):
        # Start a transaction traversal.
        g = traversal().withRemote(remote_transaction_connection)
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

    @pytest.mark.skipif(transactions_disabled(), reason="Transactions are not enabled.")
    def test_multi_commit_and_rollback(self, remote_transaction_connection):
        # Start a transaction traversal.
        g = traversal().withRemote(remote_transaction_connection)
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


def add_node_validate_transaction_state(g, g_add_to, g_start_count, g_add_to_start_count, tx_verify_list):
    # Add a single node to g_add_to, but not g.
    # Check that vertex count in g is g_start_count and vertex count in g_add_to is g_add_to_start_count + 1.
    g_add_to.addV("person").property("name", "lyndon").iterate()
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
        gtx().addV("failure").iterate()
        assert False
    except Exception as e:
        pass

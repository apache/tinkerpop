/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class TinkerTransactionGraphTest {

    final Object vid = 100;

    ///// vertex tests
    @Test
    public void shouldCommit() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        gtx.addV().next();
        gtx.addV().next();

        assertEquals(2, (long) gtx.V().count().next());

        countElementsInNewThreadTx(g, 0, 0);

        gtx.tx().commit();

        final GraphTraversalSource gtx3 = g.tx().begin();
        assertEquals(2L, (long) gtx3.V().count().next());

        countElementsInNewThreadTx(g, 2, 0);
    }

    @Test
    public void shouldDeleteVertexOnCommit() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        gtx.addV().property(T.id, vid).iterate();
        gtx.tx().commit();

        assertEquals(1, (long) gtx.V().count().next());

        gtx.V(vid).drop().iterate();
        assertEquals(0, (long) gtx.V().count().next());

        countElementsInNewThreadTx(g, 1, 0);

        gtx.tx().commit();

        assertEquals(0, (long) gtx.V().count().next());

        final GraphTraversalSource gtx3 = g.tx().begin();
        assertEquals(0L, (long) gtx3.V().count().next());

        // should remove unused container
        assertEquals(0, g.getVertices().size());

        countElementsInNewThreadTx(g, 0, 0);
    }

    @Test
    public void shouldDeleteRelatedEdgesOnVertexDelete() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().next();
        final Vertex v2 = gtx.addV().next();
        gtx.addE("tests").from(v1).to(v2).next();

        gtx.tx().commit();

        assertEquals(2L, (long) gtx.V().count().next());
        assertEquals(1L, (long) gtx.E().count().next());

        gtx.V(v1.id()).drop().iterate();
        gtx.tx().commit();

        final GraphTraversalSource gtx3 = g.tx().begin();
        assertEquals(1L, (long) gtx3.V().count().next());
        assertEquals(0L, (long) gtx3.E().count().next());

        countElementsInNewThreadTx(g, 1, 0);
    }

    @Test
    public void shouldHandleConcurrentVertexDelete() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        final TinkerVertex v1 = (TinkerVertex) gtx.addV().next();
        gtx.tx().commit();

        gtx.V(v1.id()).drop().iterate();

        // other tx try to remove same vertex
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();

            gtx2.V(v1.id()).drop().iterate();

            // should be ok
            assertEquals(0, (long) gtx2.V().count().next());
            assertEquals(0, (long) gtx2.E().count().next());

            gtx2.tx().commit();
        });
        thread.start();
        thread.join();

        // try do delete in initial tx
        try {
            gtx.tx().commit();
            fail("should throw TransactionException");
        } catch (TransactionException e) {

        }

        // should remove unused container
        assertEquals(0, g.getVertices().size());

        assertEquals(0, (long) gtx.V().count().next());
        assertEquals(0, (long) gtx.E().count().next());

        countElementsInNewThreadTx(g, 0, 0);
    }

    @Test
    public void shouldChangeVertexProperty() {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        // create vertex with test property
        gtx.addV().property(T.id, vid).property("test", 1).iterate();
        gtx.tx().commit();

        assertEquals(1, gtx.V(vid).values("test").next());

        // change test property
        gtx.V(vid).property("test", 2).iterate();
        gtx.tx().commit();

        // should be only 1 vertex with updated property
        assertEquals(1L, (long) gtx.V().count().next());
        assertEquals(2, gtx.V(vid).values("test").next());
    }

    @Test
    public void shouldHandleConcurrentChangeForVertexProperty() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        gtx.addV().property(T.id, vid).iterate();
        gtx.tx().commit();

        // change test property
        gtx.V(vid).property("test", 1).iterate();

        // change property in other tx
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            gtx2.V(vid).property("test", 2).iterate();
            gtx2.tx().commit();
        });
        thread.start();
        thread.join();

        try {
            gtx.tx().commit();
            fail("should throw TransactionException");
        } catch (TransactionException e) {

        }

        // should be only 1 vertex with updated property
        assertEquals(1L, (long) gtx.V().count().next());
        assertEquals(2, gtx.V(vid).values("test").next());
    }

    @Test
    public void shouldHandleConcurrentChangeForVertexMetaProperty() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        gtx.addV().property(T.id, vid).property("test", 1).iterate();
        gtx.tx().commit();

        // change meta property
        gtx.V(vid).properties("test").property("meta1", "tx1").iterate();

        // change same property in other tx
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            gtx2.V(vid).properties("test").property("meta2", "tx2").iterate();
            gtx2.tx().commit();
        });
        thread.start();
        thread.join();

        try {
            gtx.tx().commit();
            //fail("should throw TransactionException");
        } catch (TransactionException e) {

        }

        // should be only 1 vertex with updated property
        assertEquals(1L, (long) gtx.V().count().next());
        assertEquals("tx2", gtx.V(vid).properties("test").values("meta1", "meta2").next());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenTryToAddVertexWithUsedId() {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        // add vertex
        gtx.addV().property(T.id, vid).iterate();
        gtx.tx().commit();

        // try to add vertex with same id
        gtx.addV().property(T.id, vid).iterate();
        gtx.tx().commit();
    }

    @Test
    public void shouldRollback() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        gtx.addV().next();
        gtx.addV().next();

        assertEquals(2, (long) gtx.V().count().next());

        // test count in second transaction before commit
        countElementsInNewThreadTx(g, 0, 0);

        gtx.tx().rollback();

        final GraphTraversalSource gtx3 = g.tx().begin();
        assertEquals(0L, (long) gtx3.V().count().next());

        countElementsInNewThreadTx(g, 0, 0);
    }

    ///// transaction tests
    @Test
    public void shouldReopenClosedTransaction() {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();
        gtx.addV().next();
        gtx.tx().commit();

        gtx.addV().next();
        gtx.tx().commit();

        final GraphTraversalSource gtx2 = g.tx().begin();
        assertEquals(2L, (long) gtx2.V().count().next());
    }


    ///// Edge tests
    @Test
    public void shouldCommitEdge() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().next();
        final Vertex v2 = gtx.addV().next();
        gtx.addE("tests").from(v1).to(v2).next();

        assertEquals(2, (long) gtx.V().count().next());
        assertEquals(1, (long) gtx.E().count().next());

        countElementsInNewThreadTx(g, 0, 0);

        gtx.tx().commit();

        final GraphTraversalSource gtx3 = g.tx().begin();
        assertEquals(2L, (long) gtx3.V().count().next());
        assertEquals(1L, (long) gtx3.E().count().next());

        countElementsInNewThreadTx(g, 2, 1);
    }

    @Test
    public void shouldRollbackEdge() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().next();
        final Vertex v2 = gtx.addV().next();
        gtx.addE("tests").from(v1).to(v2).next();

        assertEquals(2, (long) gtx.V().count().next());
        assertEquals(1, (long) gtx.E().count().next());

        // test count in second transaction before commit
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            assertEquals(0L, (long) gtx2.V().count().next());
            assertEquals(0L, (long) gtx2.E().count().next());
        });
        thread.start();
        thread.join();

        gtx.tx().rollback();

        final GraphTraversalSource gtx3 = g.tx().begin();
        assertEquals(0L, (long) gtx3.V().count().next());
        assertEquals(0L, (long) gtx3.E().count().next());

        countElementsInNewThreadTx(g, 0, 0);
    }

    @Test
    public void shouldDeleteEdgeOnCommit() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        final TinkerVertex v1 = (TinkerVertex) gtx.addV().next();
        final TinkerVertex v2 = (TinkerVertex) gtx.addV().next();
        gtx.addE("tests").from(v1).to(v2).next();
        gtx.tx().commit();

        assertEquals(2, (long) gtx.V().count().next());
        assertEquals(1, (long) gtx.E().count().next());

        gtx.E().hasLabel("tests").drop().iterate();

        assertEquals(2, (long) gtx.V().count().next());
        assertEquals(0, (long) gtx.E().count().next());

        // test count in other thread transaction
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            assertEquals(2L, (long) gtx2.V().count().next());
            assertEquals(1L, (long) gtx2.E().count().next());
        });
        thread.start();
        thread.join();

        gtx.tx().commit();

        assertEquals(2, (long) gtx.V().count().next());
        assertEquals(0, (long) gtx.E().count().next());

        // should remove reference from edge to parent vertices
        final TinkerVertex v1afterTx = g.getVertices().get(v1.id()).get();
        final TinkerVertex v2afterTx = g.getVertices().get(v2.id()).get();
        assertNull(v1afterTx.inEdges);
        assertEquals(0, v1afterTx.outEdges.get("tests").size());
        assertEquals(0, v2afterTx.inEdges.get("tests").size());
        assertNull(v2afterTx.outEdges);

        // should remove unused container
        assertEquals(0, g.getEdges().size());

        countElementsInNewThreadTx(g, 2, 0);
    }

    @Test
    public void shouldHandleConcurrentChangeForProperty() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        final TinkerVertex v1 = (TinkerVertex) gtx.addV().next();
        final TinkerVertex v2 = (TinkerVertex) gtx.addV().next();
        final TinkerEdge edge = (TinkerEdge) gtx.addE("tests").from(v1).to(v2).next();
        gtx.tx().commit();

        // change test property
        gtx.E(edge.id()).property("test", 1).iterate();

        // change property in other tx
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            gtx2.E(edge.id()).property("test", 2).iterate();
            gtx2.tx().commit();
        });
        thread.start();
        thread.join();

        try {
            gtx.tx().commit();
            fail("should throw TransactionException");
        } catch (TransactionException e) {

        }

        // should be only 1 edge with updated property
        assertEquals(1L, (long) gtx.E(edge.id()).count().next());
        assertEquals(2, gtx.E(edge.id()).values("test").next());
    }

    @Test
    public void shouldHandleConcurrentEdgeDelete() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        final TinkerVertex v1 = (TinkerVertex) gtx.addV().next();
        final TinkerVertex v2 = (TinkerVertex) gtx.addV().next();
        gtx.addE("tests").from(v1).to(v2).next();
        gtx.tx().commit();

        assertEquals(2, (long) gtx.V().count().next());
        assertEquals(1, (long) gtx.E().count().next());

        gtx.E().hasLabel("tests").drop().iterate();

        assertEquals(2, (long) gtx.V().count().next());
        assertEquals(0, (long) gtx.E().count().next());

        // other tx try to remove same edge
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            assertEquals(2L, (long) gtx2.V().count().next());
            assertEquals(1L, (long) gtx2.E().count().next());

            // try to remove edge
            gtx2.E().hasLabel("tests").drop().iterate();

            // should be ok
            assertEquals(2, (long) gtx2.V().count().next());
            assertEquals(0, (long) gtx2.E().count().next());

            gtx2.tx().commit();
        });
        thread.start();
        thread.join();

        // try do delete in initial tx
        try {
            gtx.tx().commit();
            fail("should throw TransactionException");
        } catch (TransactionException e) {

        }

        // should remove unused container
        assertEquals(0, g.getEdges().size());

        assertEquals(2, (long) gtx.V().count().next());
        assertEquals(0, (long) gtx.E().count().next());

        // should remove reference from edge to parent vertices
        final TinkerVertex v1afterTx = g.getVertices().get(v1.id()).get();
        final TinkerVertex v2afterTx = g.getVertices().get(v2.id()).get();
        assertNull(v1afterTx.inEdges);
        assertEquals(0, v1afterTx.outEdges.get("tests").size());
        assertEquals(0, v2afterTx.inEdges.get("tests").size());
        assertNull(v2afterTx.outEdges);

        countElementsInNewThreadTx(g, 2, 0);
    }


    // Mike's test cases

    //tx1 adds a property to v1, tx2 deletes v1
    @Test
    public void shouldHandleAddingPropertyWhenOtherTxDeleteVertex() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        final TinkerVertex v1 = (TinkerVertex) gtx.addV().next();
        gtx.tx().commit();

        // tx1 try to add property
        gtx.V(v1.id()).property("test", 1).iterate();

        // tx2 in same time delete vertex used by tx1
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            gtx2.V(v1.id()).drop().iterate();
            gtx2.tx().commit();
        });
        thread.start();
        thread.join();

        try {
            gtx.tx().commit();
            fail("should throw TransactionException");
        } catch (TransactionException e) {

        }

        assertEquals(0, (long) gtx.V().count().next());
        assertEquals(0, (long) gtx.E().count().next());

        countElementsInNewThreadTx(g, 0, 0);
    }

    @Test
    public void shouldHandleAddingPropertyWhenOtherTxDeleteEdge() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        final TinkerVertex v1 = (TinkerVertex) gtx.addV().next();
        final TinkerVertex v2 = (TinkerVertex) gtx.addV().next();
        final TinkerEdge edge =  (TinkerEdge)gtx.addE("tests").from(v1).to(v2).next();
        gtx.tx().commit();

        // tx1 try to add property
        gtx.E(edge.id()).property("test", 1).iterate();

        // tx2 in same time delete edge used by tx1
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            gtx2.E(edge.id()).drop().iterate();
            gtx2.tx().commit();
        });
        thread.start();
        thread.join();

        try {
            gtx.tx().commit();
            fail("should throw TransactionException");
        } catch (TransactionException e) {

        }

        assertEquals(2, (long) gtx.V().count().next());
        assertEquals(0, (long) gtx.E().count().next());

        countElementsInNewThreadTx(g, 2, 0);
    }

    // tx1 adds an edge from v1 to v2, tx2 deletes v1 or v2
    @Test
    public void shouldHandleAddingEdgeWhenOtherTxDeleteVertex() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        final TinkerVertex v1 = (TinkerVertex) gtx.addV().next();
        final TinkerVertex v2 = (TinkerVertex) gtx.addV().next();
        gtx.tx().commit();

        // tx1 try to add Edge
        gtx.addE("test").from(v1).to(v2).iterate();

        // tx2 in same time delete one of vertices used by tx1
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            gtx2.V(v1.id()).drop().iterate();
            gtx2.tx().commit();
        });
        thread.start();
        thread.join();

        try {
            gtx.tx().commit();
            fail("should throw TransactionException");
        } catch (TransactionException e) {

        }

        assertEquals(1, (long) gtx.V().count().next());
        assertEquals(0, (long) gtx.E().count().next());

        countElementsInNewThreadTx(g, 1, 0);
    }

    // tx1 adds a new vertex v1, tx2 adds the same vertex
    @Test
    public void shouldHandleAddingSameVertexInDifferentTx() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        gtx.addV().property(T.id, vid).iterate();

        // tx2 in same time add vertex with same id
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            gtx2.addV().property(T.id, vid).iterate();
            gtx2.tx().commit();
        });
        thread.start();
        thread.join();

        try {
            gtx.tx().commit();
            fail("should throw TransactionException");
        } catch (TransactionException e) {

        }

        assertEquals(1, (long) gtx.V().count().next());
        assertEquals(0, (long) gtx.E().count().next());

        countElementsInNewThreadTx(g, 1, 0);
    }

    // tx1 changes a Cardinality.single property to x, tx2 changes the same Cardinality.single property to y
    // tested in shouldHandleConcurrentChangeForVertexProperty and shouldHandleConcurrentChangeForProperty

    // tx1 removes vertex v1, tx2 removes the same vertex
    // tested in shouldHandleConcurrentVertexDelete and shouldHandleConcurrentEdgeDelete

    // tx1 adds vertex v1, tx2 removes vertex v1
    @Test
    public void shouldHandleAddingVertexWhenOtherTxTryToDeleteSameVertex() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        gtx.addV().property(T.id, vid).iterate();

        // tx2 in same time try to delete same vertex
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            gtx2.V(vid).drop().iterate();

            assertEquals(0, (long) gtx.V().count().next());
            assertEquals(0, (long) gtx.E().count().next());
            gtx2.tx().commit();
        });
        thread.start();
        thread.join();

        gtx.tx().commit();

        assertEquals(1, (long) gtx.V().count().next());
        assertEquals(0, (long) gtx.E().count().next());

        countElementsInNewThreadTx(g, 1, 0);
    }

    // index tests for vertex

    @Test
    public void shouldCreateIndexForNewVertex() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        g.createIndex("test-property", Vertex.class);

        final GraphTraversalSource gtx = g.tx().begin();

        gtx.addV().property(T.id, vid).property("test-property", 1).iterate();
        gtx.addV().property("test-property", 2).iterate();

        assertEquals(1L, (long) gtx.V().has("test-property", 1).count().next());
        assertEquals(vid, gtx.V().has("test-property", 1).id().next());
        assertEquals(0L, (long) gtx.V().has("test-property", 3).count().next());
        assertEquals(2L, (long) gtx.V().count().next());

        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            assertEquals(0L, (long) gtx2.V().has("test-property", 1).count().next());
        });
        thread.start();
        thread.join();

        gtx.tx().commit();

        final GraphTraversalSource gtx3 = g.tx().begin();
        assertEquals(1L, (long) gtx3.V().has("test-property", 1).count().next());
        assertEquals(vid, gtx3.V().has("test-property", 1).id().next());
        assertEquals(0L, (long) gtx3.V().has("test-property", 3).count().next());
        assertEquals(2L, (long) gtx3.V().count().next());

        countElementsInNewThreadTx(g, 2, 0);

        final Map<Object, Set<TinkerElementContainer<?>>> index =
                (Map<Object, Set<TinkerElementContainer<?>>>) ((TinkerTransactionalIndex) g.vertexIndex).index.get("test-property");
        assertNotNull(index);
        // should be only vertex vid in set
        assertEquals(1, index.get(1).size());
        assertEquals(vid, index.get(1).iterator().next().get().id());
    }

    @Test
    public void shouldCreateIndexForNullVertexProperty() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        g.createIndex("test-property", Vertex.class);
        g.allowNullPropertyValues = true;
        final Integer nullValue = null;

        final GraphTraversalSource gtx = g.tx().begin();

        gtx.addV().property(T.id, vid).property("test-property", nullValue).iterate();
        gtx.addV().property("test-property", 2).iterate();

        assertEquals(1L, (long) gtx.V().has("test-property", nullValue).count().next());
        assertEquals(vid, gtx.V().has("test-property", nullValue).id().next());
        assertEquals(0L, (long) gtx.V().has("test-property", 1).count().next());
        assertEquals(2L, (long) gtx.V().count().next());

        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            assertEquals(0L, (long) gtx2.V().has("test-property", nullValue).count().next());
        });
        thread.start();
        thread.join();

        gtx.tx().commit();

        final GraphTraversalSource gtx3 = g.tx().begin();
        assertEquals(1L, (long) gtx3.V().has("test-property", nullValue).count().next());
        assertEquals(vid, gtx3.V().has("test-property", nullValue).id().next());
        assertEquals(0L, (long) gtx3.V().has("test-property", 1).count().next());
        assertEquals(2L, (long) gtx3.V().count().next());

        countElementsInNewThreadTx(g, 2, 0);

        final Map<Object, Set<TinkerElementContainer<?>>> index =
                (Map<Object, Set<TinkerElementContainer<?>>>) ((TinkerTransactionalIndex) g.vertexIndex).index.get("test-property");
        assertNotNull(index);
        // should be only vertex vid in set
        assertEquals(1, index.get(AbstractTinkerIndex.IndexedNull.instance()).size());
        assertEquals(vid, index.get(AbstractTinkerIndex.IndexedNull.instance()).iterator().next().get().id());
    }

    @Test
    public void shouldRemoveIndexForRemovedVertex() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        g.createIndex("test-property", Vertex.class);

        final GraphTraversalSource gtx = g.tx().begin();

        gtx.addV().property(T.id, vid).property("test-property", 1).iterate();

        gtx.tx().commit();

        gtx.V(vid).drop().iterate();

        assertEquals(0L, (long) gtx.V().has("test-property", 1).count().next());
        assertEquals(0L, (long) gtx.V().count().next());

        // in another thread elements exists before commit
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            assertEquals(1L, (long) gtx2.V().has("test-property", 1).count().next());
        });
        thread.start();
        thread.join();

        gtx.tx().commit();

        final GraphTraversalSource gtx3 = g.tx().begin();
        assertEquals(0L, (long) gtx3.V().has("test-property", 1).count().next());
        assertEquals(0L, (long) gtx3.V().count().next());

        countElementsInNewThreadTx(g, 0, 0);

        final Map<Object, Set<TinkerElementContainer<?>>> index =
                (Map<Object, Set<TinkerElementContainer<?>>>) ((TinkerTransactionalIndex) g.vertexIndex).index.get("test-property");
        assertNotNull(index);
        // should be only vertex vid in set
        assertEquals(0, index.size());
     }

    @Test
    public void shouldRemoveIndexForRemovedVertexProperty() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        g.createIndex("test-property", Vertex.class);

        final GraphTraversalSource gtx = g.tx().begin();

        gtx.addV().property(T.id, vid).property("test-property", 1).iterate();

        gtx.tx().commit();

        gtx.V(vid).properties("test-property").drop().iterate();

        assertEquals(0L, (long) gtx.V().has("test-property", 1).count().next());
        assertEquals(1L, (long) gtx.V().count().next());

        // in another thread elements exists before commit
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            assertEquals(1L, (long) gtx2.V().has("test-property", 1).count().next());
        });
        thread.start();
        thread.join();

        gtx.tx().commit();

        final GraphTraversalSource gtx3 = g.tx().begin();
        assertEquals(0L, (long) gtx3.V().has("test-property", 1).count().next());
        assertEquals(1L, (long) gtx3.V().count().next());

        countElementsInNewThreadTx(g, 1, 0);

        final Map<Object, Set<TinkerElementContainer<?>>> index =
                (Map<Object, Set<TinkerElementContainer<?>>>) ((TinkerTransactionalIndex) g.vertexIndex).index.get("test-property");
        assertNotNull(index);
        // should be only vertex vid in set
        assertEquals(0, index.size());
    }

    // index tests for edge

    @Test
    public void shouldCreateIndexForNewEdge() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        g.createIndex("test-property", Edge.class);

        final GraphTraversalSource gtx = g.tx().begin();

        final TinkerVertex v1 = (TinkerVertex) gtx.addV().next();
        final TinkerVertex v2 = (TinkerVertex) gtx.addV().next();
        final TinkerEdge edge = (TinkerEdge) gtx.addE("tests").property("test-property", 1).from(v1).to(v2).next();

        assertEquals(1L, (long) gtx.E().has("test-property", 1).count().next());
        assertEquals(1L, (long) gtx.E().count().next());

        // in another thread index not updated yet
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            assertEquals(0L, (long) gtx2.E().has("test-property", 1).count().next());
            assertEquals(0L, (long) gtx2.E().count().next());
        });
        thread.start();
        thread.join();

        gtx.tx().commit();

        final GraphTraversalSource gtx3 = g.tx().begin();
        assertEquals(1L, (long) gtx.E().has("test-property", 1).count().next());
        assertEquals(1L, (long) gtx.E().count().next());

        countElementsInNewThreadTx(g, 2, 1);

        final Map<Object, Set<TinkerElementContainer<?>>> index =
                (Map<Object, Set<TinkerElementContainer<?>>>) ((TinkerTransactionalIndex) g.edgeIndex).index.get("test-property");
        assertNotNull(index);
        // should be only vertex vid in set
        assertEquals(1, index.size());
        assertEquals(1, index.get(1).size());
        assertEquals(edge.id(), index.get(1).iterator().next().get().id());
    }

    @Test
    public void shouldChangeIndexForChangedEdge() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        g.createIndex("test-property", Edge.class);

        final GraphTraversalSource gtx = g.tx().begin();

        final TinkerVertex v1 = (TinkerVertex) gtx.addV().next();
        final TinkerVertex v2 = (TinkerVertex) gtx.addV().next();
        final TinkerEdge edge = (TinkerEdge) gtx.addE("tests").property("test-property", 1).from(v1).to(v2).next();
        gtx.tx().commit();

        gtx.E(edge.id()).property("test-property", 2).iterate();

        assertEquals(0L, (long) gtx.E().has("test-property", 1).count().next());
        assertEquals(1L, (long) gtx.E().has("test-property", 2).count().next());
        assertEquals(1L, (long) gtx.E().count().next());

        // in another thread index not updated yet
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            assertEquals(1L, (long) gtx2.E().has("test-property", 1).count().next());
            assertEquals(0L, (long) gtx2.E().has("test-property", 2).count().next());
            assertEquals(1L, (long) gtx2.E().count().next());
        });
        thread.start();
        thread.join();

        gtx.tx().commit();

        final GraphTraversalSource gtx3 = g.tx().begin();
        assertEquals(0L, (long) gtx.E().has("test-property", 1).count().next());
        assertEquals(1L, (long) gtx.E().has("test-property", 2).count().next());
        assertEquals(1L, (long) gtx.E().count().next());

        countElementsInNewThreadTx(g, 2, 1);

        final Map<Object, Set<TinkerElementContainer<?>>> index =
                (Map<Object, Set<TinkerElementContainer<?>>>) ((TinkerTransactionalIndex) g.edgeIndex).index.get("test-property");
        assertNotNull(index);
        // should be only vertex vid in set
        assertEquals(1, index.size());
        assertNull(index.get(1));
        assertEquals(1, index.get(2).size());
        assertEquals(edge.id(), index.get(2).iterator().next().get().id());
    }

    @Test
    public void shouldRemoveIndexForRemovedEdge() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        g.createIndex("test-property", Edge.class);

        final GraphTraversalSource gtx = g.tx().begin();

        final TinkerVertex v1 = (TinkerVertex) gtx.addV().next();
        final TinkerVertex v2 = (TinkerVertex) gtx.addV().next();
        final TinkerEdge edge = (TinkerEdge) gtx.addE("tests").property("test-property", 1).from(v1).to(v2).next();
        gtx.tx().commit();

        gtx.E(edge.id()).drop().iterate();

        assertEquals(0L, (long) gtx.E().has("test-property", 1).count().next());
        assertEquals(0L, (long) gtx.E().count().next());

        // in another thread elements exists before commit
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            assertEquals(1L, (long) gtx2.E().has("test-property", 1).count().next());
        });
        thread.start();
        thread.join();

        gtx.tx().commit();

        final GraphTraversalSource gtx3 = g.tx().begin();
        assertEquals(0L, (long) gtx3.E().has("test-property", 1).count().next());
        assertEquals(0L, (long) gtx3.E().count().next());

        countElementsInNewThreadTx(g, 2, 0);

        final Map<Object, Set<TinkerElementContainer<?>>> index =
                (Map<Object, Set<TinkerElementContainer<?>>>) ((TinkerTransactionalIndex) g.edgeIndex).index.get("test-property");
        assertNotNull(index);
        // should be only vertex vid in set
        assertEquals(0, index.size());
    }

    @Test
    public void shouldCreateIndexForNullProperty() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        g.createIndex("test-property", Edge.class);
        g.allowNullPropertyValues = true;
        final Integer nullValue = null;

        final GraphTraversalSource gtx = g.tx().begin();

        final TinkerVertex v1 = (TinkerVertex) gtx.addV().next();
        final TinkerVertex v2 = (TinkerVertex) gtx.addV().next();
        final TinkerEdge edge = (TinkerEdge) gtx.addE("tests").property("test-property", nullValue).
                from(v1).to(v2).next();

        assertEquals(1L, (long) gtx.E().has("test-property", nullValue).count().next());
        assertEquals(1L, (long) gtx.E().count().next());

        // in another thread index not updated yet
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            assertEquals(0L, (long) gtx2.E().has("test-property", nullValue).count().next());
            assertEquals(0L, (long) gtx2.E().count().next());
        });
        thread.start();
        thread.join();

        gtx.tx().commit();

        final GraphTraversalSource gtx3 = g.tx().begin();
        assertEquals(1L, (long) gtx3.E().has("test-property", nullValue).count().next());
        assertEquals(1L, (long) gtx3.E().count().next());

        countElementsInNewThreadTx(g, 2, 1);

        final Map<Object, Set<TinkerElementContainer<?>>> index =
                (Map<Object, Set<TinkerElementContainer<?>>>) ((TinkerTransactionalIndex) g.edgeIndex).index.get("test-property");
        assertNotNull(index);
        // should be only vertex vid in set
        assertEquals(1, index.size());
        assertEquals(1, index.get(AbstractTinkerIndex.IndexedNull.instance()).size());
        assertEquals(edge.id(), index.get(AbstractTinkerIndex.IndexedNull.instance()).iterator().next().get().id());
    }

    @Test
    public void shouldRemoveIndexForNullProperty() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        g.createIndex("test-property", Edge.class);
        final Integer nullValue = null;

        final GraphTraversalSource gtx = g.tx().begin();

        // create initial edge with indexed property
        final TinkerVertex v1 = (TinkerVertex) gtx.addV().next();
        final TinkerVertex v2 = (TinkerVertex) gtx.addV().next();
        final TinkerEdge edge = (TinkerEdge) gtx.addE("tests").property("test-property", 1).
                from(v1).to(v2).next();

        gtx.tx().commit();

        // remove property from index
        gtx.E(edge.id()).property("test-property", nullValue).iterate();

        assertEquals(0L, (long) gtx.E().has("test-property", 1).count().next());
        assertEquals(1L, (long) gtx.E().count().next());

        // in another thread index not updated yet
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            assertEquals(1L, (long) gtx2.E().has("test-property", 1).count().next());
            assertEquals(1L, (long) gtx2.E().count().next());
        });
        thread.start();
        thread.join();

        gtx.tx().commit();

        final GraphTraversalSource gtx3 = g.tx().begin();
        assertEquals(0L, (long) gtx3.E().has("test-property", nullValue).count().next());
        assertEquals(1L, (long) gtx3.E().count().next());

        countElementsInNewThreadTx(g, 2, 1);

        final Map<Object, Set<TinkerElementContainer<?>>> index =
                (Map<Object, Set<TinkerElementContainer<?>>>) ((TinkerTransactionalIndex) g.edgeIndex).index.get("test-property");
        assertNotNull(index);
        // should be only vertex vid in set
        assertEquals(0, index.size());
    }

    // todo: move to new file or remove?
    @Test
    public void vertexCloneTest() {
        final TinkerVertex vertex = new TinkerVertex(123, "label", TinkerTransactionGraph.open());
        final TinkerVertexProperty vp = new TinkerVertexProperty(vertex, "test", "qq");
        vertex.properties = new HashMap<>();
        vertex.properties.put("test", new ArrayList<>());
        vertex.properties.get("test").add(vp);

        final TinkerVertex copy = (TinkerVertex) vertex.clone();
        vertex.properties.get("test").remove(vp);
        assertEquals(1, copy.properties.get("test").size());
    }

    @Test
    public void edgeCloneTest() {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final TinkerVertex v1 = new TinkerVertex(1, "label", g);
        final TinkerVertex v2 = new TinkerVertex(2, "label", g);
        final TinkerEdge edge = new TinkerEdge(3, v1, "label", v2);
        final TinkerProperty property = new TinkerProperty(edge, "test", "qq");
        edge.properties = new HashMap<>();
        edge.properties.put(property.key(), property);

        final TinkerEdge copy = (TinkerEdge) edge.clone();
        edge.properties.remove(property.key());
        assertEquals(1, copy.properties.size());
    }

    // utility methods

    private void countElementsInNewThreadTx(final TinkerTransactionGraph g, final long verticesCount, final long edgesCount) throws InterruptedException {
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx = g.tx().begin();
            assertEquals(verticesCount, (long) gtx.V().count().next());
            assertEquals(edgesCount, (long) gtx.E().count().next());
        });
        thread.start();
        thread.join();
    }
}

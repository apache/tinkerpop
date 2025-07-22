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

import java.util.List;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TinkerTransactionGraphTest {

    private static final String PROPERTY_NAME = "name";
    final Object vid = 100;

    ///// vertex tests

    @Test
    public void shouldReturnSameVertexInstanceInsideTransaction() {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex vertex = gtx.addV().property(T.id, vid).next();
        assertEquals(vertex, gtx.V().next());
        assertEquals(vertex, gtx.V(vid).next());
        assertEquals(vertex, gtx.V().toList().get(0));
    }

    @Test
    public void shouldDeleteVertexOnCommit() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        gtx.addV().iterate();
        gtx.tx().commit();

        assertEquals(1, (long) gtx.V().count().next());

        gtx.V().drop().iterate();
        assertEquals(0, (long) gtx.V().count().next());

        countElementsInNewThreadTx(g, 1, 0);

        gtx.tx().commit();

        assertEquals(0, (long) gtx.V().count().next());

        final GraphTraversalSource gtx3 = g.tx().begin();
        assertEquals(0L, (long) gtx3.V().count().next());

        countElementsInNewThreadTx(g, 0, 0);
    }

    @Test
    public void shouldDeleteCreatedVertexOnCommit() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        // create and drop Vertex in same transaction
        gtx.addV().property(T.id, vid).iterate();
        gtx.V(vid).drop().iterate();

        assertEquals(0, (long) gtx.V().count().next());
        countElementsInNewThreadTx(g, 0, 0);

        gtx.tx().commit();

        assertEquals(0, (long) gtx.V().count().next());
        countElementsInNewThreadTx(g, 0, 0);
        // should remove unused container
        assertEquals(0, g.getVertices().size());
    }

    @Test
    public void shouldCleanUpVertexContainerOnCommit() {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        gtx.addV().property(T.id, vid).iterate();
        gtx.tx().commit();

        // read vertex in new TX
        final GraphTraversalSource gtx2 = g.tx().begin();
        final Vertex v = gtx.V(vid).next();
        gtx.V(vid).iterate();
        // should be the same instance
        assertEquals(v, g.getVertices().get(vid).getModified());
        // commit without modifications
        gtx.tx().commit();

        // should clean up unused value in container
        assertEquals(1, g.getVertices().size());
        assertNull(g.getVertices().get(vid).getModified());
    }

    @Test
    public void shouldCleanUpVertexContainerOnRollback() {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        gtx.addV().property(T.id, vid).iterate();
        gtx.tx().commit();

        // read vertex in new TX
        final GraphTraversalSource gtx2 = g.tx().begin();
        final Vertex v = gtx.V(vid).next();
        gtx.V(vid).iterate();
        // should be the same instance
        assertEquals(v, g.getVertices().get(vid).getModified());
        // rollback without modifications
        gtx.tx().rollback();

        // should clean up unused value in container
        assertEquals(1, g.getVertices().size());
        assertNull(g.getVertices().get(vid).getModified());
    }

    @Test
    public void shouldDeleteUnusedVertexContainerOnCommit() {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        gtx.addV().property(T.id, vid).iterate();
        gtx.tx().commit();

        gtx.V(vid).drop().iterate();
        gtx.tx().commit();

        // should remove unused container
        assertEquals(0, g.getVertices().size());
    }

    @Test
    public void shouldDeleteUnusedVertexContainerOnRollback() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();

        final GraphTraversalSource gtx = g.tx().begin();

        gtx.addV().property(T.id, vid).iterate();
        gtx.tx().commit();

        gtx.V(vid).properties("test-property").iterate();
        // 1 container for existing vertex
        assertEquals(1, g.getVertices().size());

        // other tx try to add new vertices and modify existing
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();

            gtx2.addV().addV().iterate();
            gtx.V(vid).properties("test-property").iterate();

            // 1 container for existing vertex + 2 for new ones
            assertEquals(3, g.getVertices().size());

            assertEquals(3, (long) gtx2.V().count().next());

            gtx2.tx().rollback();
            // containers for new vertices should be removed
            assertEquals(1, g.getVertices().size());
        });
        thread.start();
        thread.join();

        gtx.tx().rollback();

        // should keep container because vertex not created in this transaction
        assertEquals(1, g.getVertices().size());
    }

    @Test
    public void shouldDeleteUnusedVertexContainerOnConcurrentVertexDelete() throws InterruptedException {
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
    }

    ///// Edge tests

    @Test
    public void shouldRemoveContainerAndReferenceFromVertexOnDeleteEdge() throws InterruptedException {
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
        assertNull(v1afterTx.inEdgesId);
        assertEquals(0, v1afterTx.outEdgesId.get("tests").size());
        assertEquals(0, v2afterTx.inEdgesId.get("tests").size());
        assertNull(v2afterTx.outEdgesId);

        // should remove unused container
        assertEquals(0, g.getEdges().size());

        countElementsInNewThreadTx(g, 2, 0);
    }

    @Test
    public void shouldRemoveContainerAndReferenceFromVertexOnConcurrentDeleteEdge() throws InterruptedException {
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
        assertNull(v1afterTx.inEdgesId);
        assertEquals(0, v1afterTx.outEdgesId.get("tests").size());
        assertEquals(0, v2afterTx.inEdgesId.get("tests").size());
        assertNull(v2afterTx.outEdgesId);

        countElementsInNewThreadTx(g, 2, 0);
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

    // tests for cloning elements

    @Test
    public void vertexCloneTest() {
        final TinkerVertex vertex = new TinkerVertex(123, "label", TinkerTransactionGraph.open());
        final TinkerVertexProperty vp = new TinkerVertexProperty(vertex, "test", "qq");
        final TinkerEdge edge = new TinkerEdge(1, vertex, "label", vertex);
        vertex.properties = new ConcurrentHashMap<>();
        vertex.properties.put("test", new ArrayList<>());
        vertex.properties.get("test").add(vp);
        vertex.inEdgesId = new ConcurrentHashMap<>();
        vertex.inEdgesId.put("label", ConcurrentHashMap.newKeySet());
        vertex.inEdgesId.get("label").add(edge);

        final TinkerVertex copy = (TinkerVertex) vertex.clone();
        vertex.properties.get("test").remove(vp);
        assertEquals(1, copy.properties.get("test").size());
        vertex.inEdgesId.get("label").remove(edge);
        assertEquals(1, copy.inEdgesId.get("label").size());
    }

    @Test
    public void edgeCloneTest() {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final TinkerVertex v1 = (TinkerVertex) g.addVertex();
        final TinkerVertex v2 = (TinkerVertex) g.addVertex();
        final TinkerEdge edge = new TinkerEdge(3, v1, "label", v2);
        final TinkerProperty property = new TinkerProperty(edge, "test", "qq");
        edge.properties = new ConcurrentHashMap<>();
        edge.properties.put(property.key(), property);

        final TinkerEdge copy = (TinkerEdge) edge.clone();
        edge.properties.remove(property.key());
        assertEquals(1, copy.properties.size());
    }

    // utility methods

    private void countElementsInNewThreadTx(final TinkerTransactionGraph g, final long verticesCount, final long edgesCount) throws InterruptedException {
        final AtomicLong vCount = new AtomicLong(-1);
        final AtomicLong eCount = new AtomicLong(-1);
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx = g.tx().begin();
            vCount.set(gtx.V().count().next());
            eCount.set(gtx.E().count().next());
        });
        thread.start();
        thread.join();

        assertEquals(verticesCount, vCount.get());
        assertEquals(edgesCount, eCount.get());
    }

    @Test
    public void shouldNotAllowDirtyReadsOfVertexForReadOnlyTransaction() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        gtx.addV().next();

        final AtomicLong gtx2Read = new AtomicLong(-1);
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();

            gtx2Read.set(gtx2.V().count().next());
        });
        thread.start();
        thread.join();

        assertEquals(0, gtx2Read.get());
        assertEquals(1, (long) gtx.V().count().next());
    }

    @Test
    public void shouldNotAllowDirtyReadsOfVertexDropForReadOnlyTransaction() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        gtx.addV().next();
        gtx.tx().commit();

        gtx.V().drop().iterate();

        final AtomicLong gtx2Read = new AtomicLong(-1);
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();

            gtx2Read.set(gtx2.V().count().next());
        });
        thread.start();
        thread.join();

        assertEquals(1, gtx2Read.get());
        assertEquals(0, (long) gtx.V().count().next());
    }

    @Test
    public void shouldNotAllowDirtyReadsOfEdgeForReadOnlyTransaction() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().next();
        final Vertex v2 = gtx.addV().next();
        gtx.addE("test").from(v1).to(v2).iterate();

        final AtomicLong gtx2Read = new AtomicLong(-1);
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();

            gtx2Read.set(gtx2.E().count().next());
        });
        thread.start();
        thread.join();

        assertEquals(0, gtx2Read.get());
        assertEquals(1, (long) gtx.E().count().next());
    }

    @Test
    public void shouldNotAllowDirtyReadsOfEdgeDropForReadOnlyTransaction() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().next();
        final Vertex v2 = gtx.addV().next();
        gtx.addE("test").from(v1).to(v2).iterate();
        gtx.tx().commit();

        gtx.E().drop().iterate();

        final AtomicLong gtx2Read = new AtomicLong(-1);
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();

            gtx2Read.set(gtx2.E().count().next());
        });
        thread.start();
        thread.join();

        assertEquals(1, gtx2Read.get());
        assertEquals(0, (long) gtx.E().count().next());
    }

    @Test
    public void shouldNotAllowDirtyReadsOfVertexPropertyAddForReadOnlyTransaction() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        gtx.addV().property("a", 1).iterate();
        gtx.tx().commit();

        gtx.V().property("b", 2).iterate();

        final AtomicLong gtx2Read = new AtomicLong(-1);
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();

            gtx2Read.set(gtx2.V().properties().count().next());
        });
        thread.start();
        thread.join();

        assertEquals(1, gtx2Read.get());
        assertEquals(2, (long) gtx.V().properties().count().next());
    }

    @Test
    public void shouldNotAllowDirtyReadsOfVertexPropertyDropForReadOnlyTransaction() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        gtx.addV().property("a", 1).iterate();
        gtx.tx().commit();

        gtx.V().properties().drop().iterate();

        final AtomicLong gtx2Read = new AtomicLong(-1);
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();

            gtx2Read.set(gtx2.V().properties().count().next());
        });
        thread.start();
        thread.join();

        assertEquals(1, gtx2Read.get());
        assertEquals(0, (long) gtx.V().properties().count().next());
    }

    @Test
    public void shouldNotAllowDirtyReadsOfVertexPropertyUpdateForReadOnlyTransaction() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        gtx.addV().property("a", 1L).iterate();
        gtx.tx().commit();

        gtx.V().property("a", 2L).iterate();

        final AtomicLong gtx2Read = new AtomicLong(-1);
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();

            gtx2Read.set((long) gtx2.V().values("a").next());
        });
        thread.start();
        thread.join();

        assertEquals(1, gtx2Read.get());
        assertEquals(2, (long) gtx.V().values("a").next());
    }

    @Test
    public void shouldNotAllowDirtyReadsOfEdgePropertyAddForReadOnlyTransaction() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().next();
        final Vertex v2 = gtx.addV().next();
        gtx.addE("test").from(v1).to(v2).iterate();
        gtx.tx().commit();

        gtx.E().property("a", 1).iterate();

        final AtomicLong gtx2Read = new AtomicLong(-1);
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();

            gtx2Read.set(gtx2.E().properties().count().next());
        });
        thread.start();
        thread.join();

        assertEquals(0, gtx2Read.get());
        assertEquals(1, (long) gtx.E().properties().count().next());
    }

    @Test
    public void shouldNotAllowDirtyReadsOfEdgePropertyDropForReadOnlyTransaction() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().next();
        final Vertex v2 = gtx.addV().next();
        gtx.addE("test").from(v1).to(v2).property("a", 1).iterate();
        gtx.tx().commit();

        gtx.E().properties().drop().iterate();

        final AtomicLong gtx2Read = new AtomicLong(-1);
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();

            gtx2Read.set(gtx2.E().properties().count().next());
        });
        thread.start();
        thread.join();

        assertEquals(1, gtx2Read.get());
        assertEquals(0, (long) gtx.E().properties().count().next());
    }

    @Test
    public void shouldNotAllowDirtyReadsOfEdgePropertyUpdateForReadOnlyTransaction() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().next();
        final Vertex v2 = gtx.addV().next();
        gtx.addE("test").from(v1).to(v2).property("a", 1L).iterate();
        gtx.tx().commit();

        gtx.E().property("a", 2L).iterate();

        final AtomicLong gtx2Read = new AtomicLong(-1);
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();

            gtx2Read.set((long) gtx2.E().values("a").next());
        });
        thread.start();
        thread.join();

        assertEquals(1, gtx2Read.get());
        assertEquals(2, (long) gtx.E().values("a").next());
    }

    @Test
    public void shouldHandleAddingPropertyWhenOtherTxDeleteEdge() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().next();
        final Vertex v2 = gtx.addV().next();
        final Edge edge = gtx.addE("tests").from(v1).to(v2).next();
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

    //tx1 adds a property to v1, tx2 deletes v1
    @Test
    public void shouldHandleAddingPropertyWhenOtherTxDeleteVertex() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().next();
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
    public void shouldDeleteEdgeOnCommit() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().next();
        final Vertex v2 = gtx.addV().next();
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

        countElementsInNewThreadTx(g, 2, 0);
    }

    @Test
    public void shouldHandleConcurrentChangeForProperty() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().next();
        final Vertex v2 = gtx.addV().next();
        final Edge edge = gtx.addE("tests").from(v1).to(v2).next();
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

    // tx1 adds an edge from v1 to v2, tx2 deletes v1 or v2
    @Test
    public void shouldHandleAddingEdgeWhenOtherTxDeleteVertex() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().next();
        final Vertex v2 = gtx.addV().next();
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

    @Test
    public void shouldHandleConcurrentChangeForVertexProperty() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().next();
        gtx.tx().commit();

        // change test property
        gtx.V(v1.id()).property("test", 1).iterate();

        // change property in other tx
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            gtx2.V(v1.id()).property("test", 2).iterate();
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
        assertEquals(2, gtx.V(v1.id()).values("test").next());
    }

    @Test
    public void shouldHandleConcurrentDeleteEdge() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().next();
        final Vertex v2 = gtx.addV().next();
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

        assertEquals(2, (long) gtx.V().count().next());
        assertEquals(0, (long) gtx.E().count().next());

        countElementsInNewThreadTx(g, 2, 0);
    }

    // tx1 adds a new vertex v1, tx2 adds the same vertex
    @Test
    public void shouldHandleAddingSameVertexInDifferentTx() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().next();

        // tx2 in same time add vertex with same id
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            gtx2.addV().property(T.id, v1.id()).iterate();
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

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenTryToAddVertexWithUsedId() {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        // add vertex
        final Vertex v1 = gtx.addV().next();
        gtx.tx().commit();

        // try to add vertex with same id
        gtx.addV().property(T.id, v1.id()).iterate();
        gtx.tx().commit();
    }

    @Test
    public void shouldHandleConcurrentVertexDelete() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().next();
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
        assertEquals(0, (long) gtx.V().count().next());
        assertEquals(0, (long) gtx.E().count().next());

        countElementsInNewThreadTx(g, 0, 0);
    }

    @Test
    public void shouldHandleConcurrentChangeForVertexMetaProperty() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().property("test", 1).next();
        gtx.tx().commit();

        // change meta property
        gtx.V(v1.id()).properties("test").property("meta1", "tx1").iterate();

        // change same property in other tx
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            gtx2.V(v1.id()).properties("test").property("meta2", "tx2").iterate();
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
        assertEquals("tx2", gtx.V(v1.id()).properties("test").values("meta1", "meta2").next());
    }

    @Test
    public void shouldReopenClosedTransaction() {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();
        gtx.addV().iterate();
        gtx.tx().commit();

        gtx.addV().iterate();
        assertTrue(gtx.tx().isOpen());
        gtx.tx().commit();

        final GraphTraversalSource gtx2 = g.tx().begin();
        assertEquals(2L, (long) gtx2.V().count().next());
    }

    @Test
    public void shouldHandleAddingPropertyWhenOtherTxAttemptsDeleteThenRollsback() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().next();
        gtx.tx().commit();

        // tx1 try to add property
        gtx.V(v1.id()).property("test", 1).iterate();

        // tx2 in same time delete vertex used by tx1
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            gtx2.V(v1.id()).drop().iterate();
            gtx2.tx().rollback();
        });
        thread.start();
        thread.join();

        gtx.tx().commit();

        assertEquals(1, (long) gtx.V().count().next());
        assertEquals(1, gtx.V(v1.id()).values("test").next());

        countElementsInNewThreadTx(g, 1, 0);
    }

    // tx1 adds vertex v1, tx2 removes vertex v1
    @Test
    public void shouldHandleAddingVertexWhenOtherTxTryToDeleteSameVertex() throws InterruptedException {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().next();

        // tx2 in same time try to delete same vertex
        final Thread thread = new Thread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            gtx2.V(v1.id()).drop().iterate();

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

    @Test
    public void shouldHandleSequenceOfCreateReadDeleteCreateSameVertex() {
        final TinkerTransactionGraph graph = TinkerTransactionGraph.open();
        final GraphTraversalSource g = graph.traversal();

        g.addV().property(T.id, 1).next();
        graph.tx().commit();
        g.V().next();
        graph.tx().commit();
        g.V().drop().iterate();
        graph.tx().commit();

        assertEquals(false, graph.hasVertex(1));

        g.addV().property(T.id, 1).next();
        graph.tx().commit();

        assertFalse(graph.getVertices().get(1).inUse());
        assertEquals(true, graph.hasVertex(1));
    }

    @Test
    public void shouldHandleCorrectlyHandleCountForChangedAndReadElement() {
        final TinkerTransactionGraph graph = TinkerTransactionGraph.open();
        final GraphTraversalSource g = graph.traversal();

        g.addV().property(T.id, 1).next();
        graph.tx().commit();
        TinkerElementContainer vertex = graph.getVertices().get(1);

        g.V().next();
        g.V().property("prop", 5).next();
        g.V().next();
        g.V().property("prop2", "foo").next();
        assertTrue(vertex.isChanged());
        assertTrue(vertex.isRead());
        assertTrue(vertex.inUse());

        graph.tx().commit();
        assertFalse(vertex.isChanged());
        assertFalse(vertex.isRead());

        assertFalse(vertex.inUse());
    }

    @Test
    public void shouldAllowDropAddVertexInSameTransaction() {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final TinkerTransaction tx = (TinkerTransaction) g.tx();

        // create initial vertex with a property
        final GraphTraversalSource gtx = tx.begin();
        Vertex vertex = gtx.addV().property(T.id, vid).property(PROPERTY_NAME, "test").next();
        // add self-referencing edge
        gtx.addE("tests").from(vertex).to(vertex).next();
        gtx.tx().commit();

        verifyCommittedSingleVertex(g, "test");
        assertEquals(1, g.getEdgesCount());
        final long originalVersion = g.getVertices().get(vid).getUnmodified().version();

        // drop the vertex and re-create it without any properties or edges in the same transaction
        final GraphTraversalSource gtx2 = tx.begin();
        gtx2.V().drop().iterate();
        gtx2.addV().property(T.id, vid).next();
        gtx2.tx().commit();

        verifyCommittedSingleVertex(g);
        final TinkerVertex updatedVertex = g.getVertices().get(vid).getUnmodified();
        assertEquals(0, g.getEdgesCount());
        // version should have been updated
        assertNotEquals(originalVersion, updatedVertex.version());
    }

    @Test
    public void shouldAllowAddDropAddVertexInSameTransaction() {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final TinkerTransaction tx = (TinkerTransaction) g.tx();

        final GraphTraversalSource gtx = tx.begin();
        // create vertex with a property
        final Vertex vertex = gtx.addV().property(T.id, vid).property(PROPERTY_NAME, "test").next();
        // add self-referencing edge
        gtx.addE("tests").from(vertex).to(vertex).next();
        // drop vertex
        gtx.V(vid).drop().iterate();
        // add vertex again with different properties but no edges
        gtx.addV().property(T.id, vid).property(PROPERTY_NAME, "updated name").next();
        gtx.tx().commit();
        
        verifyCommittedSingleVertex(g, "updated name");
        assertEquals(0, g.getEdgesCount());
    }

    @Test
    public void shouldAllowAddDropAddDropVertexInSameTransaction() {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final TinkerTransaction tx = (TinkerTransaction) g.tx();

        final GraphTraversalSource gtx = tx.begin();
        // create vertex with a property
        final Vertex vertex = gtx.addV().property(T.id, vid).property(PROPERTY_NAME, "test").next();
        // add self-referencing edge
        gtx.addE("tests").from(vertex).to(vertex).next();
        // drop vertex
        gtx.V(vid).drop().iterate();
        // add vertex again with different properties but no edges
        gtx.addV().property(T.id, vid).property(PROPERTY_NAME, "updated name").next();
        // drop the vertex again
        gtx.V(vid).drop().iterate();
        gtx.tx().commit();
        
        assertEquals(0, g.getVerticesCount());
        assertEquals(0, g.getEdgesCount());
    }

    @Test
    public void shouldAllowAddDropAddDropVertexInMultipleTransactions() {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final TinkerTransaction tx = (TinkerTransaction) g.tx();

        // first transaction
        final GraphTraversalSource gtx1 = tx.begin();
        // create vertex with a property
        final Vertex vertex = gtx1.addV().property(T.id, vid).property(PROPERTY_NAME, "test").next();
        // add self-referencing edge
        gtx1.addE("tests").from(vertex).to(vertex).next();
        // drop vertex
        gtx1.V(vid).drop().iterate();
        gtx1.tx().commit();

        assertEquals(0, g.getVerticesCount());
        assertEquals(0, g.getEdgesCount());

        // second transaction
        final GraphTraversalSource gtx2 = tx.begin();
        // add vertex again with different properties but no edges
        gtx2.addV().property(T.id, vid).property(PROPERTY_NAME, "updated name").next();
        // drop the vertex again
        gtx2.V(vid).drop().iterate();
        gtx2.tx().commit();

        assertEquals(0, g.getVerticesCount());
        assertEquals(0, g.getEdgesCount());
    }

    @Test
    public void shouldAllowDropAddUniqueVertexInConcurrentTransactions() {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final TinkerTransaction tx = (TinkerTransaction) g.tx();

        // create first vertex and commit
        final GraphTraversalSource gtx = tx.begin();
        gtx.addV().property(T.id, vid).next();
        gtx.tx().commit();
        verifyCommittedSingleVertex(g);
        
        // drop and add but don't commit
        gtx.V(vid).drop().iterate();
        gtx.addV().property(T.id, vid).property(PROPERTY_NAME, "main thread").next();
        
        // in another thread add, drop, add a different vertex and commit
        int vid2 = (int) vid + 1;
        runInNewThread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            gtx2.addV().property(T.id, vid2).next();
            gtx2.V(vid2).drop().iterate();
            gtx2.addV().property(T.id, vid2).property(PROPERTY_NAME, "another thread").next();
            gtx2.tx().commit();
        });

        // commit the drop add
        gtx.tx().commit();
        // both vertices should exist
        assertEquals(2, g.getVerticesCount());
        assertEquals("main thread", g.getVertices().get(vid).getUnmodified().properties.get(PROPERTY_NAME).get(0).value());
        assertEquals("another thread", g.getVertices().get(vid2).getUnmodified().properties.get(PROPERTY_NAME).get(0).value());
    }

    @Test
    public void shouldPreventDropAddVertexInConcurrentTransactions() {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final TinkerTransaction tx = (TinkerTransaction) g.tx();

        // create initial vertex and commit transaction
        final GraphTraversalSource gtx = tx.begin();
        gtx.addV().property(T.id, vid).property(PROPERTY_NAME, "original").next();
        gtx.tx().commit();
        verifyCommittedSingleVertex(g, "original");

        // drop the vertex and add it back but don't commit
        gtx.V(vid).drop().iterate();
        gtx.addV().property(T.id, vid).property(PROPERTY_NAME, "should fail!").next();

        // drop the vertex and add it back in another thread and commit
        runInNewThread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            gtx2.V(vid).drop().iterate();
            gtx2.addV().property(T.id, vid).property(PROPERTY_NAME, "updated").next();
            gtx2.tx().commit();
        });

        // try to commit the drop and add in main thread - should fail
        try {
            gtx.tx().commit();
            fail("should throw TransactionException");
        } catch (TransactionException e) {
            assertEquals("Conflict: element modified in another transaction", e.getMessage());
            verifyCommittedSingleVertex(g, "updated");
        }
    }

    @Test
    public void shouldPreventDropUpdateVertexInConcurrentTransactions() {
        final TinkerTransactionGraph g = TinkerTransactionGraph.open();
        final TinkerTransaction tx = (TinkerTransaction) g.tx();

        // create initial vertex and commit transaction
        final GraphTraversalSource gtx = tx.begin();
        gtx.addV().property(T.id, vid).property(PROPERTY_NAME, "original").next();
        gtx.tx().commit();
        verifyCommittedSingleVertex(g, "original");
        
        // drop but don't commit
        gtx.V(vid).drop().iterate();
        
        // modify the vertex in another thread and commit
        runInNewThread(() -> {
            final GraphTraversalSource gtx2 = g.tx().begin();
            gtx2.V(vid).property(PROPERTY_NAME, "updated").next();
            gtx2.tx().commit();
        });
        
        // try to commit the drop vertex - should fail
        try {
            gtx.tx().commit();
            fail("should throw TransactionException");
        } catch (TransactionException e) {
            assertEquals("Conflict: element modified in another transaction", e.getMessage());
            verifyCommittedSingleVertex(g, "updated");
        }
    }
    
    private void runInNewThread(final Runnable runnable) {
        final Thread thread = new Thread(runnable);
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void verifyCommittedSingleVertex(final TinkerTransactionGraph g) {
        verifyCommittedSingleVertex(g, null);
    }

    private void verifyCommittedSingleVertex(final TinkerTransactionGraph g, final String namePropertyValue) {
        // graph should only have a single vertex
        assertEquals(1, g.getVertices().size());
        
        // the single vertex should have the expected id
        final TinkerElementContainer<TinkerVertex> vertexContainer = g.getVertices().get(vid);
        
        // container should be committed and not have an update in progress
        assertFalse(vertexContainer.isChanged());
        assertFalse(vertexContainer.inUse());
        
        // the element in the container should have the same id
        // there are multiple ways to obtain the container element's id and they should be consistent
        assertEquals(vid, vertexContainer.getElementId());
        assertEquals(vid, vertexContainer.get().id());
        final TinkerVertex unmodified = vertexContainer.getUnmodified();
        assertEquals(vid, unmodified.id());

        if (namePropertyValue != null) {
            final Map<String, List<VertexProperty>> properties = g.getVertices().get(vid).getUnmodified().properties;
            assertEquals(1, properties.size());
            final List<VertexProperty> nameProperties = properties.get(PROPERTY_NAME);
            assertEquals(1, nameProperties.size());
            assertEquals(namePropertyValue, nameProperties.get(0).value());
        } else {
            assertNull(vertexContainer.getUnmodified().properties);
        }
    }
}

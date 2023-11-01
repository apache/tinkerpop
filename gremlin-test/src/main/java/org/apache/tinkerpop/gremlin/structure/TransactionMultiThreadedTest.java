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
package org.apache.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.apache.tinkerpop.gremlin.structure.Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.ElementFeatures.FEATURE_ADD_PROPERTY;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_REMOVE_VERTICES;
import static org.junit.Assert.assertEquals;

/**
 * Tests for graphs that has multithreaded access
 */
public class TransactionMultiThreadedTest extends AbstractGremlinTest {

    ///// vertex tests

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_ADD_VERTICES)
    public void shouldCommit() throws InterruptedException {
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
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_REMOVE_VERTICES)
    public void shouldDeleteVertexOnCommit() throws InterruptedException {
        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().next();
        gtx.tx().commit();

        assertEquals(1, (long) gtx.V().count().next());

        gtx.V(v1.id()).drop().iterate();
        assertEquals(0, (long) gtx.V().count().next());

        countElementsInNewThreadTx(g, 1, 0);

        gtx.tx().commit();

        assertEquals(0, (long) gtx.V().count().next());

        final GraphTraversalSource gtx3 = g.tx().begin();
        assertEquals(0L, (long) gtx3.V().count().next());

        countElementsInNewThreadTx(g, 0, 0);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_REMOVE_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = FEATURE_ADD_EDGES)
    public void shouldDeleteRelatedEdgesOnVertexDelete() throws InterruptedException {
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
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_ADD_PROPERTY)
    public void shouldChangeVertexProperty() {
        final GraphTraversalSource gtx = g.tx().begin();

        // create vertex with test property
        final Vertex v1 = gtx.addV().property("test", 1).next();
        gtx.tx().commit();

        assertEquals(1, gtx.V(v1.id()).values("test").next());

        // change test property
        gtx.V(v1.id()).property("test", 2).iterate();
        gtx.tx().commit();

        // should be only 1 vertex with updated property
        assertEquals(1L, (long) gtx.V().count().next());
        assertEquals(2, gtx.V(v1.id()).values("test").next());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_ADD_VERTICES)
    public void shouldRollbackAddedVertex() throws InterruptedException {
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

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = FEATURE_ADD_EDGES)
    public void shouldCommitEdge() throws InterruptedException {
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
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = FEATURE_ADD_EDGES)
    public void shouldRollbackAddedEdge() throws InterruptedException {
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

    private void countElementsInNewThreadTx(final GraphTraversalSource g, final long verticesCount, final long edgesCount) throws InterruptedException {
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
}

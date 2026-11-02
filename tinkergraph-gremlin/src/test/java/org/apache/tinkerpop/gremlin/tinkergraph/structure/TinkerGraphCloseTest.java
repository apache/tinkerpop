/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Supplier;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Verifies that a closed {@link TinkerGraph} refuses further use, through the graph, a traversal, or an element
 * obtained from it before it was closed.
 */
@RunWith(Parameterized.class)
public class TinkerGraphCloseTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"TinkerMemoryGraph", (Supplier<AbstractTinkerGraph>) () -> TinkerMemoryGraph.open(multiLabelConfiguration())},
                {"TinkerStorageGraph", (Supplier<AbstractTinkerGraph>) () -> TinkerStorageGraph.open(multiLabelConfiguration())}});
    }

    /**
     * Allows a vertex any number of labels, so adding one to a held vertex is valid apart from the graph being closed.
     */
    private static Configuration multiLabelConfiguration() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_LABEL_CARDINALITY, "ZERO_OR_MORE");
        return conf;
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public Supplier<AbstractTinkerGraph> graphSupplier;

    @Test
    public void shouldRefuseGraphMethodsAfterClose() {
        final AbstractTinkerGraph graph = graphSupplier.get();
        final Vertex v = graph.addVertex("name", "marko");
        final Edge e = v.addEdge("knows", v);
        commit(graph);
        graph.close();

        assertClosed(() -> graph.addVertex("name", "vadas"));
        assertClosed(() -> graph.addEdge((TinkerVertex) v, (TinkerVertex) v, "knows"));
        assertClosed(() -> graph.removeVertex(v.id()));
        assertClosed(() -> graph.removeEdge(e.id()));
        assertClosed(graph::vertices);
        assertClosed(graph::edges);
        assertClosed(() -> graph.vertex(v.id()));
        assertClosed(() -> graph.edge(e.id()));
        assertClosed(() -> graph.hasVertex(v.id()));
        assertClosed(() -> graph.hasEdge(e.id()));
        assertClosed(graph::getVerticesCount);
        assertClosed(graph::getEdgesCount);
        assertClosed(() -> graph.countVerticesByLabel("vertex"));
        assertClosed(() -> graph.countEdgesByLabel("knows"));
        assertClosed(graph::tx);
        assertClosed(graph::variables);
        assertClosed(graph::compute);
        assertClosed(() -> graph.io(IoCore.graphson()));
        assertClosed(() -> graph.createIndex("name", Vertex.class));
        assertClosed(() -> graph.dropIndex("name", Vertex.class));
        assertClosed(() -> graph.getIndexedKeys(Vertex.class));
        assertClosed(() -> graph.index().queryVertexIndex("name", "marko"));
        assertClosed(graph::clear);
    }

    @Test
    public void shouldRefuseTraversalsAfterClose() {
        final AbstractTinkerGraph graph = graphSupplier.get();
        final GraphTraversalSource g = traversal().with(graph);
        g.addV("person").iterate();
        commit(graph);
        assertEquals(1L, g.V().count().next().longValue());
        graph.close();

        assertClosed(() -> g.V().toList());
        assertClosed(() -> g.V().count().next());
        assertClosed(() -> g.E().toList());
        assertClosed(() -> g.addV("person").iterate());
    }

    @Test
    public void shouldRefuseMutationThroughHeldElementsAfterClose() {
        final AbstractTinkerGraph graph = graphSupplier.get();
        final Vertex v = graph.addVertex("name", "marko");
        final Vertex other = graph.addVertex("name", "vadas");
        final Edge e = v.addEdge("knows", other, "weight", 0.5d);
        commit(graph);

        // elements are read from the graph before close so each is the instance a caller would be holding
        final Vertex heldV = graph.vertex(v.id());
        final Vertex heldOther = graph.vertex(other.id());
        final Edge heldE = graph.edge(e.id());
        final VertexProperty<Object> heldVp = heldV.property("name");
        final Property<Object> heldEp = heldE.property("weight");
        graph.close();

        assertClosed(() -> heldV.property("age", 29));
        assertClosed(() -> heldV.addEdge("knows", heldOther));
        assertClosed(heldV::remove);
        assertClosed(() -> ((TinkerVertex) heldV).addLabel("person"));
        assertClosed(() -> heldE.property("weight", 1.0d));
        assertClosed(heldE::remove);
        assertClosed(() -> heldVp.property("since", 2010));
        assertClosed(heldVp::remove);
        assertClosed(heldEp::remove);
    }

    @Test
    public void shouldReadThroughHeldElementsAfterClose() {
        final AbstractTinkerGraph graph = graphSupplier.get();
        final Vertex v = graph.addVertex("name", "marko");
        final Vertex other = graph.addVertex("name", "vadas");
        final Edge e = v.addEdge("knows", other, "weight", 0.5d);
        commit(graph);

        final Vertex heldV = graph.vertex(v.id());
        final Edge heldE = graph.edge(e.id());
        final VertexProperty<Object> heldVp = heldV.property("name");
        final Property<Object> heldEp = heldE.property("weight");
        graph.close();

        assertEquals("marko", heldV.value("name"));
        assertEquals("marko", heldVp.value());
        assertEquals(0.5d, heldEp.value());
        assertEquals(e.id(), heldV.edges(Direction.OUT).next().id());
        assertEquals(other.id(), heldV.vertices(Direction.OUT, "knows").next().id());
        assertEquals(v.id(), heldE.outVertex().id());
        assertEquals(other.id(), heldE.inVertex().id());
        assertThat(heldE.toString(), containsString("knows"));
    }

    @Test
    public void shouldContinueIterationStartedBeforeClose() {
        final AbstractTinkerGraph graph = graphSupplier.get();
        for (int i = 0; i < 100; i++) {
            graph.addVertex("name", "v" + i);
        }
        commit(graph);

        final Iterator<Vertex> vertices = graph.vertices();
        final GraphTraversal<Vertex, Object> names = traversal().with(graph).V().values("name");
        vertices.next();
        names.next();
        graph.close();

        assertEquals(99L, IteratorUtils.count(vertices));
        assertEquals(99L, IteratorUtils.count(names));
    }

    @Test
    public void shouldRollbackHeldTransactionAfterClose() {
        final AbstractTinkerGraph graph = graphSupplier.get();
        assumeTrue(graph.features().graph().supportsTransactions());
        final Transaction tx = graph.tx();
        graph.addVertex("name", "marko");
        graph.close();

        tx.rollback();
        assertFalse(tx.isOpen());
    }

    @Test
    public void shouldCommitHeldReadOnlyTransactionAfterClose() {
        final AbstractTinkerGraph graph = graphSupplier.get();
        assumeTrue(graph.features().graph().supportsTransactions());
        graph.addVertex("name", "marko");
        graph.tx().commit();

        final Transaction tx = graph.tx();
        assertEquals(1L, IteratorUtils.count(graph.vertices()));
        assertTrue(tx.isOpen());
        graph.close();

        tx.commit();
        assertFalse(tx.isOpen());
    }

    @Test
    public void shouldAllowCloseToBeCalledMoreThanOnce() {
        final AbstractTinkerGraph graph = graphSupplier.get();
        graph.close();
        graph.close();
        assertThat(graph.toString(), containsString("closed"));
    }

    private static void commit(final AbstractTinkerGraph graph) {
        if (graph.features().graph().supportsTransactions())
            graph.tx().commit();
    }

    private static void assertClosed(final ThrowingRunnable runnable) {
        final IllegalStateException ex = assertThrows(IllegalStateException.class, runnable);
        assertThat(ex.getMessage(), containsString("is closed"));
    }
}

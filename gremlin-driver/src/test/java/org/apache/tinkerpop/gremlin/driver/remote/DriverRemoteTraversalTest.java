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
package org.apache.tinkerpop.gremlin.driver.remote;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.tinkerpop.gremlin.process.remote.RemoteConnection.GREMLIN_REMOTE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DriverRemoteTraversal} covering the {@code next()}/{@code hasNext()} bulk-unrolling logic and
 * the {@link DriverRemoteTraversal.AttachingTraverserIterator} attachment path. These exercise the traversal's result
 * iteration directly (backed by a mocked {@link ResultSet}) without a live server.
 */
public class DriverRemoteTraversalTest {

    private static <E> DriverRemoteTraversal<?, E> traversalOf(final List<Result> results, final boolean attach,
                                                               final Optional<Configuration> conf) {
        final ResultSet rs = mock(ResultSet.class);
        when(rs.iterator()).thenReturn(results.iterator());
        return new DriverRemoteTraversal<>(rs, mock(Client.class), attach, conf);
    }

    @Test
    public void shouldUnrollBulkedTraverserAcrossNextAndHasNext() {
        // a single traverser with bulk 3 should yield the same value three times via next()/hasNext()
        final Result r = new Result(new DefaultRemoteTraverser<>("a", 3L));
        final DriverRemoteTraversal<?, String> t = traversalOf(Arrays.asList(r), false, Optional.empty());

        final List<String> out = new ArrayList<>();
        while (t.hasNext()) out.add(t.next());

        assertEquals(Arrays.asList("a", "a", "a"), out);
        assertFalse(t.hasNext());
    }

    @Test
    public void shouldWrapRawResultsAsSingleBulkTraversers() {
        // non-RemoteTraverser results are wrapped as bulk-1 traversers by the TraverserIterator
        final DriverRemoteTraversal<?, String> t = traversalOf(
                Arrays.asList(new Result("x"), new Result("y")), false, Optional.empty());

        assertTrue(t.hasNext());
        assertEquals("x", t.next());
        assertEquals("y", t.next());
        assertFalse(t.hasNext());
    }

    @Test
    public void shouldAttachDetachedElementsToConfiguredGraph() {
        final TinkerGraph graph = TinkerGraph.open();
        final Vertex hostVertex = graph.addVertex(T.id, 1, T.label, "person");
        final DetachedVertex detached = DetachedFactory.detach(hostVertex, true);

        final Configuration conf = new BaseConfiguration();
        conf.setProperty(GREMLIN_REMOTE + "attachment", (Supplier<Graph>) () -> graph);

        final DriverRemoteTraversal<?, Vertex> t = traversalOf(
                Arrays.asList(new Result(new DefaultRemoteTraverser<>(detached, 1L))), true, Optional.of(conf));

        final Vertex attached = t.next();
        // the detached vertex is attached to the host graph, resolving to the real host vertex
        assertEquals(hostVertex, attached);
        assertEquals(hostVertex.id(), attached.id());
    }

    @Test
    public void shouldPassThroughNonAttachableResultsWhenAttaching() {
        final TinkerGraph graph = TinkerGraph.open();
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(GREMLIN_REMOTE + "attachment", (Supplier<Graph>) () -> graph);

        final DriverRemoteTraversal<?, String> t = traversalOf(
                Arrays.asList(new Result("plain")), true, Optional.of(conf));

        assertEquals("plain", t.next());
    }

    @Test
    public void shouldThrowWhenAttachRequestedWithoutConfiguration() {
        final ResultSet rs = mock(ResultSet.class);
        final IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new DriverRemoteTraversal<>(rs, mock(Client.class), true, Optional.empty()));
        assertEquals("Traverser can't be reattached for testing", ex.getMessage());
    }
}

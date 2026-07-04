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
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

/**
 * Verifies the contract of the {@link TinkerGraph} interface: the static {@code open()} methods construct the
 * in-memory implementation and {@link GraphFactory} can resolve the interface name as well as the name of either
 * implementation from the {@code gremlin.graph} configuration key.
 */
public class TinkerGraphInterfaceTest {

    private static Configuration configFor(final Class<?> clazz) {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, clazz.getName());
        return conf;
    }

    @Test
    public void shouldOpenTinkerMemoryGraphFromInterface() {
        final TinkerGraph graph = TinkerGraph.open();
        assertThat(graph, instanceOf(TinkerMemoryGraph.class));
        graph.close();
    }

    @Test
    public void shouldResolveInterfaceNameThroughGraphFactory() throws Exception {
        final Graph graph = GraphFactory.open(configFor(TinkerGraph.class));
        assertThat(graph, instanceOf(TinkerMemoryGraph.class));
        graph.close();
    }

    @Test
    public void shouldResolveTinkerMemoryGraphThroughGraphFactory() throws Exception {
        final Graph graph = GraphFactory.open(configFor(TinkerMemoryGraph.class));
        assertThat(graph, instanceOf(TinkerMemoryGraph.class));
        graph.close();
    }

    @Test
    public void shouldResolveTinkerStorageGraphThroughGraphFactory() throws Exception {
        final Graph graph = GraphFactory.open(configFor(TinkerStorageGraph.class));
        assertThat(graph, instanceOf(TinkerStorageGraph.class));
        graph.close();
    }

    @Test
    public void shouldHaveBothImplementationsImplementTinkerGraph() {
        final TinkerMemoryGraph memory = TinkerMemoryGraph.open();
        final TinkerStorageGraph storage = TinkerStorageGraph.open();
        assertThat(memory, instanceOf(TinkerGraph.class));
        assertThat(storage, instanceOf(TinkerGraph.class));
        memory.close();
        storage.close();
    }
}

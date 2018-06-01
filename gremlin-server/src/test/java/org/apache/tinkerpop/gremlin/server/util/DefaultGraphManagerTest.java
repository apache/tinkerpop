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
package org.apache.tinkerpop.gremlin.server.util;

import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import javax.script.Bindings;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DefaultGraphManagerTest {

    @Test
    public void shouldReturnGraphs() {
        final Settings settings = Settings.read(DefaultGraphManagerTest.class.getResourceAsStream("../gremlin-server-integration.yaml"));
        final GraphManager graphManager = new DefaultGraphManager(settings);
        final Set<String> graphNames = graphManager.getGraphNames();

        assertNotNull(graphNames);
        assertEquals(6, graphNames.size());

        assertThat(graphNames.contains("graph"), is(true));
        assertThat(graphNames.contains("classic"), is(true));
        assertThat(graphNames.contains("modern"), is(true));
        assertThat(graphNames.contains("crew"), is(true));
        assertThat(graphNames.contains("sink"), is(true));
        assertThat(graphNames.contains("grateful"), is(true));
        assertThat(graphManager.getGraph("graph"), instanceOf(TinkerGraph.class));
    }

    @Test
    public void shouldGetAsBindings() {
        final Settings settings = Settings.read(DefaultGraphManagerTest.class.getResourceAsStream("../gremlin-server-integration.yaml"));
        final GraphManager graphManager = new DefaultGraphManager(settings);
        final Bindings bindings = graphManager.getAsBindings();

        assertNotNull(bindings);
        assertEquals(6, bindings.size());
        assertThat(bindings.containsKey("graph"), is(true));
        assertThat(bindings.containsKey("classic"), is(true));
        assertThat(bindings.containsKey("modern"), is(true));
        assertThat(bindings.containsKey("crew"), is(true));
        assertThat(bindings.containsKey("sink"), is(true));
        assertThat(bindings.containsKey("grateful"), is(true));
        assertThat(bindings.get("graph"), instanceOf(TinkerGraph.class));
    }

    @Test
    public void shouldGetGraph() {
        final Settings settings = Settings.read(DefaultGraphManagerTest.class.getResourceAsStream("../gremlin-server-integration.yaml"));
        final GraphManager graphManager = new DefaultGraphManager(settings);
        final Graph graph = graphManager.getGraph("graph");

        assertNotNull(graph);
        assertThat(graph, instanceOf(TinkerGraph.class));
    }

    @Test
    public void shouldGetDynamicallyAddedGraph() {
        final Settings settings = Settings.read(DefaultGraphManagerTest.class.getResourceAsStream("../gremlin-server-integration.yaml"));
        final GraphManager graphManager = new DefaultGraphManager(settings);
        final Graph graph = graphManager.getGraph("graph"); //fake out a graph instance
        graphManager.putGraph("newGraph", graph);

        final Set<String> graphNames = graphManager.getGraphNames();
        assertNotNull(graphNames);
        assertEquals(7, graphNames.size());
        assertThat(graphNames.contains("newGraph"), is(true));
        assertThat(graphNames.contains("graph"), is(true));
        assertThat(graphNames.contains("classic"), is(true));
        assertThat(graphNames.contains("modern"), is(true));
        assertThat(graphNames.contains("crew"), is(true));
        assertThat(graphNames.contains("sink"), is(true));
        assertThat(graphNames.contains("grateful"), is(true));
        assertThat(graphManager.getGraph("newGraph"), instanceOf(TinkerGraph.class));
    }

    @Test
    public void shouldNotGetRemovedGraph() throws Exception {
        final Settings settings = Settings.read(DefaultGraphManagerTest.class.getResourceAsStream("../gremlin-server-integration.yaml"));
        final GraphManager graphManager = new DefaultGraphManager(settings);
        final Graph graph = graphManager.getGraph("graph"); //fake out a graph instance
        graphManager.putGraph("newGraph", graph);
        final Set<String> graphNames = graphManager.getGraphNames();
        assertNotNull(graphNames);
        assertEquals(7, graphNames.size());
        assertThat(graphNames.contains("newGraph"), is(true));
        assertThat(graphManager.getGraph("newGraph"), instanceOf(TinkerGraph.class));

        graphManager.removeGraph("newGraph");

        final Set<String> graphNames2 = graphManager.getGraphNames();
        assertEquals(6, graphNames2.size());
        assertThat(graphNames2.contains("newGraph"), is(false));
    }

    @Test
    public void openGraphShouldReturnExistingGraph() {
        final Settings settings = Settings.read(DefaultGraphManagerTest.class.getResourceAsStream("../gremlin-server-integration.yaml"));
        final GraphManager graphManager = new DefaultGraphManager(settings);

        final Graph graph = graphManager.openGraph("graph", null);
        assertNotNull(graph);
        assertThat(graph, instanceOf(TinkerGraph.class));
    }

    @Test
    public void openGraphShouldReturnNewGraphUsingThunk() {
        final Settings settings = Settings.read(DefaultGraphManagerTest.class.getResourceAsStream("../gremlin-server-integration.yaml"));
        final GraphManager graphManager = new DefaultGraphManager(settings);

        final Graph graph = graphManager.getGraph("graph"); //fake out graph instance

        final Graph newGraph = graphManager.openGraph("newGraph", (String gName) -> {
            return graph;
        });

        assertNotNull(graph);
        assertThat(graph, instanceOf(TinkerGraph.class));
        assertSame(graph, newGraph);
    }
}

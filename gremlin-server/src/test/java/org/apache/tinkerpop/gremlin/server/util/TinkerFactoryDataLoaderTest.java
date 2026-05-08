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
package org.apache.tinkerpop.gremlin.server.util;

import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TinkerFactoryDataLoaderTest {

    private LifeCycleHook.Context createContext(final GraphManager graphManager) {
        return new LifeCycleHook.Context(LoggerFactory.getLogger(TinkerFactoryDataLoaderTest.class), graphManager);
    }

    @Test(expected = IllegalArgumentException.class)
    public void initShouldThrowWhenGraphMissing() {
        final TinkerFactoryDataLoader loader = new TinkerFactoryDataLoader();
        final Map<String, Object> config = new HashMap<>();
        config.put("dataset", "modern");
        loader.init(config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void initShouldThrowWhenDatasetMissing() {
        final TinkerFactoryDataLoader loader = new TinkerFactoryDataLoader();
        final Map<String, Object> config = new HashMap<>();
        config.put("graph", "graph");
        loader.init(config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void initShouldThrowWhenConfigEmpty() {
        final TinkerFactoryDataLoader loader = new TinkerFactoryDataLoader();
        loader.init(new HashMap<>());
    }

    @Test
    public void onStartUpShouldHandleMissingGraph() {
        final TinkerFactoryDataLoader loader = new TinkerFactoryDataLoader();
        final Map<String, Object> config = new HashMap<>();
        config.put("graph", "nonexistent");
        config.put("dataset", "modern");
        loader.init(config);

        final GraphManager gm = mock(GraphManager.class);
        when(gm.getGraph("nonexistent")).thenReturn(null);

        // should not throw — just logs a warning
        loader.onStartUp(createContext(gm));
    }

    @Test
    public void onStartUpShouldHandleNonTinkerGraph() {
        final TinkerFactoryDataLoader loader = new TinkerFactoryDataLoader();
        final Map<String, Object> config = new HashMap<>();
        config.put("graph", "graph");
        config.put("dataset", "modern");
        loader.init(config);

        final Graph mockGraph = mock(Graph.class);
        final GraphManager gm = mock(GraphManager.class);
        when(gm.getGraph("graph")).thenReturn(mockGraph);

        // should not throw — just logs a warning
        loader.onStartUp(createContext(gm));
    }

    @Test
    public void onStartUpShouldHandleUnknownDataset() {
        final TinkerFactoryDataLoader loader = new TinkerFactoryDataLoader();
        final Map<String, Object> config = new HashMap<>();
        config.put("graph", "graph");
        config.put("dataset", "bogus");
        loader.init(config);

        final TinkerGraph graph = TinkerGraph.open();
        try {
            final GraphManager gm = mock(GraphManager.class);
            when(gm.getGraph("graph")).thenReturn(graph);

            // should not throw — just logs a warning
            loader.onStartUp(createContext(gm));
            assertThat((int) graph.traversal().V().count().next().longValue(), is(0));
        } finally {
            graph.close();
        }
    }

    @Test
    public void onStartUpShouldLoadModern() {
        final TinkerFactoryDataLoader loader = new TinkerFactoryDataLoader();
        final Map<String, Object> config = new HashMap<>();
        config.put("graph", "graph");
        config.put("dataset", "modern");
        loader.init(config);

        final TinkerGraph graph = TinkerGraph.open();
        try {
            final GraphManager gm = mock(GraphManager.class);
            when(gm.getGraph("graph")).thenReturn(graph);

            loader.onStartUp(createContext(gm));
            assertThat((int) graph.traversal().V().count().next().longValue(), is(6));
            assertThat((int) graph.traversal().E().count().next().longValue(), is(6));
        } finally {
            graph.close();
        }
    }

    @Test
    public void onStartUpShouldLoadClassic() {
        final TinkerFactoryDataLoader loader = new TinkerFactoryDataLoader();
        final Map<String, Object> config = new HashMap<>();
        config.put("graph", "graph");
        config.put("dataset", "classic");
        loader.init(config);

        final TinkerGraph graph = TinkerGraph.open();
        try {
            final GraphManager gm = mock(GraphManager.class);
            when(gm.getGraph("graph")).thenReturn(graph);

            loader.onStartUp(createContext(gm));
            assertThat((int) graph.traversal().V().count().next().longValue(), is(6));
        } finally {
            graph.close();
        }
    }

    @Test
    public void onStartUpShouldLoadCrew() {
        final TinkerFactoryDataLoader loader = new TinkerFactoryDataLoader();
        final Map<String, Object> config = new HashMap<>();
        config.put("graph", "graph");
        config.put("dataset", "crew");
        loader.init(config);

        final TinkerGraph graph = TinkerGraph.open();
        try {
            final GraphManager gm = mock(GraphManager.class);
            when(gm.getGraph("graph")).thenReturn(graph);

            loader.onStartUp(createContext(gm));
            assertThat((int) graph.traversal().V().count().next().longValue(), greaterThan(0));
        } finally {
            graph.close();
        }
    }

    @Test
    public void onStartUpShouldLoadGrateful() {
        final TinkerFactoryDataLoader loader = new TinkerFactoryDataLoader();
        final Map<String, Object> config = new HashMap<>();
        config.put("graph", "graph");
        config.put("dataset", "grateful");
        loader.init(config);

        final TinkerGraph graph = TinkerGraph.open();
        try {
            final GraphManager gm = mock(GraphManager.class);
            when(gm.getGraph("graph")).thenReturn(graph);

            loader.onStartUp(createContext(gm));
            assertThat((int) graph.traversal().V().count().next().longValue(), greaterThan(0));
        } finally {
            graph.close();
        }
    }

    @Test
    public void onStartUpShouldLoadSink() {
        final TinkerFactoryDataLoader loader = new TinkerFactoryDataLoader();
        final Map<String, Object> config = new HashMap<>();
        config.put("graph", "graph");
        config.put("dataset", "sink");
        loader.init(config);

        final TinkerGraph graph = TinkerGraph.open();
        try {
            final GraphManager gm = mock(GraphManager.class);
            when(gm.getGraph("graph")).thenReturn(graph);

            loader.onStartUp(createContext(gm));
            assertThat((int) graph.traversal().V().count().next().longValue(), greaterThan(0));
        } finally {
            graph.close();
        }
    }

    @Test
    public void onStartUpShouldLoadAirRoutes() {
        final TinkerFactoryDataLoader loader = new TinkerFactoryDataLoader();
        final Map<String, Object> config = new HashMap<>();
        config.put("graph", "graph");
        config.put("dataset", "airroutes");
        loader.init(config);

        final TinkerGraph graph = TinkerGraph.open();
        try {
            final GraphManager gm = mock(GraphManager.class);
            when(gm.getGraph("graph")).thenReturn(graph);

            loader.onStartUp(createContext(gm));
            assertThat((int) graph.traversal().V().count().next().longValue(), greaterThan(0));
        } finally {
            graph.close();
        }
    }
}

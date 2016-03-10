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
package org.apache.tinkerpop.gremlin.server;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.strategy.DriverServerConnection;
import org.apache.tinkerpop.gremlin.process.server.ServerGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import java.util.Collections;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class WithServerIntegrationTest extends AbstractGremlinServerIntegrationTest {
    @Override
    public Settings overrideSettings(final Settings settings) {
        settings.scriptEngines.get("gremlin-groovy").scripts = Collections.singletonList("scripts/generate-modern.groovy");
        return settings;
    }

    @Test
    public void shouldDoTraversalReturningVertices() {
        final Graph graph = TinkerGraph.open();
        final Cluster cluster = Cluster.open();
        final GraphTraversalSource g = graph.traversal().withServer(DriverServerConnection.using(cluster));
        g.V().forEachRemaining(v -> System.out.println("TEST: " + v.toString()));
        cluster.close();
    }

    @Test
    public void shouldDoServerGraphReturningVertices() {
        final Cluster cluster = Cluster.open();
        final Graph graph = ServerGraph.open(DriverServerConnection.using(cluster), TinkerGraph.class);
        final GraphTraversalSource g = graph.traversal();
        g.V().forEachRemaining(v -> System.out.println("TEST: " + v.toString()));
        cluster.close();
    }
}

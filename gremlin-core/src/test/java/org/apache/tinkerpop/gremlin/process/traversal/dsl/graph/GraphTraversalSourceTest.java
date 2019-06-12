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
package org.apache.tinkerpop.gremlin.process.traversal.dsl.graph;

import org.apache.commons.configuration.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import java.util.HashMap;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphTraversalSourceTest {
    @Test
    public void shouldCloseRemoteConnectionOnWithRemote() throws Exception {
        final RemoteConnection mock = mock(RemoteConnection.class);
        final GraphTraversalSource g = traversal().withRemote(mock);
        g.close();

        verify(mock, times(1)).close();
    }

    @Test
    public void shouldSupportMapBasedStrategies() throws Exception {
        GraphTraversalSource g = EmptyGraph.instance().traversal();
        assertFalse(g.getStrategies().getStrategy(SubgraphStrategy.class).isPresent());
        g = g.withStrategies(SubgraphStrategy.create(new MapConfiguration(new HashMap<String, Object>() {{
            put("vertices", __.hasLabel("person"));
            put("vertexProperties", __.limit(0));
            put("edges", __.hasLabel("knows"));
        }})));
        assertTrue(g.getStrategies().getStrategy(SubgraphStrategy.class).isPresent());
        g = g.withoutStrategies(SubgraphStrategy.class);
        assertFalse(g.getStrategies().getStrategy(SubgraphStrategy.class).isPresent());
        //
        assertFalse(g.getStrategies().getStrategy(ReadOnlyStrategy.class).isPresent());
        g = g.withStrategies(ReadOnlyStrategy.instance());
        assertTrue(g.getStrategies().getStrategy(ReadOnlyStrategy.class).isPresent());
        g = g.withoutStrategies(ReadOnlyStrategy.class);
        assertFalse(g.getStrategies().getStrategy(ReadOnlyStrategy.class).isPresent());
    }
}

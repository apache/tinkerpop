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
package org.apache.tinkerpop.gremlin.process.remote;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertEquals;

public class EmbeddedRemoteConnectionTest {
    @Test
    public void shouldAllowToUseLocalGraphAsRemote() {
        final Graph graph = EmptyGraph.instance();
        final GraphTraversalSource g = graph.traversal();
        final GraphTraversalSource simulatedRemoteG = traversal().with(new EmbeddedRemoteConnection(g));
        assertEquals(0, simulatedRemoteG.V().count().next().intValue());
    }

    @Test
    public void shouldAllowToUseLocalGraphAsRemoteWithParameter() {
        final Graph graph = EmptyGraph.instance();
        final GraphTraversalSource g = graph.traversal();
        final GraphTraversalSource simulatedRemoteG = traversal().with(new EmbeddedRemoteConnection(g));
        assertEquals(33, simulatedRemoteG.inject(GValue.of(null, 11), GValue.of("x", 22)).sum().next());
    }
}

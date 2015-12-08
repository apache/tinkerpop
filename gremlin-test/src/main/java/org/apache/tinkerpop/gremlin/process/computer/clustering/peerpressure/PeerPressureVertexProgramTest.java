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

package org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PeerPressureVertexProgramTest extends AbstractGremlinProcessTest {

    @Test
    @LoadGraphWith(MODERN)
    public void shouldExecutePeerPressure() throws Exception {
        if (g.getGraphComputer().get().features().supportsResultGraphPersistCombination(GraphComputer.ResultGraph.NEW, GraphComputer.Persist.VERTEX_PROPERTIES)) {
            final ComputerResult result = graph.compute(g.getGraphComputer().get().getClass()).program(PeerPressureVertexProgram.build().create(graph)).submit().get();
            final Set<Object> clusters = new HashSet<>();
            result.graph().traversal().V().forEachRemaining(v -> {
                assertEquals(4, v.keys().size()); // name, age/lang, voteStrength, cluster
                assertTrue(v.keys().contains("name"));
                assertTrue(v.keys().contains(PeerPressureVertexProgram.VOTE_STRENGTH));
                assertTrue(v.keys().contains(PeerPressureVertexProgram.CLUSTER));
                assertEquals(1, IteratorUtils.count(v.values("name")));
                assertEquals(1, IteratorUtils.count(v.values(PeerPressureVertexProgram.CLUSTER)));
                final Object cluster = v.value(PeerPressureVertexProgram.CLUSTER);
                clusters.add(cluster);
            });
            assertEquals(2, clusters.size());
            assertEquals(3, result.memory().getIteration());
            assertEquals(1, result.memory().asMap().size());
            assertTrue(result.memory().keys().contains("gremlin.peerPressureVertexProgram.voteToHalt"));  // this is private in PeerPressureVertexProgram (and that is okay)
        }
    }
}
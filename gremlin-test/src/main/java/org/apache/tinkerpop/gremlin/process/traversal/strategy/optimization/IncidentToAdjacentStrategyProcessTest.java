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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_TraverserGenerator;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.junit.Assert.assertTrue;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
@RunWith(GremlinProcessRunner.class)
public class IncidentToAdjacentStrategyProcessTest extends AbstractGremlinProcessTest {

    @Test
    @LoadGraphWith(MODERN)
    public void shouldGenerateCorrectTraversers() throws Exception {

        final GraphTraversalSource itag = g.withStrategies(IncidentToAdjacentStrategy.instance());
        
        GraphTraversal.Admin traversal;

        traversal = itag.V().outE().iterate().asAdmin();
        assertTrue(traversal.getTraverserGenerator() instanceof B_O_TraverserGenerator);
        traversal = itag.V().outE().inV().iterate().asAdmin();
        assertTrue(traversal.getTraverserGenerator() instanceof B_O_TraverserGenerator);
        traversal = itag.V().outE().otherV().iterate().asAdmin();
        assertTrue(traversal.getTraverserGenerator() instanceof B_O_TraverserGenerator);
        traversal = itag.V().out().iterate().asAdmin();
        assertTrue(traversal.getTraverserGenerator() instanceof B_O_TraverserGenerator);

        traversal = itag.V().bothE().iterate().asAdmin();
        assertTrue(traversal.getTraverserGenerator() instanceof B_O_TraverserGenerator);
        traversal = itag.V().bothE().otherV().iterate().asAdmin();
        assertTrue(traversal.getTraverserGenerator() instanceof B_O_TraverserGenerator);
        traversal = itag.V().both().iterate().asAdmin();
        assertTrue(traversal.getTraverserGenerator() instanceof B_O_TraverserGenerator);

        traversal = itag.V().flatMap(bothE()).iterate().asAdmin();
        assertTrue(traversal.getTraverserGenerator() instanceof B_O_TraverserGenerator);
        traversal = itag.V().flatMap(bothE().otherV()).iterate().asAdmin();
        assertTrue(traversal.getTraverserGenerator() instanceof B_O_TraverserGenerator);
        traversal = itag.V().flatMap(both()).iterate().asAdmin();
        assertTrue(traversal.getTraverserGenerator() instanceof B_O_TraverserGenerator);
    }
}

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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.CountStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertTrue;

public class ExplainStrategyTest {

    private static final GraphTraversalSource g = traversal().withEmbedded(EmptyGraph.instance()).
            withStrategies(CountStrategy.instance(), ReferenceElementStrategy.instance());

    @Test
    public void shouldModifyTraversal() {
        final Traversal<Vertex, ?> t = g.withStrategies(ExplainStrategy.instance()).V().limit(1).count().is(0);

        final String explanation = (String) t.next();

        // GraphStep removed
        assertTrue(t.asAdmin().getSteps().stream().noneMatch(s -> s instanceof GraphStep));
        // InjectStep added
        assertTrue(t.asAdmin().getSteps().stream().anyMatch(s -> s instanceof InjectStep));
        assertTrue(explanation.startsWith("Traversal Explanation"));
    }
}

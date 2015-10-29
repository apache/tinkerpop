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
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.sideEffect.TinkerGraphStep;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */

public class TinkerGraphStepStrategyTest extends AbstractGremlinProcessTest {

    @Test
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void shouldFoldInHasContainers() {
        GraphTraversal.Admin traversal = g.V().has("name", "marko").asAdmin();
        assertEquals(2, traversal.getSteps().size());
        assertEquals(HasStep.class, traversal.getEndStep().getClass());
        traversal.applyStrategies();
        assertEquals(1, traversal.getSteps().size());
        assertEquals(TinkerGraphStep.class, traversal.getStartStep().getClass());
        assertEquals(TinkerGraphStep.class, traversal.getEndStep().getClass());
        assertEquals(1, ((TinkerGraphStep) traversal.getStartStep()).getHasContainers().size());
        assertEquals("name", ((TinkerGraphStep<?, ?>) traversal.getStartStep()).getHasContainers().get(0).getKey());
        assertEquals("marko", ((TinkerGraphStep<?, ?>) traversal.getStartStep()).getHasContainers().get(0).getValue());
        ////
        traversal = g.V().has("name", "marko").has("age", P.gt(20)).asAdmin();
        traversal.applyStrategies();
        assertEquals(1, traversal.getSteps().size());
        assertEquals(TinkerGraphStep.class, traversal.getStartStep().getClass());
        assertEquals(2, ((TinkerGraphStep) traversal.getStartStep()).getHasContainers().size());
        ////
        traversal = g.V().has("name", "marko").out().has("name", "daniel").asAdmin();
        traversal.applyStrategies();
        assertEquals(3, traversal.getSteps().size());
        assertEquals(TinkerGraphStep.class, traversal.getStartStep().getClass());
        assertEquals(1, ((TinkerGraphStep) traversal.getStartStep()).getHasContainers().size());
        assertEquals("name", ((TinkerGraphStep<?, ?>) traversal.getStartStep()).getHasContainers().get(0).getKey());
        assertEquals("marko", ((TinkerGraphStep<?, ?>) traversal.getStartStep()).getHasContainers().get(0).getValue());
        assertEquals(HasStep.class, traversal.getEndStep().getClass());
        ////
        traversal = g.V().has("name", "marko").out().V().has("name", "daniel").asAdmin();
        traversal.applyStrategies();
        assertEquals(3, traversal.getSteps().size());
        assertEquals(TinkerGraphStep.class, traversal.getStartStep().getClass());
        assertEquals(1, ((TinkerGraphStep) traversal.getStartStep()).getHasContainers().size());
        assertEquals("name", ((TinkerGraphStep<?, ?>) traversal.getStartStep()).getHasContainers().get(0).getKey());
        assertEquals("marko", ((TinkerGraphStep<?, ?>) traversal.getStartStep()).getHasContainers().get(0).getValue());
        assertEquals(TinkerGraphStep.class, traversal.getSteps().get(2).getClass());
        assertEquals(1, ((TinkerGraphStep) traversal.getSteps().get(2)).getHasContainers().size());
        assertEquals("name", ((TinkerGraphStep<?, ?>) traversal.getSteps().get(2)).getHasContainers().get(0).getKey());
        assertEquals("daniel", ((TinkerGraphStep<?,?>) traversal.getSteps().get(2)).getHasContainers().get(0).getValue());
        assertEquals(TinkerGraphStep.class, traversal.getEndStep().getClass());
    }

}

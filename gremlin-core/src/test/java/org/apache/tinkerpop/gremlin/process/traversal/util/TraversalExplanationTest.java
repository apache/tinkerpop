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

package org.apache.tinkerpop.gremlin.process.traversal.util;

import java.util.List;
import java.util.stream.Stream;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.AdjacentToIncidentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.CountStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IncidentToAdjacentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.ProductiveByStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalExplanationTest {

    @Test
    public void shouldMakeImmutable() {
        final TraversalExplanation explanation = __.V().out().out().explain();
        final ImmutableExplanation immutable = explanation.asImmutable();
        assertEquals(explanation.prettyPrint(), immutable.prettyPrint());
        assertEquals(explanation.toString(), immutable.toString());
    }

    @Test
    public void shouldSupportAnonymousTraversals() {
        final String toString = __.out("knows").in("created").explain().toString();
        assertTrue(toString.contains("Traversal Explanation"));
        assertTrue(toString.contains("Original Traversal"));
        assertTrue(toString.contains("Final Traversal"));
    }

    @Test
    public void shouldWordWrapCorrectly() {
        GraphTraversal<?, ?> traversal = __.V().out().out();
        String toString = traversal.explain().prettyPrint();
        assertFalse(toString.contains("VertexStep(OUT,vertex),\n"));
        //System.out.println(toString);
        ///
        traversal = __.V().out().out().out().out();
        toString = traversal.explain().prettyPrint();
        assertTrue(toString.contains("VertexStep(OUT,vertex),"));
        //System.out.println(toString);
        ///
        for (int i = 0; i < 30; i++) {
            traversal = __.V();
            for (int j = 0; j < i; j++) {
                traversal.out();
            }
            traversal.asAdmin().setStrategies(TraversalStrategies.GlobalCache.getStrategies(Graph.class));
            toString = traversal.explain().prettyPrint();
            if (i < 3)
                assertFalse(toString.contains("VertexStep(OUT,vertex),\n"));
            else {
                assertTrue(Stream.of(toString.split("\n"))
                        .filter(s -> s.startsWith(" "))
                        .map(String::trim)
                        .filter(s -> Character.isLowerCase(s.charAt(0)))
                        .findAny()
                        .isPresent()); // all indented word wraps should start with steps
                assertTrue(toString.contains("vertex"));
            }
            for (int j = 80; j < 200; j++) {
                for (final String line : traversal.explain().prettyPrint(j).split("\n")) {
                    assertTrue(line.length() <= j);
                }
            }
            // System.out.println(toString);
        }
    }

    @Test
    public void shouldApplyStrategiesCorrectly() {
        Traversal.Admin<?, ?> traversal = __.out().count().asAdmin();
        traversal.setStrategies(TraversalStrategies.GlobalCache.getStrategies(Graph.class));
        checkTraversalExplanation(traversal.explain(), List.of(new ExplainExpectation(AdjacentToIncidentStrategy.class, "[VertexStepPlaceholder(OUT,edge)")));
        ///
        traversal = __.out().group().by(__.in().count()).asAdmin();
        traversal.setStrategies(TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone());
        checkTraversalExplanation(traversal.explain(), List.of(new ExplainExpectation(AdjacentToIncidentStrategy.class, "[VertexStepPlaceholder(IN,edge)")));
        ///
        traversal = __.outE().inV().group().by(__.inE().outV().groupCount().by(__.both().count().is(P.gt(2)))).asAdmin();
        traversal.setStrategies(TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().removeStrategies(ProductiveByStrategy.class));
        checkTraversalExplanation(traversal.explain(), List.of(new ExplainExpectation(IncidentToAdjacentStrategy.class, "[VertexStepPlaceholder(IN,vertex)"),
                new ExplainExpectation(IncidentToAdjacentStrategy.class, "[VertexStepPlaceholder(OUT,vertex)"),
                new ExplainExpectation(AdjacentToIncidentStrategy.class, "[VertexStepPlaceholder(BOTH,edge)"),
                new ExplainExpectation(CountStrategy.class, "RangeGlobalStep(0,3)")));
    }

    private void checkTraversalExplanation(final TraversalExplanation explanation, final List<ExplainExpectation> expectations) {
        // explanation string triplet consists of strategy name + strategy category + steps
        expectations.forEach(expectation -> assertTrue(expectation.toString(), explanation.getIntermediates()
                .anyMatch(t -> t.getValue0().contains(expectation.strategyClass.getSimpleName()) &&
                        t.getValue2().contains(expectation.stepSubstring))));
    }

    private static class ExplainExpectation {
        final Class<? extends TraversalStrategy<?>> strategyClass;
        final String stepSubstring;

        ExplainExpectation(final Class<? extends TraversalStrategy<?>> strategyClass, final String stepSubstring) {
            this.strategyClass = strategyClass;
            this.stepSubstring = stepSubstring;
        }

        public String toString() {
            return String.format("Expecting strategy %s and steps substring %s", strategyClass.getSimpleName(), stepSubstring);
        }
    }
}

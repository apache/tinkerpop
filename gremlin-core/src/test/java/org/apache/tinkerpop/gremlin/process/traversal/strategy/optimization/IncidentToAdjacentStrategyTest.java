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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.GValueManagerVerifier;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Set;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
@RunWith(Enclosed.class)
public class IncidentToAdjacentStrategyTest {

    @RunWith(Parameterized.class)
    public static class StandardTest {
        @Parameterized.Parameter(value = 0)
        public Traversal.Admin original;

        @Parameterized.Parameter(value = 1)
        public Traversal optimized;

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> generateTestParameters() {
            Function<Traverser<Vertex>, Vertex> lambda = Traverser::get; // to ensure same hashCode
            return Arrays.asList(new Traversal[][]{
                    {__.outE().inV(), __.out()},
                    {__.inE().outV(), __.in()},
                    {__.bothE().otherV(), __.both()},
                    {__.outE().outV(), __.outE().outV()},
                    {__.inE().inV(), __.inE().inV()},
                    {__.bothE().bothV(), __.bothE().bothV()},
                    {__.bothE().inV(), __.bothE().inV()},
                    {__.bothE().outV(), __.bothE().outV()},
                    {__.outE().otherV(), __.out()},
                    {__.inE().otherV(), __.in()},
                    {__.outE().as("a").inV(), __.outE().as("a").inV()}, // todo: this can be optimized, but requires a lot more checks
                    {__.outE().inV().path(), __.outE().inV().path()},
                    {__.outE().inV().simplePath(), __.outE().inV().simplePath()},
                    {__.outE().inV().tree(), __.outE().inV().tree()},
                    {__.outE().inV().map(lambda), __.outE().inV().map(lambda)},
                    {__.union(__.outE().inV(), __.inE().outV()).path(), __.union(__.outE().inV(), __.inE().outV()).path()},
                    {__.as("a").outE().inV().as("b"), __.as("a").out().as("b")},
                    {__.outE("link").inV(), __.out("link")},
                    {__.inE("link").outV(), __.in("link")},
                    {__.bothE("link").otherV(), __.both("link")},
            });
        }

        @Test
        public void doTest() {
            final String repr = original.getGremlinLang().getGremlin("__");
            GValueManagerVerifier.verify(original.asAdmin(), IncidentToAdjacentStrategy.instance(), Set.of(GValueReductionStrategy.instance()))
                    .afterApplying()
                    .managerIsEmpty();
            assertEquals(repr, this.optimized, this.original);
        }
    }

    /**
     * Tests that GValueManager.copyParams is being called correctly in IncidentToAdjacentStrategy
     * such that the newly added VertexStep has the same parameters that previous one had.
     */
    @RunWith(Parameterized.class)
    public static class GValueTest {

        @Parameterized.Parameter(value = 0)
        public Traversal.Admin<?, ?> traversal;

        @Parameterized.Parameter(value = 1)
        public int expectedLabelCount;

        @Parameterized.Parameter(value = 2)
        public Set<String> expectedNames;

        @Parameterized.Parameter(value = 3)
        public Set<String> expectedValues;

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {
                        __.outE(GValue.of("x", "link")).inV().asAdmin(),
                        1,
                        Set.of("x"),
                        Set.of("link")
                    },
                    {
                        __.outE(GValue.of("x", "link"), GValue.of("y", "knows"), GValue.of("z", "created")).inV().asAdmin(),
                        3,
                        Set.of("x", "y", "z"),
                        Set.of("link", "knows", "created")
                    },
                    {
                        __.inE(GValue.of("x", "link")).outV().asAdmin(),
                        1,
                        Set.of("x"),
                        Set.of("link")
                    },
                    {
                        __.inE(GValue.of("x", "link"), GValue.of("y", "knows"), GValue.of("z", "created")).outV().asAdmin(),
                        3,
                        Set.of("x", "y", "z"),
                        Set.of("link", "knows", "created")
                    },
                    {
                        __.bothE(GValue.of("x", "link")).otherV().asAdmin(),
                        1,
                        Set.of("x"),
                        Set.of("link")
                    },
                    {
                        __.bothE(GValue.of("x", "link"), GValue.of("y", "knows"), GValue.of("z", "created")).otherV().asAdmin(),
                        3,
                        Set.of("x", "y", "z"),
                        Set.of("link", "knows", "created")
                    }
            });
        }

        @Test
        public void shouldCopyGValueParameters() {
            // get reference to the start step before we apply strategies as its going to get replaced and we need
            // it to verify the before state.
            final Step step = traversal.asAdmin().getSteps().get(0);
            GValueManagerVerifier.verify(traversal, AdjacentToIncidentStrategy.instance()).
                    beforeApplying().
                        hasVariables(expectedNames).
                        isVertexStepPlaceholder(step, expectedLabelCount, expectedNames, expectedValues).
                    afterApplying().
                        hasVariables(expectedNames).
                        variablesArePreserved().
                        isVertexStepPlaceholder(traversal.getStartStep(), expectedLabelCount, expectedNames, expectedValues);
        }
    }
}

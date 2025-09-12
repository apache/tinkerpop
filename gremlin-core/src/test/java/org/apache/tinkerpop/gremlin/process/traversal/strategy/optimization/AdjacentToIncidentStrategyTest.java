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
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.GValueManagerVerifier;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.GValueReductionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.translator.GroovyTranslator;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
@RunWith(Enclosed.class)
public class AdjacentToIncidentStrategyTest {
    private static final Translator.ScriptTranslator translator = GroovyTranslator.of("__");

    @RunWith(Parameterized.class)
    public static class StandardTest {
        @Parameterized.Parameter(value = 0)
        public Traversal.Admin original;

        @Parameterized.Parameter(value = 1)
        public Traversal optimized;

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Traversal[][]{
                    {__.outE().count(), __.outE().count()},
                    {__.bothE("knows").count(), __.bothE("knows").count()},
                    {__.properties().count(), __.properties().count()},
                    {__.properties("name").count(), __.properties("name").count()},
                    {__.out().count(), __.outE().count()},
                    {__.in().count(), __.inE().count()},
                    {__.both().count(), __.bothE().count()},
                    {__.out("knows").count(), __.outE("knows").count()},
                    {__.out("knows", "likes").count(), __.outE("knows", "likes").count()},
                    {__.filter(__.out()), __.filter(__.outE())},
                    {__.where(__.not(__.out())), __.where(__.not(__.outE()))},
                    {__.where(__.out("knows")), __.where(__.outE("knows"))},
                    {__.values().count(), __.properties().count()},
                    {__.values("name").count(), __.properties("name").count()},
                    {__.where(__.values()), __.where(__.properties())},
                    {__.and(__.out(), __.in()), __.and(__.outE(), __.inE())},
                    {__.or(__.out(), __.in()), __.or(__.outE(), __.inE())},
                    {__.out().as("a").count(), __.outE().count()},   // TODO: is this good?
                    {__.where(__.as("a").out("knows").as("b")), __.where(__.as("a").out("knows").as("b"))}});
        }

        @Test
        public void doTest() {
            final String repr = translator.translate(original.getBytecode()).getScript();
            GValueManagerVerifier.verify(original.asAdmin(), AdjacentToIncidentStrategy.instance(), Set.of(GValueReductionStrategy.instance()))
                    .afterApplying()
                    .managerIsEmpty();
            assertEquals(repr, optimized, original);
        }
    }

    /**
     * Tests that GValueManager.copyParams is being called correctly in AdjacentToIncidentStrategy
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
                        __.out(GValue.of("x", "link")).count().asAdmin(),
                        1,
                        Set.of("x"),
                        Set.of("link")
                    },
                    {
                        __.out(GValue.of("x", "link"), GValue.of("y", "knows"), GValue.of("z", "created")).count().asAdmin(),
                        3,
                        Set.of("x", "y", "z"),
                        Set.of("link", "knows", "created")
                    },
                    {
                        __.in(GValue.of("x", "link")).count().asAdmin(),
                        1,
                        Set.of("x"),
                        Set.of("link")
                    },
                    {
                        __.in(GValue.of("x", "link"), GValue.of("y", "knows"), GValue.of("z", "created")).count().asAdmin(),
                        3,
                        Set.of("x", "y", "z"),
                        Set.of("link", "knows", "created")
                    },
                    {
                        __.both(GValue.of("x", "link")).count().asAdmin(),
                        1,
                        Set.of("x"),
                        Set.of("link")
                    },
                    {
                        __.both(GValue.of("x", "link"), GValue.of("y", "knows"), GValue.of("z", "created")).count().asAdmin(),
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

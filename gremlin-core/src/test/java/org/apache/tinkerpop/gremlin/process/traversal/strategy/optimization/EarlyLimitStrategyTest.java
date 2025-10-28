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

import org.apache.tinkerpop.gremlin.process.traversal.GValueManager;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.GValueManagerVerifier;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ProfileStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.translator.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.util.CollectionUtil;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.aggregate;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
@RunWith(Enclosed.class)
public class EarlyLimitStrategyTest {
    private static final Translator.ScriptTranslator translator = GroovyTranslator.of("__");

    @RunWith(Parameterized.class)
    public static class StandardTest {
        @Parameterized.Parameter()
        public Traversal.Admin original;

        @Parameterized.Parameter(value = 1)
        public Traversal optimized;

        @Parameterized.Parameter(value = 2)
        public Collection<TraversalStrategy> otherStrategies;

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {__.out().valueMap().limit(1), __.out().limit(1).valueMap(), Collections.emptyList()},
                    {__.out().limit(5).valueMap().range(5, 10), __.start().out().discard(), Collections.emptyList()},
                    {__.out().limit(5).valueMap().range(6, 10), __.start().out().discard(), Collections.emptyList()},
                    {__.V().out().valueMap().limit(1), __.V().out().limit(1).valueMap(), Collections.singleton(LazyBarrierStrategy.instance())},
                    {__.out().out().limit(1).in().in(), __.out().out().limit(1).in().barrier(LazyBarrierStrategy.MAX_BARRIER_SIZE).in(), Collections.singleton(LazyBarrierStrategy.instance())},
                    {__.out().has("name","marko").limit(1).in().in(), __.out().has("name","marko").limit(1).in().in(), Collections.emptyList()},
                    {__.out().map(__.identity()).map(__.identity()).limit(1), __.out().limit(1).map(__.identity()).map(__.identity()), Collections.singleton(LazyBarrierStrategy.instance())},
                    {__.out().map(__.identity()).map(__.identity()).limit(1).as("a"), __.out().limit(1).map(__.identity()).map(__.identity()).as("a"), Collections.singleton(LazyBarrierStrategy.instance())},
                    {__.out().map(__.identity()).map(__.identity()).limit(2).out().map(__.identity()).map(__.identity()).limit(1), __.out().limit(2).map(__.identity()).map(__.identity()).out().limit(1).map(__.identity()).map(__.identity()), Collections.emptyList()},
                    {__.out().map(__.identity()).map(__.identity()).limit(2).map(__.identity()).map(__.identity()).limit(1), __.out().limit(1).map(__.identity()).map(__.identity()).map(__.identity()).map(__.identity()), Collections.emptyList()},
                    {__.out().map(__.identity()).map(__.identity()).range(5, 20).map(__.identity()).map(__.identity()).range(5, 10), __.out().range(10, 15).map(__.identity()).map(__.identity()).map(__.identity()).map(__.identity()), Collections.emptyList()},
                    {__.out().map(__.identity()).map(__.identity()).range(50, 100).map(__.identity()).map(__.identity()).range(10, 50), __.out().range(60, 100).map(__.identity()).map(__.identity()).map(__.identity()).map(__.identity()), Collections.emptyList()},
                    {__.out().map(__.identity()).map(__.identity()).range(50, 100).map(__.identity()).map(__.identity()).range(10, 60), __.out().range(60, 100).map(__.identity()).map(__.identity()).map(__.identity()).map(__.identity()), Collections.emptyList()},
                    {__.out().map(__.identity()).map(__.identity()).range(50, -1).map(__.identity()).map(__.identity()).range(10, 60), __.out().range(60, 110).map(__.identity()).map(__.identity()).map(__.identity()).map(__.identity()), Collections.emptyList()},
                    {__.out().map(__.identity()).map(__.identity()).range(50, 100).map(__.identity()).map(__.identity()).range(10, -1), __.out().range(60, 100).map(__.identity()).map(__.identity()).map(__.identity()).map(__.identity()), Collections.emptyList()},
                    {__.out().map(__.identity()).map(__.identity()).range(50, 100).as("a").map(__.identity()).map(__.identity()).range(10, -1).as("b"), __.out().range(60, 100).map(__.identity()).map(__.identity()).as("a").map(__.identity()).map(__.identity()).as("b"), Collections.emptyList()},
                    {__.out().map(__.identity()).map(__.identity()).range(50, 100).map(__.identity()).map(__.identity()).range(50, -1), __.out().discard(), Collections.emptyList()},
                    {__.out().map(__.identity()).map(__.identity()).range(50, 100).map(__.identity()).map(__.identity()).range(60, -1), __.out().discard(), Collections.emptyList()},
                    {__.out().map(__.identity()).map(__.identity()).range(50, 100).as("a").map(__.identity()).map(__.identity()).range(60, -1).as("b"), __.out().discard(), Collections.emptyList()},
                    {__.out().range(50, 100).map(__.identity()).range(50, -1).profile(), __.out().discard().profile(), Collections.singleton(ProfileStrategy.instance())},
                    {__.out().aggregate("a").limit(10), __.out().aggregate("a").limit(10), Collections.emptyList()},
                    {__.V().branch(__.label()).option("person", __.out("knows").valueMap().limit(1)).option("software", __.out("created").valueMap().limit(2).fold()),
                            __.V().branch(__.label()).option("person", __.out("knows").limit(1).valueMap()).option("software", __.out("created").limit(2).valueMap().fold()), Collections.emptyList()}
            });
        }

        @Test
        public void doTest() {
            final String repr = translator.translate(original.getBytecode()).getScript();
            final TraversalStrategies strategies = new DefaultTraversalStrategies();
            strategies.addStrategies(EarlyLimitStrategy.instance());
            for (final TraversalStrategy strategy : this.otherStrategies) {
                strategies.addStrategies(strategy);
                if (strategy instanceof ProfileStrategy) {
                    final TraversalStrategies os = new DefaultTraversalStrategies();
                    os.addStrategies(ProfileStrategy.instance());
                    this.optimized.asAdmin().setStrategies(os);
                    this.optimized.asAdmin().applyStrategies();
                }
            }
            this.original.asAdmin().setStrategies(strategies);
            this.original.asAdmin().applyStrategies();
            assertEquals(repr, this.optimized, this.original);

            assertThat(original.asAdmin().getGValueManager().hasVariables(), is(false));
        }
    }

    /**
     * Tests that {@link GValueManager} is properly maintaining state in cases where new {@link RangeGlobalStep}
     * instances are being introduced.
     */
    @RunWith(Parameterized.class)
    public static class GValuePropagatedTest {

        @Parameterized.Parameter(value = 0)
        public Traversal.Admin<?, ?> traversal;

        @Parameterized.Parameter(value = 1)
        public int expectedLowRange;

        @Parameterized.Parameter(value = 2)
        public int expectedHighRange;

        @Parameterized.Parameter(value = 3)
        public String[] expectedNames;

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {
                        __.out().map(__.identity()).range(GValue.of("x", 5L), GValue.of("y", 10L)).asAdmin(),
                        5,
                        10,
                        new String[]{"x", "y"}
                    },
                    {
                        __.out().map(__.identity()).range(GValue.of("x", 5L), GValue.of("y", -1L)).asAdmin(),
                        5,
                        -1,
                        new String[]{"x", "y"}
                    },
                    {
                        __.out().map(__.identity()).map(__.identity()).range(GValue.of("x", 5L), GValue.of("y", 10L)).asAdmin(),
                        5,
                        10,
                        new String[]{"x", "y"}
                    },
                    {
                        __.out().map(__.identity()).map(__.identity()).range(GValue.of(5L), GValue.of("y", 10L)).asAdmin(),
                        5,
                        10,
                        new String[]{null, "y"}
                    },
                    {
                        __.out().valueMap().limit(GValue.of("x", 10L)).asAdmin(),
                        0,
                        10,
                        new String[]{null, "x"}
                    },
                    {
                        __.out().map(__.identity()).limit(GValue.of("z", 15L)).asAdmin(),
                        0,
                        15,
                        new String[]{null, "z"}
                    },
                    {
                        __.out().map(__.identity()).skip(GValue.of("w", 8L)).asAdmin(),
                        8,
                        -1,
                        new String[]{"w", null}
                    }
            });
        }

        @Test
        public void shouldMaintainGValueManagerState() {
            final Optional<RangeGlobalStepContract> maybeOriginalRangeGlobalStep = TraversalHelper.getFirstStepOfAssignableClass(RangeGlobalStepContract.class, traversal);
            assertThat(maybeOriginalRangeGlobalStep.isPresent(), is(true));
            final RangeGlobalStepContract originalRangeGlobalStep = maybeOriginalRangeGlobalStep.get();

            // Use GValueManagerVerifier to verify the state before and after applying the strategy
            GValueManagerVerifier.verify(traversal, EarlyLimitStrategy.instance()).
                    beforeApplying().
                    isRangeGlobalStepPlaceholder(originalRangeGlobalStep, expectedLowRange, expectedHighRange, expectedNames[0], expectedNames[1]).
                    afterApplying().
                    isRangeGlobalStepPlaceholder(
                                TraversalHelper.getFirstStepOfAssignableClass(RangeGlobalStepContract.class, traversal).get(),
                                expectedLowRange, expectedHighRange, expectedNames[0], expectedNames[1]).
                    hasVariables(CollectionUtil.asSet(Arrays.stream(expectedNames).filter(Objects::nonNull).toArray(String[]::new)));
        }
    }

    /**
     * Tests that {@link GValueManager} is properly maintaining state in cases where {@link RangeGlobalStep} is just
     * moved around but not copied.
     */
    @RunWith(Parameterized.class)
    public static class GValueLeftAloneTest {

        @Parameterized.Parameter(value = 0)
        public Traversal.Admin<?, ?> traversal;

        @Parameterized.Parameter(value = 1)
        public int expectedLowRange;

        @Parameterized.Parameter(value = 2)
        public int expectedHighRange;

        @Parameterized.Parameter(value = 3)
        public String[] expectedNames;

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {
                            __.out().range(GValue.of("x", 5L), GValue.of("y", -1L)).
                                    map(__.identity()).asAdmin(),
                            5,
                            -1,
                            new String[]{"x", "y"}
                    },
                    {
                            __.out().limit(GValue.of("z", 10L)).
                                    map(__.identity()).asAdmin(),
                            0,
                            10,
                            new String[]{null, "z"}
                    },
                    {
                            __.out().skip(GValue.of("w", 7L)).
                                    map(__.identity()).asAdmin(),
                            7,
                            -1,
                            new String[]{"w", null}
                    }
            });
        }

        @Test
        public void shouldMaintainGValueManagerState() {
            final Optional<RangeGlobalStepContract> maybeOriginalRangeGlobalStep = TraversalHelper.getFirstStepOfAssignableClass(RangeGlobalStepContract.class, traversal);
            assertThat(maybeOriginalRangeGlobalStep.isPresent(), is(true));
            final RangeGlobalStepContract originalRangeGlobalStep = maybeOriginalRangeGlobalStep.get();

            // Use GValueManagerVerifier to verify the state before and after applying the strategy
            GValueManagerVerifier.verify(traversal, EarlyLimitStrategy.instance()).
                    beforeApplying().
                    isRangeGlobalStepPlaceholder(originalRangeGlobalStep, expectedLowRange, expectedHighRange, expectedNames[0], expectedNames[1]).
                    afterApplying().
                    isRangeGlobalStepPlaceholder(TraversalHelper.getFirstStepOfAssignableClass(RangeGlobalStepContract.class, traversal).get(),expectedLowRange, expectedHighRange, expectedNames[0], expectedNames[1]).
                        hasVariables(CollectionUtil.asSet(Arrays.stream(expectedNames).filter(Objects::nonNull).toArray(String[]::new)));
        }
    }

    /**
     * Tests that {@link GValueManager} is properly maintaining state in cases where {@link RangeGlobalStep} is merged
     * and the manager's state must be cleared.
     */
    @RunWith(Parameterized.class)
    public static class GValueExpectClearStateTest {

        @Parameterized.Parameter(value = 0)
        public Traversal.Admin<?, ?> traversal;

        @Parameterized.Parameter(value = 1)
        public Set<String> expectedVariables;

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {
                        __.out().range(GValue.of("x", 5L), GValue.of("y", -1L)).
                                map(__.identity()).
                                range(GValue.of("z", 10L), GValue.of("w", 60L)).asAdmin(),
                        CollectionUtil.asSet("x", "y", "z", "w")
                    },
                    {
                        __.out().limit(GValue.of("a", 15L)).
                                map(__.identity()).
                                limit(GValue.of("b", 10L)).asAdmin(),
                        CollectionUtil.asSet("a", "b")
                    },
                    {
                        __.out().skip(GValue.of("c", 5L)).
                                map(__.identity()).
                                limit(GValue.of("d", 20L)).asAdmin(),
                        CollectionUtil.asSet("c", "d")
                    },
                    {
                        __.out().limit(GValue.of("e", 30L)).
                                map(__.identity()).
                                skip(GValue.of("f", 10L)).asAdmin(),
                        CollectionUtil.asSet("e", "f")
                    }
            });
        }

        @Test
        public void shouldMaintainGValueManagerState() {
            // Use GValueManagerVerifier to verify the state before and after applying the strategy
            GValueManagerVerifier.verify(traversal, EarlyLimitStrategy.instance(), Collections.singletonList(GValueReductionStrategy.instance())).
                    beforeApplying().
                        hasVariables(expectedVariables).
                    afterApplying().
                        managerIsEmpty();
        }
    }
}

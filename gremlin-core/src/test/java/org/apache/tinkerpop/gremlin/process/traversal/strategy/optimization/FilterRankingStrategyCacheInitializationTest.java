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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.where;

/**
 * Testing whether the cache for the FilterRankingStrategy is correctly initialized,
 * which includes gathering/collecting the traversal parents and their children steps
 * in the correct deep first manner.
 */
@RunWith(JUnit4.class)
public class FilterRankingStrategyCacheInitializationTest {


    @Test
    public void shouldCorrectlyInitializeTraversalParentCacheForNestedLambdas() {
        // traversal
        final Traversal<?, ?> traversal = __.V().as("n").
                where(
                        __.or(
                                __.select("n").hasLabel("software"),
                                __.select("n").map(Lambda.function("it.get().out()"))
                        )
                ).
                select("n").by("name");

        // expected order of traversal parents
        final List<Traversal<?, ?>> collectedTraversalParents = Arrays.asList(
                __.or(
                        __.select("n").hasLabel("software"),
                        __.select("n").map(Lambda.function("it.get().out()"))
                ),
                __.where(
                        __.or(
                                __.select("n").hasLabel("software"),
                                __.select("n").map(Lambda.function("it.get().out()"))
                        )
                ));

        // cache entries
        final List<Pair<Boolean, Set<String>>> expectedTraversalParentCacheEntries = Arrays.asList(
                new Pair<>(true, new HashSet<>()),
                new Pair<>(true, new HashSet<>()),
                new Pair<>(true, new HashSet<>()) // last entry for empty step/root traversal parent
        );

        doTest(traversal, Optional.of(collectedTraversalParents), Optional.of(expectedTraversalParentCacheEntries));
    }


    @Test
    public void shouldCorrectlyInitializeTraversalParentCacheForNestedWhereOrSteps() {
        // traversal
        final Traversal<?, ?> traversal = __.V().as("n").
                where(
                        __.or(
                                __.select("n").hasLabel("software"),
                                __.select("n").hasLabel("person")
                        )
                ).
                select("n").by("name");

        // expected order of traversal parents
        final List<Traversal<?, ?>> collectedTraversalParents = Arrays.asList(
                __.or(
                        __.select("n").hasLabel("software"),
                        __.select("n").hasLabel("person")
                ),
                __.where(
                        __.or(
                                __.select("n").hasLabel("software"),
                                __.select("n").hasLabel("person")
                        )
                ));

        // cache entries
        final List<Pair<Boolean, Set<String>>> expectedTraversalParentCacheEntries = Arrays.asList(
                new Pair<>(false, new HashSet<>(Collections.singletonList("n"))),
                new Pair<>(false, new HashSet<>(Collections.singletonList("n"))),
                new Pair<>(false, new HashSet<>(Collections.singletonList("n"))) // last entry for empty step/root traversal parent
        );

        doTest(traversal, Optional.of(collectedTraversalParents), Optional.of(expectedTraversalParentCacheEntries));
    }


    @Test
    public void shouldCorrectlyInitializeTraversalParentCacheForNestedWhereOrStepsWithDifferentLabels() {
        // traversal
        final Traversal<?, ?> traversal = __.V().as("n").out().as("m").
                where(
                        __.or(
                                __.select("n").hasLabel("software"),
                                __.select("m").hasLabel("person")
                        )
                ).
                select("n").by("name");

        // expected order of traversal parents
        final List<Traversal<?, ?>> collectedTraversalParents = Arrays.asList(
                __.or(
                        __.select("n").hasLabel("software"),
                        __.select("m").hasLabel("person")
                ),
                __.where(
                        __.or(
                                __.select("n").hasLabel("software"),
                                __.select("m").hasLabel("person")
                        )
                ));

        // cache entries
        final List<Pair<Boolean, Set<String>>> expectedTraversalParentCacheEntries = Arrays.asList(
                new Pair<>(false, new HashSet<>(Arrays.asList("n", "m"))),
                new Pair<>(false, new HashSet<>(Arrays.asList("n", "m"))),
                new Pair<>(false, new HashSet<>(Arrays.asList("n", "m"))) // last entry for empty step/root traversal parent
        );

        doTest(traversal, Optional.of(collectedTraversalParents), Optional.of(expectedTraversalParentCacheEntries));
    }


    @Test
    public void shouldCorrectlyInitializeTraversalParentCacheForNestedUnionOrStepsWithDifferentLabels() {
        // traversal
        final Traversal<?, ?> traversal = __.V().as("n").out().as("m").
                union(
                        __.or(
                                __.select("n").hasLabel("software"),
                                __.select("m").hasLabel("person")
                        ),
                        __.or(
                                __.select("n").hasLabel("person")
                        ),
                        __.or(
                                __.hasLabel("person").as("x").select("x")
                        )
                );

        // expected order of traversal parents
        final List<Traversal<?, ?>> collectedTraversalParents = Arrays.asList(
                __.or(
                        __.select("n").hasLabel("software"),
                        __.select("m").hasLabel("person")
                ),
                __.or(
                        __.select("n").hasLabel("person")
                ),
                __.or(
                        __.hasLabel("person").as("x").select("x")
                ),
                __.union(
                        __.or(
                                __.select("n").hasLabel("software"),
                                __.select("m").hasLabel("person")
                        ),
                        __.or(
                                __.select("n").hasLabel("person")
                        ),
                        __.or(
                                __.hasLabel("person").as("x").select("x")
                        )
                ));

        // cache entries
        final List<Pair<Boolean, Set<String>>> expectedTraversalParentCacheEntries = Arrays.asList(
                new Pair<>(false, new HashSet<>(Arrays.asList("n", "m"))),
                new Pair<>(false, new HashSet<>(Collections.singletonList("n"))),
                new Pair<>(false, new HashSet<>(Collections.singletonList("x"))),
                new Pair<>(false, new HashSet<>(Arrays.asList("n", "m", "x"))),
                new Pair<>(false, new HashSet<>(Arrays.asList("n", "m", "x"))) // last entry for empty step/root traversal parent
        );

        doTest(traversal, Optional.of(collectedTraversalParents), Optional.of(expectedTraversalParentCacheEntries));
    }


    @Test
    public void shouldCorrectlyInitializeTraversalParentCacheForNestedWhereUnionSteps() {
        // traversal
        final Traversal<?, ?> traversal = __.V().as("n").
                where(
                        __.or(
                                __.select("n").hasLabel("software"),
                                __.select("n").hasLabel("person")
                        )
                ).
                union(
                        __.select("n").hasLabel("software"),
                        __.select("n").hasLabel("person")
                ).
                select("n").by("name");

        // expected order of traversal parents
        final List<Traversal<?, ?>> collectedTraversalParents = Arrays.asList(
                __.or(
                        __.select("n").hasLabel("software"),
                        __.select("n").hasLabel("person")
                ),
                __.where(
                        __.or(
                                __.select("n").hasLabel("software"),
                                __.select("n").hasLabel("person")
                        )
                ),
                __.union(
                        __.select("n").hasLabel("software"),
                        __.select("n").hasLabel("person")
                )
        );

        // cache entries
        final List<Pair<Boolean, Set<String>>> expectedTraversalParentCacheEntries = Arrays.asList(
                new Pair<>(false, new HashSet<>(Collections.singletonList("n"))),
                new Pair<>(false, new HashSet<>(Collections.singletonList("n"))),
                new Pair<>(false, new HashSet<>(Collections.singletonList("n"))),
                new Pair<>(false, new HashSet<>(Collections.singletonList("n"))) // last entry for empty step/root traversal parent
        );

        doTest(traversal, Optional.of(collectedTraversalParents), Optional.of(expectedTraversalParentCacheEntries));
    }


    @Test
    public void shouldCorrectlyInitializeTraversalParentCacheForDeeplyNestedWhereUnionSteps() {
        // traversal
        final Traversal<?, ?> traversal = __.V().as("n").
                where(
                        where(
                                __.or(
                                        __.select("n").hasLabel("software"),
                                        __.select("n").hasLabel("person")
                                )
                        ).
                                union(
                                        __.select("n").hasLabel("software"),
                                        __.select("n").hasLabel("person")
                                ).
                                select("n").by("name")
                );

        // expected order of traversal parents
        final List<Traversal<?, ?>> collectedTraversalParents = Arrays.asList(new Traversal<?, ?>[]{
                __.or(
                        __.select("n").hasLabel("software"),
                        __.select("n").hasLabel("person")
                ),
                __.where(
                        __.or(
                                __.select("n").hasLabel("software"),
                                __.select("n").hasLabel("person")
                        )
                ),
                __.union(
                        __.select("n").hasLabel("software"),
                        __.select("n").hasLabel("person")
                ),
                where(
                        where(
                                __.or(
                                        __.select("n").hasLabel("software"),
                                        __.select("n").hasLabel("person")
                                )
                        ).
                                union(
                                        __.select("n").hasLabel("software"),
                                        __.select("n").hasLabel("person")
                                ).
                                select("n").by("name")
                )
        });

        // cache entries
        final List<Pair<Boolean, Set<String>>> expectedTraversalParentCacheEntries = Arrays.asList(
                new Pair<>(false, new HashSet<>(Collections.singletonList("n"))),
                new Pair<>(false, new HashSet<>(Collections.singletonList("n"))),
                new Pair<>(false, new HashSet<>(Collections.singletonList("n"))),
                new Pair<>(false, new HashSet<>(Collections.singletonList("n"))),
                new Pair<>(false, new HashSet<>(Collections.singletonList("n"))) // last entry for empty step/root traversal parent
        );

        doTest(traversal, Optional.of(collectedTraversalParents), Optional.of(expectedTraversalParentCacheEntries));
    }

    @Test
    public void shouldCorrectlyInitializeTraversalParentCacheForDeeplyNestedWhereUnionSteps2() {
        // traversal
        final Traversal<?, ?> traversal = __.V().as("n").
                where(
                        where(
                                where(
                                        __.or(
                                                __.select("n").hasLabel("software"),
                                                __.select("n").hasLabel("person")
                                        )
                                ).
                                        union(
                                                __.select("n").hasLabel("software"),
                                                __.select("n").hasLabel("person")
                                        ).
                                        select("n").by("name")
                        )
                );

        // no cache entries/no expected order of traversal parents, so the method will just compare with old collection method
        doTest(traversal, Optional.empty(), Optional.empty());
    }


    @Test
    public void shouldCorrectlyInitializeTraversalParentCacheForDeeplyNestedWhereUnionSteps3() {
        // traversal
        final Traversal<?, ?> traversal = __.V().as("n").
                where(
                        where(
                                where(
                                        __.or(
                                                __.select("n").hasLabel("software1"),
                                                __.select("n").hasLabel("person1")
                                        )
                                ).
                                        union(
                                                __.select("n").hasLabel("software2"),
                                                __.select("n").hasLabel("person2")
                                        ).
                                        select("n").by("name")
                        ).
                                union(
                                        __.or(
                                                __.select("n").hasLabel("abc"),
                                                __.select("n").hasLabel("123")
                                        ),
                                        __.select("m").hasLabel("person")
                                ).
                                select("m").by("name")
                );

        // no cache entries/no expected order of traversal parents, so the method will just compare with old collection method
        doTest(traversal, Optional.empty(), Optional.empty());
    }

    public void doTest(final Traversal<?, ?> traversal,
                       final Optional<List<Traversal<?, ?>>> collectedTraversalParents,
                       final Optional<List<Pair<Boolean, Set<String>>>> expectedTraversalParentCacheEntries) {

        List<Pair<TraversalParent, List<Step<?, ?>>>> traversalParentsStepsCollection =
                FilterRankingStrategy.collectStepsOfAssignableClassRecursivelyFromDepthGroupedByParent(traversal.asAdmin());
        Map<TraversalParent, Pair<Boolean, Set<String>>> traversalParentCache = FilterRankingStrategy.getTraversalParentCache(traversalParentsStepsCollection);

        // Old method of collecting traversal parents and their children steps, which can be significantly less efficient
        // when hash codes of very complex steps must be computing. Replacing LinkedHashMap with IdentityHashMap will show some failures
        final Map<TraversalParent, List<Step<?, ?>>> traversalParentsStepsMap =
                TraversalHelper.getStepsOfAssignableClassRecursivelyFromDepth(traversal.asAdmin(), TraversalParent.class, LambdaHolder.class).stream().
                        collect(Collectors.groupingBy(step -> ((Step<?, ?>) step).getTraversal().getParent(), LinkedHashMap::new, Collectors.toList()));
        // Note that the new method does not result in exactly the same list as traversal parents on the same level/depth are ordered
        // differently, i.e., we cannot compare traversalParentsStepsCollection and oldTraversalParentsStepsCollection.
        // The new collection method leads however also to a correct initialization of the cache.
        List<Pair<TraversalParent, List<Step<?, ?>>>> oldTraversalParentsStepsCollection = new ArrayList<>();
        traversalParentsStepsMap.forEach((k, v) -> {
            oldTraversalParentsStepsCollection.add(new Pair<>(k, v));
        });
        // computing the cache on the old way of collecting the traversal parents
        Map<TraversalParent, Pair<Boolean, Set<String>>> oldCollectionTraversalParentCache = FilterRankingStrategy.getTraversalParentCache(oldTraversalParentsStepsCollection);

        expectedTraversalParentCacheEntries.ifPresent(pairs -> Assert.assertEquals(pairs.size(), traversalParentCache.size()));

        if (collectedTraversalParents.isPresent() && expectedTraversalParentCacheEntries.isPresent()) {
            for (int i = 0; i < collectedTraversalParents.get().size(); ++i) {
                final Pair<TraversalParent, List<Step<?, ?>>> traversalParentStepListPair = traversalParentsStepsCollection.get(i);
                final TraversalParent traversalParent = traversalParentStepListPair.getValue0();
                final Traversal<?, ?> expectedTraversalParent = collectedTraversalParents.get().get(i);
                Assert.assertEquals(((DefaultGraphTraversal<?, ?>) expectedTraversalParent).getSteps().get(0), traversalParent);

                final Pair<Boolean, Set<String>> expectedCacheEntry = expectedTraversalParentCacheEntries.get().get(i);
                final Pair<Boolean, Set<String>> cacheEntry = traversalParentCache.get(traversalParent);
                Assert.assertEquals(expectedCacheEntry, cacheEntry);
            }

            Assert.assertEquals(collectedTraversalParents.get().size() + 1, traversalParentsStepsCollection.size());
            Assert.assertEquals(traversalParentsStepsCollection.get(collectedTraversalParents.get().size()).getValue0(), EmptyStep.instance());
            final Pair<Boolean, Set<String>> expectedCacheEntry = expectedTraversalParentCacheEntries.get().get(collectedTraversalParents.get().size());
            final Pair<Boolean, Set<String>> cacheEntry = traversalParentCache.get(EmptyStep.instance());
            Assert.assertEquals(expectedCacheEntry, cacheEntry);
        }

        Assert.assertEquals(oldCollectionTraversalParentCache, traversalParentCache);
    }

}



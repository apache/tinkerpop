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

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.util.function.ConstantSupplier;
import org.apache.tinkerpop.gremlin.util.function.HashSetSupplier;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversalTest {

    @Test
    public void shouldRespectThreadInterruption() throws Exception {
        final AtomicBoolean exceptionThrown = new AtomicBoolean(false);
        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch startedIterating = new CountDownLatch(100);
        final List<Integer> l = IntStream.range(0, 1000000).boxed().collect(Collectors.toList());
        final Thread t = new Thread(() -> {
            try {
                __.inject(l).unfold().sideEffect(i -> {
                    startedIterating.countDown();
                    counter.incrementAndGet();
                }).iterate();
            } catch (Exception ex) {
                exceptionThrown.set(ex instanceof TraversalInterruptedException);
            }
        });

        t.start();
        startedIterating.await();
        t.interrupt();
        t.join();

        // ensure that some but not all of the traversal was iterated and that the right exception was tossed
        assertThat(counter.get(), greaterThan(0));
        assertThat(counter.get(), lessThan(1000000));
        assertThat(exceptionThrown.get(), CoreMatchers.is(true));
    }

    @Test
    public void shouldCloneTraversalCorrectly() {
        final DefaultGraphTraversal<?, ?> original = new DefaultGraphTraversal<>();
        original.out().groupCount("m").values("name").count();
        final DefaultTraversal<?, ?> clone = (DefaultTraversal) original.clone();
        assertEquals(original.hashCode(), clone.hashCode());
        assertEquals(original.getSteps().size(), clone.getSteps().size());

        for (int i = 0; i < original.steps.size(); i++) {
            assertEquals(original.getSteps().get(i), clone.getSteps().get(i));
        }
        assertNotEquals(original.sideEffects, clone.sideEffects);
        original.getSideEffects().set("m", 1);
        assertEquals(1, original.getSideEffects().<Integer>get("m").intValue());
        clone.getSideEffects().set("m", 2);
        assertEquals(1, original.getSideEffects().<Integer>get("m").intValue());
        assertEquals(2, clone.getSideEffects().<Integer>get("m").intValue());
        assertNotSame(original.gremlinLang, clone.gremlinLang);
        assertEquals(original.closed, clone.closed);
    }

    @Test
    public void shouldBeTheSameSideEffectsThroughoutAllChildTraversals() {
        final DefaultTraversal.Admin<?, ?> traversal = (DefaultTraversal.Admin) __.out().repeat(__.in().groupCount("a").by(__.select("a"))).in();
        final TraversalSideEffects sideEffects = traversal.getSideEffects();
        sideEffects.register("a", (Supplier) HashSetSupplier.instance(), Operator.addAll);
        sideEffects.register("b", new ConstantSupplier<>(1), Operator.sum);
        assertEquals(1, sideEffects.<Integer>get("b").intValue());
        assertFalse(traversal.isLocked());
        traversal.applyStrategies();
        assertTrue(traversal.isLocked());
        sideEffects.add("b", 7);
        assertEquals(0, sideEffects.<Set>get("a").size());
        assertEquals(8, sideEffects.<Integer>get("b").intValue());
        recursiveTestTraversals(traversal, sideEffects, sideEffects.get("a"), 8);
        sideEffects.add("a", new HashSet<>(Arrays.asList("marko", "bob")));
        sideEffects.set("b", 3);
        recursiveTestTraversals(traversal, sideEffects, new HashSet<>(Arrays.asList("marko", "bob")), 3);
        sideEffects.add("a", new HashSet<>(Arrays.asList("marko", "x", "x", "bob")));
        sideEffects.add("b", 10);
        recursiveTestTraversals(traversal, sideEffects, new HashSet<>(Arrays.asList("marko", "bob", "x")), 13);
    }

    private void recursiveTestTraversals(final Traversal.Admin<?, ?> traversal, final TraversalSideEffects sideEffects, final Set aValue, final int bValue) {
        assertTrue(traversal.getSideEffects() == sideEffects);
        assertEquals(sideEffects.keys().size(), traversal.getSideEffects().keys().size());
        assertEquals(bValue, traversal.getSideEffects().<Integer>get("b").intValue());
        assertEquals(aValue.size(), traversal.getSideEffects().<Set>get("a").size());
        assertFalse(aValue.stream().filter(k -> !traversal.getSideEffects().<Set>get("a").contains(k)).findAny().isPresent());
        assertFalse(traversal.getSideEffects().exists("c"));
        for (final Step<?, ?> step : traversal.getSteps()) {
            assertTrue(step.getTraversal().getSideEffects() == sideEffects);
            assertEquals(sideEffects.keys().size(), step.getTraversal().getSideEffects().keys().size());
            assertEquals(bValue, step.getTraversal().getSideEffects().<Integer>get("b").intValue());
            assertEquals(aValue.size(), step.getTraversal().getSideEffects().<Set>get("a").size());
            assertFalse(aValue.stream().filter(k -> !step.getTraversal().getSideEffects().<Set>get("a").contains(k)).findAny().isPresent());
            assertFalse(step.getTraversal().getSideEffects().exists("c"));
            if (step instanceof TraversalParent) {
                ((TraversalParent) step).getGlobalChildren().forEach(t -> this.recursiveTestTraversals(t, sideEffects, aValue, bValue));
                ((TraversalParent) step).getLocalChildren().forEach(t -> this.recursiveTestTraversals(t, sideEffects, aValue, bValue));
            }
        }
    }
}

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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.Traverser;
import org.apache.tinkerpop.gremlin.process.graph.traversal.__;
import org.apache.tinkerpop.gremlin.process.util.metric.Metrics;
import org.apache.tinkerpop.gremlin.process.util.metric.StandardTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.util.metric.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.graph.traversal.__.both;
import static org.junit.Assert.*;

/**
 * @author Bob Briody (http://bobbriody.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class ProfileTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, StandardTraversalMetrics> get_g_V_out_out_profile();

    public abstract Traversal<Vertex, StandardTraversalMetrics> get_g_V_repeat_both_profile();

    public abstract Traversal<Vertex, StandardTraversalMetrics> get_nested_profile();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_modern_profile() {
        final Traversal<Vertex, StandardTraversalMetrics> traversal = get_g_V_out_out_profile();
        printTraversalForm(traversal);

        traversal.iterate();

        final TraversalMetrics traversalMetrics = traversal.asAdmin().getSideEffects().get(TraversalMetrics.METRICS_KEY);
        traversalMetrics.toString(); // ensure no exceptions are thrown

        Metrics metrics = traversalMetrics.getMetrics(0);
        assertEquals(6, metrics.getCount(Metrics.TRAVERSER_COUNT_ID));
        assertEquals(6, metrics.getCount(Metrics.ELEMENT_COUNT_ID));
        assertTrue("Percent duration should be positive.", Double.valueOf(metrics.getAnnotation(Metrics.PERCENT_DURATION_KEY)) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        metrics = traversalMetrics.getMetrics(1);
        assertEquals(6, metrics.getCount(Metrics.ELEMENT_COUNT_ID));
        assertNotEquals(0, metrics.getCount(Metrics.TRAVERSER_COUNT_ID));
        assertTrue("Percent duration should be positive.", Double.valueOf(metrics.getAnnotation(Metrics.PERCENT_DURATION_KEY)) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        metrics = traversalMetrics.getMetrics(2);
        assertEquals(2, metrics.getCount(Metrics.ELEMENT_COUNT_ID));
        assertNotEquals(0, metrics.getCount(Metrics.TRAVERSER_COUNT_ID));
        assertTrue("Percent duration should be positive.", Double.valueOf(metrics.getAnnotation(Metrics.PERCENT_DURATION_KEY)) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        double totalPercentDuration = 0;
        for (Metrics m : traversalMetrics.getMetrics()) {
            totalPercentDuration += Double.valueOf(m.getAnnotation(Metrics.PERCENT_DURATION_KEY));
        }
        assertEquals(100, totalPercentDuration, 0.000001);
    }


    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_out_out_grateful_profile() {
        final Traversal<Vertex, StandardTraversalMetrics> traversal = get_g_V_out_out_profile();
        printTraversalForm(traversal);

        traversal.iterate();
        final TraversalMetrics traversalMetrics = traversal.asAdmin().getSideEffects().get(TraversalMetrics.METRICS_KEY);
        traversalMetrics.toString(); // ensure no exceptions are thrown

        Metrics metrics = traversalMetrics.getMetrics(0);
        assertEquals(808, metrics.getCount(Metrics.TRAVERSER_COUNT_ID));
        assertEquals(808, metrics.getCount(Metrics.ELEMENT_COUNT_ID));
        assertTrue("Percent duration should be positive.", Double.valueOf(metrics.getAnnotation(Metrics.PERCENT_DURATION_KEY)) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        metrics = traversalMetrics.getMetrics(1);
        assertEquals(8049, metrics.getCount(Metrics.ELEMENT_COUNT_ID));
        assertNotEquals(0, metrics.getCount(Metrics.TRAVERSER_COUNT_ID));
        assertTrue("Percent duration should be positive.", Double.valueOf(metrics.getAnnotation(Metrics.PERCENT_DURATION_KEY)) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        metrics = traversalMetrics.getMetrics(2);
        assertEquals(327370, metrics.getCount(Metrics.ELEMENT_COUNT_ID));
        assertNotEquals(0, metrics.getCount(Metrics.TRAVERSER_COUNT_ID));
        assertTrue("Percent duration should be positive.", Double.valueOf(metrics.getAnnotation(Metrics.PERCENT_DURATION_KEY)) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        double totalPercentDuration = 0;
        for (Metrics m : traversalMetrics.getMetrics()) {
            totalPercentDuration += Double.valueOf(m.getAnnotation(Metrics.PERCENT_DURATION_KEY));
        }
        assertEquals(100, totalPercentDuration, 0.000001);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeat_both_modern_profile() {
        final Traversal<Vertex, StandardTraversalMetrics> traversal = get_g_V_repeat_both_profile();
        printTraversalForm(traversal);

        traversal.iterate();

        final TraversalMetrics traversalMetrics = traversal.asAdmin().getSideEffects().get(TraversalMetrics.METRICS_KEY);
        traversalMetrics.toString(); // ensure no exceptions are thrown

        Metrics metrics = traversalMetrics.getMetrics(0);
        assertEquals(6, metrics.getCount(Metrics.TRAVERSER_COUNT_ID));
        assertEquals(6, metrics.getCount(Metrics.ELEMENT_COUNT_ID));
        assertTrue("Percent duration should be positive.", Double.valueOf(metrics.getAnnotation(Metrics.PERCENT_DURATION_KEY)) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        metrics = traversalMetrics.getMetrics(1);
        assertEquals(72, metrics.getCount(Metrics.ELEMENT_COUNT_ID));
        assertNotEquals(0, metrics.getCount(Metrics.TRAVERSER_COUNT_ID));
        assertTrue("Count should be greater than traversers.",
                metrics.getCount(Metrics.ELEMENT_COUNT_ID) > metrics.getCount(Metrics.TRAVERSER_COUNT_ID));
        assertTrue("Percent duration should be positive.", Double.valueOf(metrics.getAnnotation(Metrics.PERCENT_DURATION_KEY)) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        double totalPercentDuration = 0;
        for (Metrics m : traversalMetrics.getMetrics()) {
            totalPercentDuration += Double.valueOf(m.getAnnotation(Metrics.PERCENT_DURATION_KEY));
        }
        assertEquals(100, totalPercentDuration, 0.000001);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void test_nested_profile() {
        final Traversal<Vertex, StandardTraversalMetrics> traversal = get_nested_profile();
        printTraversalForm(traversal);

        traversal.iterate();

        // The traversal:
        // g.V().has(__.in("created").count().is(1l)).values("name").profile();

        final TraversalMetrics traversalMetrics = traversal.asAdmin().getSideEffects().get(TraversalMetrics.METRICS_KEY);
        traversalMetrics.toString(); // ensure no exceptions are thrown

        assertEquals("There should be 3 top-level metrics.", 3, traversalMetrics.getMetrics().size());

        Metrics metrics = traversalMetrics.getMetrics(0);
        assertEquals(6, metrics.getCount(Metrics.TRAVERSER_COUNT_ID));
        assertEquals(6, metrics.getCount(Metrics.ELEMENT_COUNT_ID));

        metrics = traversalMetrics.getMetrics(1);
        assertEquals(1, metrics.getCount(Metrics.TRAVERSER_COUNT_ID));
        assertEquals(1, metrics.getCount(Metrics.ELEMENT_COUNT_ID));

        assertEquals("Metrics 1 should have 3 nested metrics.", 3, metrics.getNested().size());

    }


    public static class StandardTest extends ProfileTest {

        @Test
        @LoadGraphWith(MODERN)
        public void testProfileTimes() {
            final Traversal<Vertex, StandardTraversalMetrics> traversal = get_g_V_sleep_sleep_profile();
            printTraversalForm(traversal);

            traversal.iterate();

            assertEquals("There should be 6 steps in this traversal (counting injected profile steps).", 6, traversal.asAdmin().getSteps().size());

            TraversalMetrics traversalMetrics = traversal.asAdmin().getSideEffects().get(TraversalMetrics.METRICS_KEY);
            traversalMetrics.toString(); // ensure no exceptions are thrown

            // Grab the second (sideEffect{sleep}) step and check the times.
            Metrics metrics = traversalMetrics.getMetrics(1);
            // 6 elements w/ a 10ms sleep each = 60ms with 10ms for other computation
            assertTrue("Duration should be at least the length of the sleep: " + metrics.getDuration(TimeUnit.MILLISECONDS),
                    metrics.getDuration(TimeUnit.MILLISECONDS) >= 60);
            assertTrue("Check that duration is within tolerant range: " + metrics.getDuration(TimeUnit.MILLISECONDS),
                    metrics.getDuration(TimeUnit.MILLISECONDS) < 80);

            // 6 elements w/ a 5ms sleep each = 30ms plus 20ms for other computation
            metrics = traversalMetrics.getMetrics(2);
            assertTrue("Duration should be at least the length of the sleep: " + metrics.getDuration(TimeUnit.MILLISECONDS),
                    metrics.getDuration(TimeUnit.MILLISECONDS) >= 30);
            assertTrue("Check that duration is within tolerant range: " + metrics.getDuration(TimeUnit.MILLISECONDS),
                    metrics.getDuration(TimeUnit.MILLISECONDS) < 50);

            double totalPercentDuration = 0;
            for (Metrics m : traversalMetrics.getMetrics()) {
                totalPercentDuration += Double.valueOf(m.getAnnotation(Metrics.PERCENT_DURATION_KEY));
            }
            assertEquals(100, totalPercentDuration, 0.000001);
        }

        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_out_out_profile() {
            return (Traversal) g.V().out().out().profile();
        }

        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_repeat_both_profile() {
            return (Traversal) g.V().repeat(both()).times(3).profile();
        }

        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_sleep_sleep_profile() {
            return (Traversal) g.V().sideEffect(new Consumer<Traverser<Vertex>>() {
                @Override
                public void accept(final Traverser<Vertex> vertexTraverser) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                    }
                }
            }).sideEffect(new Consumer<Traverser<Vertex>>() {
                @Override
                public void accept(final Traverser<Vertex> vertexTraverser) {
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException e) {
                    }
                }
            }).profile();
        }

        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_nested_profile() {
            return (Traversal) g.V().has(__.in("created").count().is(1l)).values("name").profile();
        }

    }

    public static class ComputerTest extends ProfileTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_out_out_profile() {
            return (Traversal) g.V().out().out().profile();
        }

        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_repeat_both_profile() {
            return (Traversal) g.V().repeat(both()).times(3).profile();
        }

        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_nested_profile() {
            return (Traversal) g.V().has(__.in("created").count().is(1l)).values("name").profile();
        }
    }
}
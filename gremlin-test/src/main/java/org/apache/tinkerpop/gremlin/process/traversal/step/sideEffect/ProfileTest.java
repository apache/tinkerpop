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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.*;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.StandardTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.junit.Assert.*;

/**
 * @author Bob Briody (http://bobbriody.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class ProfileTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, StandardTraversalMetrics> get_g_V_out_out_profile();

    public abstract Traversal<Vertex, StandardTraversalMetrics> get_g_V_repeat_both_profile();

    public abstract Traversal<Vertex, StandardTraversalMetrics> get_g_V_sleep_sleep_profile();

    public abstract Traversal<Vertex, StandardTraversalMetrics> get_g_V_hasXinXcreatedX_count_isX1XX_valuesXnameX_profile();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_modern_profile() {
        final Traversal<Vertex, StandardTraversalMetrics> traversal = get_g_V_out_out_profile();
        printTraversalForm(traversal);

        traversal.iterate();

        final TraversalMetrics traversalMetrics = traversal.asAdmin().getSideEffects().get(TraversalMetrics.METRICS_KEY);
        traversalMetrics.toString(); // ensure no exceptions are thrown

        Metrics metrics = traversalMetrics.getMetrics(0);
        assertEquals(6, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID));
        assertEquals(6, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID));
        assertTrue("Percent duration should be positive.", (Double) metrics.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        metrics = traversalMetrics.getMetrics(1);
        assertEquals(6, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID));
        assertNotEquals(0, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID));
        assertTrue("Percent duration should be positive.", (Double) metrics.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        metrics = traversalMetrics.getMetrics(2);
        assertEquals(2, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID));
        assertNotEquals(0, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID));
        assertTrue("Percent duration should be positive.", (Double) metrics.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        double totalPercentDuration = 0;
        for (Metrics m : traversalMetrics.getMetrics()) {
            totalPercentDuration += (Double) m.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY);
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
        assertEquals(808, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID));
        assertEquals(808, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID));
        assertTrue("Percent duration should be positive.", (Double) metrics.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        metrics = traversalMetrics.getMetrics(1);
        assertEquals(8049, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID));
        assertNotEquals(0, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID));
        assertTrue("Percent duration should be positive.", (Double) metrics.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        metrics = traversalMetrics.getMetrics(2);
        assertEquals(327370, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID));
        assertNotEquals(0, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID));
        assertTrue("Percent duration should be positive.", (Double) metrics.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        double totalPercentDuration = 0;
        for (Metrics m : traversalMetrics.getMetrics()) {
            totalPercentDuration += (Double) m.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY);
        }
        assertEquals(100, totalPercentDuration, 0.000001);
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_sleep_sleep_profile() {
        final Traversal<Vertex, StandardTraversalMetrics> traversal = get_g_V_sleep_sleep_profile();
        printTraversalForm(traversal);

        traversal.iterate();

        assertEquals("There should be 6 steps in this traversal (counting injected profile steps).", 6, traversal.asAdmin().getSteps().size());

        TraversalMetrics traversalMetrics = traversal.asAdmin().getSideEffects().get(TraversalMetrics.METRICS_KEY);
        traversalMetrics.toString(); // ensure no exceptions are thrown

        // Grab the second (sideEffect{sleep}) step and check the times.
        Metrics metrics = traversalMetrics.getMetrics(1);
        // 6 elements w/ a 10ms sleep each = 60ms with 10ms for other computation.
        assertTrue("Duration should be at least the length of the sleep (59ms): " + metrics.getDuration(TimeUnit.MILLISECONDS),
                metrics.getDuration(TimeUnit.MILLISECONDS) >= 59);
        assertTrue("Check that duration is within tolerant range: " + metrics.getDuration(TimeUnit.MILLISECONDS),
                metrics.getDuration(TimeUnit.MILLISECONDS) < 100);

        // 6 elements w/ a 5ms sleep each = 30ms plus 20ms for other computation
        metrics = traversalMetrics.getMetrics(2);
        assertTrue("Duration should be at least the length of the sleep (29ms): " + metrics.getDuration(TimeUnit.MILLISECONDS),
                metrics.getDuration(TimeUnit.MILLISECONDS) >= 29);
        assertTrue("Check that duration is within tolerant range: " + metrics.getDuration(TimeUnit.MILLISECONDS),
                metrics.getDuration(TimeUnit.MILLISECONDS) < 100);

        double totalPercentDuration = 0;
        for (Metrics m : traversalMetrics.getMetrics()) {
            totalPercentDuration += (Double) m.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY);
        }
        assertEquals(100, totalPercentDuration, 0.000001);
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_repeat_both_modern_profile() {
        final Traversal<Vertex, StandardTraversalMetrics> traversal = get_g_V_repeat_both_profile();
        printTraversalForm(traversal);

        traversal.iterate();

        final TraversalMetrics traversalMetrics = traversal.asAdmin().getSideEffects().get(TraversalMetrics.METRICS_KEY);
        traversalMetrics.toString(); // ensure no exceptions are thrown

        Metrics metrics = traversalMetrics.getMetrics(0);
        assertEquals(6, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID));
        assertEquals(6, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID));
        assertTrue("Percent duration should be positive.", (Double) metrics.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        metrics = traversalMetrics.getMetrics(1);
        assertEquals(72, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID));
        assertNotEquals(0, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID));
        assertTrue("Count should be greater than traversers.", metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID) > metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID));
        assertTrue("Percent duration should be positive.", (Double) metrics.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        // Test the nested global metrics of the repeat step
        Metrics vertexStepNestedInRepeat = (Metrics) metrics.getNested().toArray()[0];
        assertEquals(114, vertexStepNestedInRepeat.getCount(TraversalMetrics.ELEMENT_COUNT_ID));
        assertNotEquals(0, vertexStepNestedInRepeat.getCount(TraversalMetrics.TRAVERSER_COUNT_ID));
        assertTrue("Count should be greater than traversers.", vertexStepNestedInRepeat.getCount(TraversalMetrics.ELEMENT_COUNT_ID) > vertexStepNestedInRepeat.getCount(TraversalMetrics.TRAVERSER_COUNT_ID));
        assertTrue("Times should be positive.", vertexStepNestedInRepeat.getDuration(TimeUnit.MICROSECONDS) > 0);

        Metrics repeatEndStepNestedInRepeat = (Metrics) metrics.getNested().toArray()[1];
        assertEquals(72, repeatEndStepNestedInRepeat.getCount(TraversalMetrics.ELEMENT_COUNT_ID));
        assertNotEquals(0, repeatEndStepNestedInRepeat.getCount(TraversalMetrics.TRAVERSER_COUNT_ID));
        assertTrue("Count should be greater than traversers.", repeatEndStepNestedInRepeat.getCount(TraversalMetrics.ELEMENT_COUNT_ID) > repeatEndStepNestedInRepeat.getCount(TraversalMetrics.TRAVERSER_COUNT_ID));
        assertTrue("Times should be positive.", repeatEndStepNestedInRepeat.getDuration(TimeUnit.MICROSECONDS) > 0);


        double totalPercentDuration = 0;
        for (Metrics m : traversalMetrics.getMetrics()) {
            totalPercentDuration += (Double) m.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY);
        }
        assertEquals(100, totalPercentDuration, 0.000001);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXinXcreatedX_count_isX1XX_valuesXnameX_profile() {
        final Traversal<Vertex, StandardTraversalMetrics> traversal = get_g_V_hasXinXcreatedX_count_isX1XX_valuesXnameX_profile();
        printTraversalForm(traversal);

        traversal.iterate();

        final TraversalMetrics traversalMetrics = traversal.asAdmin().getSideEffects().get(TraversalMetrics.METRICS_KEY);
        traversalMetrics.toString(); // ensure no exceptions are thrown

        assertEquals("There should be 3 top-level metrics.", 3, traversalMetrics.getMetrics().size());

        Metrics metrics = traversalMetrics.getMetrics(0);
        assertEquals(6, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID));
        assertEquals(6, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID));

        metrics = traversalMetrics.getMetrics(1);
        assertEquals(1, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID));
        assertEquals(1, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID));

        assertEquals("Metrics 1 should have 3 nested metrics.", 3, metrics.getNested().size());
    }

    @UseEngine(TraversalEngine.Type.STANDARD)
    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class Traversals extends ProfileTest {

        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_out_out_profile() {
            return (Traversal) g.V().out().out().profile();
        }

        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_repeat_both_profile() {
            return (Traversal) g.V().repeat(both()).times(3).profile();
        }

        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_sleep_sleep_profile() {
            return (Traversal) g.V().sideEffect(new Consumer<Traverser<Vertex>>() {
                @Override
                public void accept(final Traverser<Vertex> vertexTraverser) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).sideEffect(new Consumer<Traverser<Vertex>>() {
                @Override
                public void accept(final Traverser<Vertex> vertexTraverser) {
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).profile();
        }

        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_hasXinXcreatedX_count_isX1XX_valuesXnameX_profile() {
            return (Traversal) g.V().has(__.in("created").count().is(1l)).values("name").profile();
        }
    }
}
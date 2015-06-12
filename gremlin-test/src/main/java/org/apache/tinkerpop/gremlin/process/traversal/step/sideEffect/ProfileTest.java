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
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.StandardTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.junit.Assert.*;

/**
 * @author Bob Briody (http://bobbriody.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class ProfileTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, StandardTraversalMetrics> get_g_V_out_out_profile();

    public abstract Traversal<Vertex, StandardTraversalMetrics> get_g_V_repeat_both_profile();

    public abstract Traversal<Vertex, StandardTraversalMetrics> get_g_V_sideEffectXThread_sleepX10XX_sideEffectXThread_sleepX5XX_profile();

    public abstract Traversal<Vertex, StandardTraversalMetrics> get_g_V_whereXinXcreatedX_count_isX1XX_valuesXnameX_profile();

    /**
     * Many of the tests in this class are coupled to not-totally-generic vendor behavior. However, this test is intended to provide
     * fully generic validation.
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_profile_simple() {
        final Traversal<Vertex, StandardTraversalMetrics> traversal = get_g_V_out_out_profile();
        printTraversalForm(traversal);

        traversal.iterate();

        final TraversalMetrics traversalMetrics = traversal.asAdmin().getSideEffects().<TraversalMetrics>get(TraversalMetrics.METRICS_KEY).get();
        traversalMetrics.toString(); // ensure no exceptions are thrown

        // Every other step should be a Profile step
        List<Step> steps = traversal.asAdmin().getSteps();
        for (int ii = 1; ii < steps.size(); ii += 2) {
            assertEquals("Every other Step should be a ProfileStep.", ProfileStep.class, steps.get(ii).getClass());
        }

        // Validate the last Metrics only, which must be consistent across vendors.
        Metrics metrics = traversalMetrics.getMetrics(traversalMetrics.getMetrics().size() - 1);
        assertEquals(2, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());
        assertNotEquals(0, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
        assertTrue("Percent duration should be positive.", (Double) metrics.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        // Ensure durations sum to 100
        double totalPercentDuration = 0;
        for (Metrics m : traversalMetrics.getMetrics()) {
            totalPercentDuration += (Double) m.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY);
        }
        assertEquals(100, totalPercentDuration, 0.000001);
    }


    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_profile_modern() {
        final Traversal<Vertex, StandardTraversalMetrics> traversal = get_g_V_out_out_profile();
        printTraversalForm(traversal);

        traversal.iterate();

        final TraversalMetrics traversalMetrics = traversal.asAdmin().getSideEffects().<TraversalMetrics>get(TraversalMetrics.METRICS_KEY).get();
        traversalMetrics.toString(); // ensure no exceptions are thrown

        Metrics metrics = traversalMetrics.getMetrics(0);
        assertEquals(6, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
        assertEquals(6, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());
        assertTrue("Percent duration should be positive.", (Double) metrics.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        metrics = traversalMetrics.getMetrics(1);
        assertEquals(6, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());
        assertNotEquals(0, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
        assertTrue("Percent duration should be positive.", (Double) metrics.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        metrics = traversalMetrics.getMetrics(2);
        assertEquals(2, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());
        assertNotEquals(0, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
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
    public void g_V_out_out_profile_grateful() {
        final Traversal<Vertex, StandardTraversalMetrics> traversal = get_g_V_out_out_profile();
        printTraversalForm(traversal);

        traversal.iterate();
        final TraversalMetrics traversalMetrics = traversal.asAdmin().getSideEffects().<TraversalMetrics>get(TraversalMetrics.METRICS_KEY).get();
        traversalMetrics.toString(); // ensure no exceptions are thrown

        Metrics metrics = traversalMetrics.getMetrics(0);
        assertEquals(808, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
        assertEquals(808, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());
        assertTrue("Percent duration should be positive.", (Double) metrics.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        metrics = traversalMetrics.getMetrics(1);
        assertEquals(8049, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());
        assertNotEquals(0, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
        assertTrue("Percent duration should be positive.", (Double) metrics.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        metrics = traversalMetrics.getMetrics(2);
        assertEquals(327370, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());
        assertNotEquals(0, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
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
    public void g_V_sideEffectXThread_sleepX10XX_sideEffectXThread_sleepX5XX_profile() {
        final Traversal<Vertex, StandardTraversalMetrics> traversal = get_g_V_sideEffectXThread_sleepX10XX_sideEffectXThread_sleepX5XX_profile();
        printTraversalForm(traversal);

        traversal.iterate();

        assertEquals("There should be 6 steps in this traversal (counting injected profile steps).", 6, traversal.asAdmin().getSteps().size());

        TraversalMetrics traversalMetrics = traversal.asAdmin().getSideEffects().<TraversalMetrics>get(TraversalMetrics.METRICS_KEY).get();
        traversalMetrics.toString(); // ensure no exceptions are thrown

        // Grab the second (sideEffect{sleep}) step and check the times.
        Metrics metrics = traversalMetrics.getMetrics(1);
        // 6 elements w/ a 10ms sleep each = 60ms with 10ms for other computation.
        assertTrue("Duration should be at least the length of the sleep (59ms): " + metrics.getDuration(TimeUnit.MILLISECONDS),
                metrics.getDuration(TimeUnit.MILLISECONDS) >= 59);
        //assertTrue("Check that duration is within tolerant range: " + metrics.getDuration(TimeUnit.MILLISECONDS),
        //        metrics.getDuration(TimeUnit.MILLISECONDS) < 130);

        // 6 elements w/ a 5ms sleep each = 30ms plus 20ms for other computation
        metrics = traversalMetrics.getMetrics(2);
        assertTrue("Duration should be at least the length of the sleep (29ms): " + metrics.getDuration(TimeUnit.MILLISECONDS),
                metrics.getDuration(TimeUnit.MILLISECONDS) >= 29);
        //assertTrue("Check that duration is within tolerant range: " + metrics.getDuration(TimeUnit.MILLISECONDS),
        //        metrics.getDuration(TimeUnit.MILLISECONDS) < 70);

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

        final TraversalMetrics traversalMetrics = traversal.asAdmin().getSideEffects().<TraversalMetrics>get(TraversalMetrics.METRICS_KEY).get();
        traversalMetrics.toString(); // ensure no exceptions are thrown

        Metrics metrics = traversalMetrics.getMetrics(0);
        assertEquals(6, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
        assertEquals(6, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());
        assertTrue("Percent duration should be positive.", (Double) metrics.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        metrics = traversalMetrics.getMetrics(1);
        assertEquals(72, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());
        assertNotEquals(0, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
        assertTrue("Count should be greater than traversers.", metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID) > metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
        assertTrue("Percent duration should be positive.", (Double) metrics.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY) > 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) > 0);

        // Test the nested global metrics of the repeat step
        final Metrics vertexStepNestedInRepeat = (Metrics) metrics.getNested().toArray()[0];
        assertEquals(114, vertexStepNestedInRepeat.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());
        assertNotEquals(0, vertexStepNestedInRepeat.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
        assertTrue("Count should be greater than traversers.", vertexStepNestedInRepeat.getCount(TraversalMetrics.ELEMENT_COUNT_ID) > vertexStepNestedInRepeat.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
        assertTrue("Times should be positive.", vertexStepNestedInRepeat.getDuration(TimeUnit.MICROSECONDS) > 0);

        final Metrics repeatEndStepNestedInRepeat = (Metrics) metrics.getNested().toArray()[1];
        assertEquals(72, repeatEndStepNestedInRepeat.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());
        assertNotEquals(0, repeatEndStepNestedInRepeat.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
        assertTrue("Count should be greater than traversers.", repeatEndStepNestedInRepeat.getCount(TraversalMetrics.ELEMENT_COUNT_ID) > repeatEndStepNestedInRepeat.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
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
        final Traversal<Vertex, StandardTraversalMetrics> traversal = get_g_V_whereXinXcreatedX_count_isX1XX_valuesXnameX_profile();
        printTraversalForm(traversal);

        traversal.iterate();

        final TraversalMetrics traversalMetrics = traversal.asAdmin().getSideEffects().<TraversalMetrics>get(TraversalMetrics.METRICS_KEY).get();
        traversalMetrics.toString(); // ensure no exceptions are thrown

        assertEquals("There should be 3 top-level metrics.", 3, traversalMetrics.getMetrics().size());

        Metrics metrics = traversalMetrics.getMetrics(0);
        assertEquals(6, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
        assertEquals(6, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());

        metrics = traversalMetrics.getMetrics(1);
        assertEquals(1, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
        assertEquals(1, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());

        assertEquals("Metrics 1 should have 3 nested metrics.", 3, metrics.getNested().size());
    }

    /**
     * ProfileStrategy callback test. Goal: ensure that a step that implements Profileable gets a callback to setMetrics
     */

    // Setup a "mock" step to test the strategy
    static public class MockStep extends FlatMapStep<Vertex, Vertex> implements Profiling {
        public static boolean callbackCalled = false;

        public MockStep(final Traversal.Admin traversal) {
            super(traversal);
        }

        @Override
        protected Iterator<Vertex> flatMap(final Traverser.Admin<Vertex> traverser) {
            List<Vertex> l = new ArrayList<>();
            l.add(traverser.get());
            return l.iterator();
        }

        @Override
        public void setMetrics(final MutableMetrics parentMetrics) {
            if (parentMetrics != null) {
                callbackCalled = true;
            }
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void testProfileStrategyCallback() {
        final Traversal<Vertex, StandardTraversalMetrics> t = get_g_V_out_out_profile();
        MockStep mockStep = new MockStep(t.asAdmin());
        t.asAdmin().addStep(3, mockStep);
        t.iterate();
        assertTrue(mockStep.callbackCalled);
    }

    /**
     * Traversals
     */
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
        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_sideEffectXThread_sleepX10XX_sideEffectXThread_sleepX5XX_profile() {
            return (Traversal) g.V().sideEffect(v -> {
                try {
                    Thread.sleep(10);
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                }
            }).sideEffect(v -> {
                try {
                    Thread.sleep(5);
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                }
            }).profile();
        }

        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_whereXinXcreatedX_count_isX1XX_valuesXnameX_profile() {
            return (Traversal) g.V().where(__.in("created").count().is(1l)).values("name").profile();
        }
    }
}

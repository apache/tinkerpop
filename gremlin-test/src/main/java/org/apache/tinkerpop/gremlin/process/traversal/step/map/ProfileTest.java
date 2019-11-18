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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRank;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.CountStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.RepeatUnrollStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

/**
 * @author Bob Briody (http://bobbriody.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class ProfileTest extends AbstractGremlinProcessTest {
    private static final String METRICS_KEY = "metrics";

    public abstract Traversal<Vertex, TraversalMetrics> get_g_V_out_out_profile();

    public abstract Traversal<Vertex, TraversalMetrics> get_g_V_repeatXbothX_timesX3X_profile();

    public abstract Traversal<Vertex, TraversalMetrics> get_g_V_sideEffectXThread_sleepX10XX_sideEffectXThread_sleepX5XX_profile();

    public abstract Traversal<Vertex, TraversalMetrics> get_g_V_whereXinXcreatedX_count_isX1XX_name_profile();

    public abstract Traversal<Vertex, TraversalMetrics> get_g_V_matchXa_created_b__b_in_count_isXeqX1XXX_selectXa_bX_profile();

    public abstract Traversal<Vertex, Vertex> get_g_V_out_out_profileXmetricsX();

    public abstract Traversal<Vertex, Vertex> get_g_V_repeatXbothX_timesX3X_profileXmetricsX();

    public abstract Traversal<Vertex, Vertex> get_g_V_sideEffectXThread_sleepX10XX_sideEffectXThread_sleepX5XX_profileXmetricsX();

    public abstract Traversal<Vertex, String> get_g_V_whereXinXcreatedX_count_isX1XX_name_profileXmetricsX();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__b_in_count_isXeqX1XXX_selectXa_bX_profileXmetricsX();

    public abstract Traversal<Vertex, TraversalMetrics> get_g_V_hasLabelXpersonX_pageRank_withXpropertyName_rankX_withXedges_bothEX_rank_profile();

    public abstract Traversal<Vertex, TraversalMetrics> get_g_V_groupXmX_profile();

    @Test
    @LoadGraphWith(MODERN)
    public void modern_V_out_out_profile() {
        final Traversal<Vertex, TraversalMetrics> traversal = get_g_V_out_out_profile();
        printTraversalForm(traversal);
        validate_g_V_out_out_profile_modern(traversal, traversal.next());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void modern_V_out_out_profileXmetricsX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_out_out_profileXmetricsX();
        printTraversalForm(traversal);
        traversal.iterate();
        validate_g_V_out_out_profile_modern(traversal, traversal.asAdmin().getSideEffects().get(METRICS_KEY));
    }

    private void validate_g_V_out_out_profile_modern(final Traversal traversal, final TraversalMetrics traversalMetrics) {
        traversalMetrics.toString(); // ensure no exceptions are thrown

        assumeThat("The following assertions apply to TinkerGraph only as provider strategies can alter the steps to not comply with expectations",
                graph.getClass().getSimpleName(), equalTo("TinkerGraph"));

        Metrics metrics = traversalMetrics.getMetrics(0);
        assertEquals(6, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
        assertEquals(6, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());

        metrics = traversalMetrics.getMetrics(1);
        assertEquals(6, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());
        assertNotEquals(0, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());

        metrics = traversalMetrics.getMetrics(2);
        assertEquals(2, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());
        assertNotEquals(0, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());

        if (!onGraphComputer(traversal.asAdmin())) {
            // Every other step should be a Profile step
            List<Step> steps = traversal.asAdmin().getSteps();
            for (int ii = 1; ii <= 6; ii += 2) {
                assertEquals("Every other Step should be a ProfileStep.", ProfileStep.class, steps.get(ii).getClass());
            }
        }
    }

    ////////////////////

    @Test
    @LoadGraphWith(GRATEFUL)
    public void grateful_V_out_out_profile() {
        final Traversal<Vertex, TraversalMetrics> traversal = get_g_V_out_out_profile();
        printTraversalForm(traversal);
        final TraversalMetrics traversalMetrics = traversal.next();
        validate_g_V_out_out_profile_grateful(traversalMetrics);
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void grateful_V_out_out_profileXmetricsX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_out_out_profileXmetricsX();
        printTraversalForm(traversal);
        traversal.iterate();
        final TraversalMetrics traversalMetrics = traversal.asAdmin().getSideEffects().get(METRICS_KEY);
        validate_g_V_out_out_profile_grateful(traversalMetrics);
    }

    private void validate_g_V_out_out_profile_grateful(final TraversalMetrics traversalMetrics) {
        traversalMetrics.toString(); // ensure no exceptions are thrown

        assumeThat("The following assertions apply to TinkerGraph only as provider strategies can alter the steps to not comply with expectations",
                graph.getClass().getSimpleName(), equalTo("TinkerGraph"));

        Metrics metrics = traversalMetrics.getMetrics(0);
        assertEquals(808, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
        assertEquals(808, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());
        assertTrue("Percent duration should be positive.", (Double) metrics.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY) >= 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) >= 0);

        metrics = traversalMetrics.getMetrics(1);
        assertEquals(8049, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());
        assertNotEquals(0, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
        assertTrue("Percent duration should be positive.", (Double) metrics.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY) >= 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) >= 0);

        metrics = traversalMetrics.getMetrics(2);
        assertEquals(327370, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());
        assertNotEquals(0, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
        assertTrue("Percent duration should be positive.", (Double) metrics.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY) >= 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) >= 0);

        double totalPercentDuration = 0;
        for (Metrics m : traversalMetrics.getMetrics()) {
            totalPercentDuration += (Double) m.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY);
        }
        assertEquals(100, totalPercentDuration, 0.000001);
    }

    ///////////////////

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_sideEffectXThread_sleepX10XX_sideEffectXThread_sleepX5XX_profile() {
        final Traversal<Vertex, TraversalMetrics> traversal = get_g_V_sideEffectXThread_sleepX10XX_sideEffectXThread_sleepX5XX_profile();
        printTraversalForm(traversal);

        // This assertion is really only meant for tinkergraph
        if (graph.getClass().getSimpleName().equals("TinkerGraph"))
            assertEquals("There should be 8 steps in this traversal (counting injected profile steps).", 8, traversal.asAdmin().getSteps().size());

        final TraversalMetrics traversalMetrics = traversal.next();
        validate_g_V_sideEffectXThread_sleepX10XX_sideEffectXThread_sleepX5XX_profile(traversalMetrics);
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_sideEffectXThread_sleepX10XX_sideEffectXThread_sleepX5XX_profileXmetricsX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_sideEffectXThread_sleepX10XX_sideEffectXThread_sleepX5XX_profileXmetricsX();
        printTraversalForm(traversal);
        traversal.iterate();

        // This assertion is really only meant for tinkergraph
        if (graph.getClass().getSimpleName().equals("TinkerGraph"))
            assertEquals("There should be 7 steps in this traversal (counting injected profile steps).", 7, traversal.asAdmin().getSteps().size());

        final TraversalMetrics traversalMetrics = traversal.asAdmin().getSideEffects().get(METRICS_KEY);
        validate_g_V_sideEffectXThread_sleepX10XX_sideEffectXThread_sleepX5XX_profile(traversalMetrics);
    }

    private void validate_g_V_sideEffectXThread_sleepX10XX_sideEffectXThread_sleepX5XX_profile(final TraversalMetrics traversalMetrics) {
        traversalMetrics.toString(); // ensure no exceptions are thrown

        assumeThat("The following assertions apply to TinkerGraph only as provider strategies can alter the steps to not comply with expectations",
                graph.getClass().getSimpleName(), equalTo("TinkerGraph"));

        // Grab the second (sideEffect{sleep}) step and check the times.
        Metrics metrics = traversalMetrics.getMetrics(1);
        // 6 elements w/ a 10ms sleep each = 60ms with 10ms for other computation.
        assertTrue("Duration should be at least the length of the sleep (59ms): " + metrics.getDuration(TimeUnit.MILLISECONDS),
                metrics.getDuration(TimeUnit.MILLISECONDS) >= 59);

        // 6 elements w/ a 5ms sleep each = 30ms plus 20ms for other computation
        metrics = traversalMetrics.getMetrics(2);
        assertTrue("Duration should be at least the length of the sleep (29ms): " + metrics.getDuration(TimeUnit.MILLISECONDS),
                metrics.getDuration(TimeUnit.MILLISECONDS) >= 29);

        double totalPercentDuration = 0;
        for (Metrics m : traversalMetrics.getMetrics()) {
            totalPercentDuration += (Double) m.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY);
        }
        assertEquals(100, totalPercentDuration, 0.000001);
    }

    ///////////////////

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeat_both_profile() {
        final Traversal<Vertex, TraversalMetrics> traversal = get_g_V_repeatXbothX_timesX3X_profile();
        printTraversalForm(traversal);

        final TraversalMetrics traversalMetrics = traversal.next();
        validate_g_V_repeat_both_modern_profile(traversalMetrics,
                traversal.asAdmin().getStrategies().getStrategy(RepeatUnrollStrategy.class).isPresent() &&
                        !traversal.asAdmin().getStrategies().getStrategy(ComputerVerificationStrategy.class).isPresent());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeat_both_profileXmetricsX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_repeatXbothX_timesX3X_profileXmetricsX();
        printTraversalForm(traversal);
        traversal.iterate();
        final TraversalMetrics traversalMetrics = traversal.asAdmin().getSideEffects().get(METRICS_KEY);
        validate_g_V_repeat_both_modern_profile(traversalMetrics,
                traversal.asAdmin().getStrategies().getStrategy(RepeatUnrollStrategy.class).isPresent() &&
                        !traversal.asAdmin().getStrategies().getStrategy(ComputerVerificationStrategy.class).isPresent());
    }

    private void validate_g_V_repeat_both_modern_profile(final TraversalMetrics traversalMetrics, final boolean withRepeatUnrollStrategy) {
        traversalMetrics.toString(); // ensure no exceptions are thrown

        assumeThat("The following assertions apply to TinkerGraph only as provider strategies can alter the steps to not comply with expectations",
                graph.getClass().getSimpleName(), equalTo("TinkerGraph"));

        Metrics metrics = traversalMetrics.getMetrics(0);
        assertEquals(6, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
        assertEquals(6, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());

        metrics = traversalMetrics.getMetrics(1);
        assertEquals(withRepeatUnrollStrategy ? 12 : 72, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());
        assertNotEquals(0, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
        if (!withRepeatUnrollStrategy)
            assertTrue("Count should be greater than traversers.", metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID) > metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
        assertTrue("Percent duration should be positive.", (Double) metrics.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY) >= 0);
        assertTrue("Times should be positive.", metrics.getDuration(TimeUnit.MICROSECONDS) >= 0);

        // Test the nested global metrics of the repeat step
        if (!withRepeatUnrollStrategy) {
            final Metrics vertexStepNestedInRepeat = (Metrics) metrics.getNested().toArray()[0];
            assertEquals(114, vertexStepNestedInRepeat.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());
            assertNotEquals(0, vertexStepNestedInRepeat.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
            assertTrue("Count should be greater than traversers.", vertexStepNestedInRepeat.getCount(TraversalMetrics.ELEMENT_COUNT_ID) > vertexStepNestedInRepeat.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
            assertTrue("Times should be positive.", vertexStepNestedInRepeat.getDuration(TimeUnit.MICROSECONDS) >= 0);
        }

        double totalPercentDuration = 0;
        for (Metrics m : traversalMetrics.getMetrics()) {
            totalPercentDuration += (Double) m.getAnnotation(TraversalMetrics.PERCENT_DURATION_KEY);
        }
        assertEquals(100, totalPercentDuration, 0.000001);
    }

    /////////////

    private void validate_g_V_whereXinXcreatedX_count_isX1XX_name_profile(final Traversal traversal, final TraversalMetrics traversalMetrics) {
        traversalMetrics.toString(); // ensure no exceptions are thrown

        assumeThat("The following assertions apply to TinkerGraph only as provider strategies can alter the steps to not comply with expectations",
                graph.getClass().getSimpleName(), equalTo("TinkerGraph"));

        assertEquals("There should be 3 top-level metrics.", 3, traversalMetrics.getMetrics().size());

        Metrics metrics = traversalMetrics.getMetrics(0);
        assertEquals(6, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
        assertEquals(6, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());

        metrics = traversalMetrics.getMetrics(1);
        assertEquals(1, metrics.getCount(TraversalMetrics.TRAVERSER_COUNT_ID).longValue());
        assertEquals(1, metrics.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());

        if (traversal.asAdmin().getStrategies().getStrategy(CountStrategy.class).isPresent()) {
            assertEquals("Metrics 1 should have 4 nested metrics.", 4, metrics.getNested().size());
        } else {
            assertEquals("Metrics 1 should have 3 nested metrics.", 3, metrics.getNested().size());
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_whereXinXcreatedX_count_isX1XX_name_profile() {
        final Traversal<Vertex, TraversalMetrics> traversal = get_g_V_whereXinXcreatedX_count_isX1XX_name_profile();
        printTraversalForm(traversal);
        final TraversalMetrics traversalMetrics = traversal.next();
        validate_g_V_whereXinXcreatedX_count_isX1XX_name_profile(traversal, traversalMetrics);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_whereXinXcreatedX_count_isX1XX_name_profileXmetricsX() {
        final Traversal<Vertex, String> traversal = get_g_V_whereXinXcreatedX_count_isX1XX_name_profileXmetricsX();
        printTraversalForm(traversal);
        traversal.iterate();
        final TraversalMetrics traversalMetrics = traversal.asAdmin().getSideEffects().get(METRICS_KEY);
        validate_g_V_whereXinXcreatedX_count_isX1XX_name_profile(traversal, traversalMetrics);
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
                parentMetrics.setCount("bogusCount", 100);
            }
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void testProfileStrategyCallback() {
        final Traversal<Vertex, TraversalMetrics> t = get_g_V_out_out_profile();
        MockStep mockStep = new MockStep(t.asAdmin());
        t.asAdmin().addStep(3, mockStep);
        TraversalMetrics traversalMetrics = t.next();
        assertTrue(mockStep.callbackCalled);

        if (!onGraphComputer(t.asAdmin())) {
            assertEquals(100, traversalMetrics.getMetrics(3).getCount("bogusCount").longValue());
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void testProfileStrategyCallbackSideEffect() {
        final Traversal<Vertex, Vertex> t = get_g_V_out_out_profileXmetricsX();
        MockStep mockStep = new MockStep(t.asAdmin());
        t.asAdmin().addStep(3, mockStep);
        t.iterate();
        assertTrue(mockStep.callbackCalled);

        if (!onGraphComputer(t.asAdmin())) {
            final TraversalMetrics traversalMetrics = t.asAdmin().getSideEffects().get(METRICS_KEY);
            assertEquals(100, traversalMetrics.getMetrics(3).getCount("bogusCount").longValue());
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_matchXa_created_b__b_in_count_isXeqX1XXX_selectXa_bX_profile() {
        final Traversal<Vertex, TraversalMetrics> traversal = get_g_V_matchXa_created_b__b_in_count_isXeqX1XXX_selectXa_bX_profile();
        printTraversalForm(traversal);
        traversal.iterate();
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_matchXa_created_b__b_in_count_isXeqX1XXX_selectXa_bX_profileXmetricsX() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_matchXa_created_b__b_in_count_isXeqX1XXX_selectXa_bX_profileXmetricsX();
        printTraversalForm(traversal);
        traversal.iterate();
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.STANDARD)
    public void g_V_hasLabelXpersonX_pageRank_withXpropertyName_rankX_withXedges_bothEX_rank_profile() {
        final Traversal<Vertex, TraversalMetrics> traversal = get_g_V_hasLabelXpersonX_pageRank_withXpropertyName_rankX_withXedges_bothEX_rank_profile();
        //printTraversalForm(traversal);
        try {
            traversal.iterate();
            fail("Should have tossed an exception because multi-OLAP is unsolvable");
        } catch (Exception ex) {
            assertTrue(ex instanceof VerificationException || ExceptionUtils.getRootCause(ex) instanceof VerificationException);
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_groupXmX_profile() {
        final Traversal<Vertex, TraversalMetrics> traversal = get_g_V_groupXmX_profile();
        printTraversalForm(traversal);
        traversal.next();
        assertFalse(traversal.hasNext());
    }

    private static boolean onGraphComputer(final Traversal.Admin<?, ?> traversal) {
        return !TraversalHelper.getStepsOfClass(TraversalVertexProgramStep.class, TraversalHelper.getRootTraversal(traversal)).isEmpty();
    }

    /**
     * Traversals
     */
    public static class Traversals extends ProfileTest {

        @Override
        public Traversal<Vertex, TraversalMetrics> get_g_V_out_out_profile() {
            return g.V().out().out().profile();
        }

        @Override
        public Traversal<Vertex, TraversalMetrics> get_g_V_repeatXbothX_timesX3X_profile() {
            return g.V().repeat(both()).times(3).profile();
        }

        @Override
        public Traversal<Vertex, TraversalMetrics> get_g_V_sideEffectXThread_sleepX10XX_sideEffectXThread_sleepX5XX_profile() {
            return g.V().sideEffect(v -> {
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
        public Traversal<Vertex, TraversalMetrics> get_g_V_whereXinXcreatedX_count_isX1XX_name_profile() {
            return g.V().where(__.in("created").count().is(1l)).<String>values("name").profile();
        }

        @Override
        public Traversal<Vertex, TraversalMetrics> get_g_V_matchXa_created_b__b_in_count_isXeqX1XXX_selectXa_bX_profile() {
            return g.V().match(__.as("a").out("created").as("b"), __.as("b").in().count().is(P.eq(1))).<String>select("a", "b").profile();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_out_out_profileXmetricsX() {
            return g.V().out().out().profile(METRICS_KEY);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXbothX_timesX3X_profileXmetricsX() {
            return g.V().repeat(both()).times(3).profile(METRICS_KEY);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_sideEffectXThread_sleepX10XX_sideEffectXThread_sleepX5XX_profileXmetricsX() {
            return g.V().sideEffect(v -> {
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
            }).profile(METRICS_KEY);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_whereXinXcreatedX_count_isX1XX_name_profileXmetricsX() {
            return g.V().where(__.in("created").count().is(1l)).<String>values("name").profile(METRICS_KEY);
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__b_in_count_isXeqX1XXX_selectXa_bX_profileXmetricsX() {
            return g.V().match(__.as("a").out("created").as("b"), __.as("b").in().count().is(P.eq(1))).<String>select("a", "b").profile(METRICS_KEY);
        }

        @Override
        public Traversal<Vertex, TraversalMetrics> get_g_V_hasLabelXpersonX_pageRank_withXpropertyName_rankX_withXedges_bothEX_rank_profile() {
            return g.V().hasLabel("person").pageRank().with(PageRank.propertyName, "rank").with(PageRank.edges, __.bothE()).values("rank").profile();
        }

        @Override
        public Traversal<Vertex, TraversalMetrics> get_g_V_groupXmX_profile() {
            return g.V().group("m").profile();
        }
    }
}

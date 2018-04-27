/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.function.BiFunction;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.Operator.sum;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.desc;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.math;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.sack;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class MathTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Double> get_g_V_asXaX_outXknowsX_asXbX_mathXa_plus_bX_byXageX();

    public abstract Traversal<Vertex, Double> get_g_withSideEffectXx_100X_V_age_mathX__plus_xX();

    public abstract Traversal<Vertex, Double> get_g_V_asXaX_outXcreatedX_asXbX_mathXb_plus_aX_byXinXcreatedX_countX_byXageX();

    public abstract Traversal<Integer, Double> get_g_withSackX1X_injectX1X_repeatXsackXsumX_byXconstantX1XXX_timesX5X_emit_mathXsin__X_byXsackX();

    public abstract Traversal<Vertex, String> get_g_V_projectXa_b_cX_byXbothE_weight_sumX_byXbothE_countX_byXnameX_order_byXmathXa_div_bX_descX_selectXcX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_outXknowsX_asXbX_mathXa_plus_bX_byXageX() {
        final Traversal<Vertex, Double> traversal = get_g_V_asXaX_outXknowsX_asXbX_mathXa_plus_bX_byXageX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(56.0d, 61.0d), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_withSideEffectXx_100X_V_age_mathX__plus_xX() {
        final Traversal<Vertex, Double> traversal = get_g_withSideEffectXx_100X_V_age_mathX__plus_xX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(129.0d, 127.0d, 132.0d, 135.0d), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_outXcreatedX_asXbX_mathXb_plus_aX_byXinXcreatedX_countX_byXageX() {
        final Traversal<Vertex, Double> traversal = get_g_V_asXaX_outXcreatedX_asXbX_mathXb_plus_aX_byXinXcreatedX_countX_byXageX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(32.0d, 33.0d, 35.0d, 38.0d), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_withSackX1X_injectX1X_repeatXsackXsumX_byXconstantX1XXX_timesX5X_emit_mathXsin__X_byXsackX() {
        final Traversal<Integer, Double> traversal = get_g_withSackX1X_injectX1X_repeatXsackXsumX_byXconstantX1XXX_timesX5X_emit_mathXsin__X_byXsackX();
        printTraversalForm(traversal);
        assertEquals(0.9092974268256817d, traversal.next(), 0.01d);
        assertEquals(0.1411200080598672d, traversal.next(), 0.01d);
        assertEquals(-0.7568024953079282d, traversal.next(), 0.01d);
        assertEquals(-0.9589242746631385d, traversal.next(), 0.01d);
        assertEquals(-0.27941549819892586d, traversal.next(), 0.01d);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_projectXa_b_cX_byXbothE_weight_sumX_byXbothE_countX_byXnameX_order_byXmathXa_div_bX_descX_selectXcX() {
        final Traversal<Vertex, String> traversal = get_g_V_projectXa_b_cX_byXbothE_weight_sumX_byXbothE_countX_byXnameX_order_byXmathXa_div_bX_descX_selectXcX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("ripple", "josh", "marko", "vadas", "lop", "peter"), traversal);
    }

    public static class Traversals extends MathTest {

        @Override
        public Traversal<Vertex, Double> get_g_V_asXaX_outXknowsX_asXbX_mathXa_plus_bX_byXageX() {
            return g.V().as("a").out("knows").as("b").math("a + b").by("age");
        }

        @Override
        public Traversal<Vertex, Double> get_g_withSideEffectXx_100X_V_age_mathX__plus_xX() {
            return g.withSideEffect("x", 100).V().values("age").math("_ + x");
        }

        @Override
        public Traversal<Vertex, Double> get_g_V_asXaX_outXcreatedX_asXbX_mathXb_plus_aX_byXinXcreatedX_countX_byXageX() {
            return g.V().as("a").out("created").as("b").math("b + a").by(in("created").count()).by("age");
        }

        @Override
        public Traversal<Integer, Double> get_g_withSackX1X_injectX1X_repeatXsackXsumX_byXconstantX1XXX_timesX5X_emit_mathXsin__X_byXsackX() {
            return g.withSack(1).inject(1).repeat(__.sack((BiFunction) sum).by(__.constant(1))).times(10).emit().math("sin _").by(sack());
        }

        @Override
        public Traversal<Vertex, String> get_g_V_projectXa_b_cX_byXbothE_weight_sumX_byXbothE_countX_byXnameX_order_byXmathXa_div_bX_descX_selectXcX() {
            return g.V().project("a", "b", "c").by(bothE().values("weight").sum()).by(bothE().count()).by("name").order().by(math("a / b"), desc).select("c");
        }
    }
}
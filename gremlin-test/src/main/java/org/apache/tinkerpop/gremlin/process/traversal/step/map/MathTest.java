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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class MathTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Double> get_g_V_asXaX_outXknowsX_asXbX_mathXa_plus_bX_byXageX();

    public abstract Traversal<Vertex, Double> get_g_withSideEffectXx_100X_V_age_mathX__plus_xX();

    public abstract Traversal<Vertex, Double> get_g_V_asXaX_outXcreatedX_asXbX_mathXb_plus_aX_byXinXcreatedX_countX_byXageX();

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
    }
}
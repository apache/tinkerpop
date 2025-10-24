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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashSet;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.aggregate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class SideEffectTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Object> get_g_V_valuesXnameX_sideEffectXidentityX();

    public abstract Traversal<Vertex, Object> get_g_V_valuesXnameX_sideEffectXidentity_substringX1XX();

    public abstract Traversal<Vertex, String> get_g_withSideEffectXx_setX_V_both_both_sideEffectX_localX_aggregateXxX_byXnameXX_capXxX_unfold();

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_valuesXnameX_sideEffectXidentityX() {
        final Traversal<Vertex, Object> traversal = get_g_V_valuesXnameX_sideEffectXidentityX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("josh", "peter", "ripple", "marko", "vadas", "lop"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_valuesXnameX_sideEffectXidentity_substringX1XX() {
        final Traversal<Vertex, Object> traversal = get_g_V_valuesXnameX_sideEffectXidentity_substringX1XX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("josh", "peter", "ripple", "marko", "vadas", "lop"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_withSideEffectXx_setX_V_both_both_sideEffectXstoreXxX_byXnameXX_capXxX_unfold() {
        final Traversal<Vertex, String> traversal =  get_g_withSideEffectXx_setX_V_both_both_sideEffectX_localX_aggregateXxX_byXnameXX_capXxX_unfold();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("josh", "peter", "ripple", "marko", "vadas", "lop"), traversal);
        checkSideEffects(traversal.asAdmin().getSideEffects(), "x", HashSet.class);
    }

    public static class Traversals extends SideEffectTest {

        @Override
        public Traversal<Vertex, Object> get_g_V_valuesXnameX_sideEffectXidentityX() {
            return g.V().values("name").sideEffect(__.identity());
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_valuesXnameX_sideEffectXidentity_substringX1XX() {
            return g.V().values("name").sideEffect(__.identity().substring(1));
        }

        @Override
        public Traversal<Vertex, String> get_g_withSideEffectXx_setX_V_both_both_sideEffectX_localX_aggregateXxX_byXnameXX_capXxX_unfold() {
            return g.withSideEffect("x",new HashSet<>()).V().both().both().sideEffect(__.local(aggregate("x").by("name"))).cap("x").unfold();
        }

    }
}

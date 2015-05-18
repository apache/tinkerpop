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

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BinaryOperator;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class FoldTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, List<Vertex>> get_g_V_fold();

    public abstract Traversal<Vertex, Vertex> get_g_V_fold_unfold();

    public abstract Traversal<Vertex, Integer> get_g_V_age_foldX0_plusX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_fold() {
        final Traversal<Vertex, List<Vertex>> traversal = get_g_V_fold();
        printTraversalForm(traversal);
        final List<Vertex> list = traversal.next();
        assertFalse(traversal.hasNext());
        final Set<Vertex> vertices = new HashSet<>(list);
        assertEquals(6, vertices.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_fold_unfold() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_fold_unfold();
        printTraversalForm(traversal);
        int count = 0;
        final Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            vertices.add(traversal.next());
            count++;
        }
        assertFalse(traversal.hasNext());
        assertEquals(6, count);
        assertEquals(6, vertices.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_age_foldX0_plusX() {
        final Traversal<Vertex, Integer> traversal = get_g_V_age_foldX0_plusX();
        printTraversalForm(traversal);
        final Integer ageSum = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(Integer.valueOf(123), ageSum);
    }

    public static class Traversals extends FoldTest {

        @Override
        public Traversal<Vertex, List<Vertex>> get_g_V_fold() {
            return g.V().fold();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_fold_unfold() {
            return g.V().fold().unfold();
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_foldX0_plusX() {
            return g.V().<Integer>values("age").fold(0, (BinaryOperator) Operator.sum);
        }
    }
}

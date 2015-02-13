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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.map;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.graph.traversal.__.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class MinTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Integer> get_g_V_age_min();

    public abstract Traversal<Vertex, Integer> get_g_V_repeatXbothX_timesX5X_age_min();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_age_min() {
        final List<Traversal<Vertex, Integer>> traversals = Arrays.asList(
                get_g_V_age_min(),
                get_g_V_repeatXbothX_timesX5X_age_min());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            checkResults(Arrays.asList(27), traversal);
        });
    }

    public static class StandardTest extends MinTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_min() {
            return g.V().values("age").min();
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_repeatXbothX_timesX5X_age_min() {
            return g.V().repeat(both()).times(5).values("age").min();
        }

    }

    public static class ComputerTest extends MinTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_min() {
            return g.V().values("age").<Integer>min();
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_repeatXbothX_timesX5X_age_min() {
            return g.V().repeat(both()).times(5).values("age").<Integer>min();
        }
    }
}
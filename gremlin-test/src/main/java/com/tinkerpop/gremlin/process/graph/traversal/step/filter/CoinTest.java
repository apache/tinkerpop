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
package com.tinkerpop.gremlin.process.graph.traversal.step.filter;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class CoinTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_V_coinX1X();

    public abstract Traversal<Vertex, Vertex> get_g_V_coinX0X();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_coinX1X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_coinX1X();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            traversal.next();
        }
        assertEquals(6, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_coinX0X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_coinX0X();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            traversal.next();
        }
        assertEquals(0, counter);
        assertFalse(traversal.hasNext());
    }

    public static class StandardTest extends CoinTest {

        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_coinX1X() {
            return g.V().coin(1.0d);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_coinX0X() {
            return g.V().coin(0.0d);
        }
    }

    public static class ComputerTest extends CoinTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_coinX1X() {
            return g.V().coin(1.0d).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_coinX0X() {
            return g.V().coin(0.0d).submit(g.compute());
        }
    }
}

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

package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class TerminalTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_V_out_iterate();

    public abstract Traversal<Vertex, Vertex> get_g_V_out();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_iterate() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_out_iterate();
        printTraversalForm(traversal);
        assertEquals("iterate", traversal.asAdmin().getBytecode().getStepInstructions().get(traversal.asAdmin().getBytecode().getStepInstructions().size() - 1).getOperator());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_toList() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_out();
        assertEquals(6, traversal.toList().size());
        printTraversalForm(traversal);
        assertEquals("toList", traversal.asAdmin().getBytecode().getStepInstructions().get(traversal.asAdmin().getBytecode().getStepInstructions().size() - 1).getOperator());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_toSet() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_out();
        assertEquals(4, traversal.toSet().size());
        printTraversalForm(traversal);
        assertEquals("toSet", traversal.asAdmin().getBytecode().getStepInstructions().get(traversal.asAdmin().getBytecode().getStepInstructions().size() - 1).getOperator());
    }


    public static class Traversals extends TerminalTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_out_iterate() {
            return g.V().out().iterate();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_out() {
            return g.V().out();
        }

    }
}
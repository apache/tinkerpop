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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.constant;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Matt Frantz (http://github.com/mhfrantz)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class ConstantTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Integer> get_g_V_constantX123X();

    public abstract Traversal<Vertex, Void> get_g_V_constantXnullX();

    public abstract Traversal<Vertex, String> get_g_V_chooseXhasLabelXpersonX_valuesXnameX_constantXinhumanXX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_constantX123X() {
        final Traversal<Vertex, Integer> traversal = get_g_V_constantX123X();
        printTraversalForm(traversal);
        assertEquals(Arrays.asList(123, 123, 123, 123, 123, 123), traversal.toList());
    }
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_constantXnullX() {
        final Traversal<Vertex, Void> traversal = get_g_V_constantXnullX();
        printTraversalForm(traversal);
        assertEquals(Arrays.asList(null, null, null, null, null, null), traversal.toList());
    }

    /** Scenario: Anonymous traversal within choose */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_chooseXhasLabelXpersonX_valuesXnameX_constantXinhumanXX() {
        final Traversal<Vertex, String> traversal = get_g_V_chooseXhasLabelXpersonX_valuesXnameX_constantXinhumanXX();
        printTraversalForm(traversal);
        assertThat(traversal.toList(), containsInAnyOrder("marko", "vadas", "inhuman", "josh", "inhuman", "peter"));
    }

    public static class Traversals extends ConstantTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_constantX123X() {
            return g.V().constant(123);
        }

        @Override
        public Traversal<Vertex, Void> get_g_V_constantXnullX() {
            return g.V().constant(null);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_chooseXhasLabelXpersonX_valuesXnameX_constantXinhumanXX() {
            return g.V().choose(hasLabel("person"), values("name"), constant("inhuman"));
        }
    }
}

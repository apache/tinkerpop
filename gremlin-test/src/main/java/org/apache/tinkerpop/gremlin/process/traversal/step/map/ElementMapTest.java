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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.CREW;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.structure.T.id;
import static org.apache.tinkerpop.gremlin.structure.T.label;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class ElementMapTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Map<Object, Object>> get_g_V_elementMap();

    public abstract Traversal<Vertex, Map<Object, Object>> get_g_V_elementMapXname_ageX();

    public abstract Traversal<Edge, Map<Object, Object>> get_g_EX11X_elementMap(final Object e11Id);

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_elementMap() {
        final Traversal<Vertex, Map<Object, Object>> traversal = get_g_V_elementMap();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<Object, Object> values = traversal.next();
            final String name = (String) values.get("name");
            assertEquals(4, values.size());
            if (name.equals("marko")) {
                assertEquals(29, values.get("age"));
                assertEquals("person", values.get(label));
            } else if (name.equals("josh")) {
                assertEquals(32, values.get("age"));
                assertEquals("person", values.get(label));
            } else if (name.equals("peter")) {
                assertEquals(35, values.get("age"));
                assertEquals("person", values.get(label));
            } else if (name.equals("vadas")) {
                assertEquals(27, values.get("age"));
                assertEquals("person", values.get(label));
            } else if (name.equals("lop")) {
                assertEquals("java", values.get("lang"));
                assertEquals("software", values.get(label));
            } else if (name.equals("ripple")) {
                assertEquals("java", values.get("lang"));
                assertEquals("software", values.get(label));
            } else {
                throw new IllegalStateException("It is not possible to reach here: " + values);
            }

            assertThat(values.containsKey(T.id), is(true));
        }
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_elementMapXname_ageX() {
        final Traversal<Vertex, Map<Object, Object>> traversal = get_g_V_elementMapXname_ageX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<Object, Object> values = traversal.next();
            final String name = (String) values.get("name");
            if (name.equals("marko")) {
                assertEquals(29, values.get("age"));
                assertEquals("person", values.get(label));
                assertEquals(4, values.size());
            } else if (name.equals("josh")) {
                assertEquals(32, values.get("age"));
                assertEquals("person", values.get(label));
                assertEquals(4, values.size());
            } else if (name.equals("peter")) {
                assertEquals(35, values.get("age"));
                assertEquals("person", values.get(label));
                assertEquals(4, values.size());
            } else if (name.equals("vadas")) {
                assertEquals(27, values.get("age"));
                assertEquals("person", values.get(label));
                assertEquals(4, values.size());
            } else if (name.equals("lop")) {
                assertNull(values.get("lang"));
                assertEquals("software", values.get(label));
                assertEquals(3, values.size());
            } else if (name.equals("ripple")) {
                assertNull(values.get("lang"));
                assertEquals("software", values.get(label));
                assertEquals(3, values.size());
            } else {
                throw new IllegalStateException("It is not possible to reach here: " + values);
            }

            assertThat(values.containsKey(T.id), is(true));
        }
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_EX11X_elementMap() {
        final Object edgeId = convertToEdgeId("josh", "created", "lop");
        final Traversal<Edge, Map<Object,Object>> traversal = get_g_EX11X_elementMap(edgeId);
        printTraversalForm(traversal);

        final Map<Object,Object> m = traversal.next();
        assertEquals(5, m.size());
        assertEquals(0.4d, m.get("weight"));
        assertEquals("created", m.get(label));
        assertEquals(edgeId, m.get(T.id));

        final Vertex vJosh = convertToVertex("josh");
        assertEquals(vJosh.id(), ((Map<Object,Object>) m.get(Direction.OUT)).get(T.id));

        if (!hasGraphComputerRequirement())
            assertEquals(vJosh.label(), ((Map<Object,Object>) m.get(Direction.OUT)).get(T.label));

        final Vertex vLop = convertToVertex("lop");
        assertEquals(vLop.id(), ((Map<Object,Object>) m.get(Direction.IN)).get(T.id));

        if (!hasGraphComputerRequirement())
            assertEquals(vLop.label(), ((Map<Object,Object>) m.get(Direction.IN)).get(T.label));

        assertThat(traversal.hasNext(), is(false));
    }

    public static class Traversals extends ElementMapTest {
        @Override
        public Traversal<Vertex, Map<Object, Object>> get_g_V_elementMap() {
            return g.V().elementMap();
        }

        @Override
        public Traversal<Vertex, Map<Object, Object>> get_g_V_elementMapXname_ageX() {
            return g.V().elementMap("name", "age");
        }

        @Override
        public Traversal<Edge, Map<Object, Object>> get_g_EX11X_elementMap(final Object e11Id) {
            return g.E(e11Id).elementMap();
        }
    }
}

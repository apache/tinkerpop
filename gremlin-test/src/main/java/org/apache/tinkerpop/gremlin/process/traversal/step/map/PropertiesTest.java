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
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.SINK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class PropertiesTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXname_ageX_value();

    public abstract Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXage_nameX_value();

    public abstract Traversal<Vertex, Object> get_g_V_hasXageX_properties_hasXid_nameIdX_value(final Object nameId);

    public abstract Traversal<Vertex, VertexProperty<String>> get_g_V_hasXageX_propertiesXnameX();

    public abstract Traversal<VertexProperty<String>, String> get_g_injectXg_VX1X_propertiesXnameX_nextX_value(final Object v1Id);

    public abstract Traversal<Vertex, Object> get_g_V_hasLabelXpersonX_properties_dedup_value();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_propertiesXname_ageX_value() {
        final Traversal<Vertex, Object> traversal = get_g_V_hasXageX_propertiesXname_ageX_value();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", 29, "vadas", 27, "josh", 32, "peter", 35), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_propertiesXage_nameX_value() {
        final Traversal<Vertex, Object> traversal = get_g_V_hasXageX_propertiesXage_nameX_value();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", 29, "vadas", 27, "josh", 32, "peter", 35), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_properties_hasXid_nameIdX_value() {
        final Traversal<Vertex, Object> traversal = get_g_V_hasXageX_properties_hasXid_nameIdX_value(convertToVertexPropertyId("marko", "name").next());
        printTraversalForm(traversal);
        checkResults(Collections.singletonList("marko"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_properties_hasXid_nameIdAsStringX_value() {
        final Traversal<Vertex, Object> traversal = get_g_V_hasXageX_properties_hasXid_nameIdX_value(convertToVertexPropertyId("marko", "name").next().toString());
        printTraversalForm(traversal);
        checkResults(Collections.singletonList("marko"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_propertiesXnameX() {
        final Traversal<Vertex, VertexProperty<String>> traversal = get_g_V_hasXageX_propertiesXnameX();
        printTraversalForm(traversal);
        final Set<String> keys = new HashSet<>();
        final Set<String> values = new HashSet<>();
        final Set<Object> ids = new HashSet<>();
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final VertexProperty<String> vertexProperty = traversal.next();
            keys.add(vertexProperty.key());
            values.add(vertexProperty.value());
            ids.add(vertexProperty.id());
            Object id = vertexProperty.id();
            id = id instanceof Number ? ((Number) id).longValue() : id;
            Object otherId = convertToVertex(graph, vertexProperty.value()).property("name").id();
            otherId = otherId instanceof Number ? ((Number) otherId).longValue() : otherId;
            assertEquals(otherId, id);
            assertEquals("name", vertexProperty.key());
            assertEquals(convertToVertex(graph, vertexProperty.value()).values("name").next(), vertexProperty.value());
            assertEquals(convertToVertex(graph, vertexProperty.value()).value("name"), vertexProperty.value());
        }
        assertEquals(4, counter);
        assertEquals(1, keys.size());
        assertTrue(keys.contains("name"));
        assertEquals(4, values.size());
        assertTrue(values.contains("marko"));
        assertTrue(values.contains("vadas"));
        assertTrue(values.contains("josh"));
        assertTrue(values.contains("peter"));
        assertEquals(4, ids.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_injectXg_VX1X_propertiesXnameX_nextX_value() {
        final Traversal<VertexProperty<String>, String> traversal = get_g_injectXg_VX1X_propertiesXnameX_nextX_value(convertToVertexId(graph, "marko"));
        printTraversalForm(traversal);
        checkResults(Collections.singletonList("marko"), traversal);
    }

    @Test
    @LoadGraphWith(SINK)
    public void g_V_hasLabelXpersonX_properties_dedup_value() {
        final Traversal<Vertex, Object> traversal = get_g_V_hasLabelXpersonX_properties_dedup_value();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("alice", 30, "bob", 30), traversal);
    }

    public static class Traversals extends PropertiesTest {
        @Override
        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXname_ageX_value() {
            return g.V().has("age").properties("name", "age").value();
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXage_nameX_value() {
            return g.V().has("age").properties("age", "name").value();
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_hasXageX_properties_hasXid_nameIdX_value(final Object nameId) {
            return g.V().has("age").properties().has(T.id, nameId).value();
        }

        @Override
        public Traversal<Vertex, VertexProperty<String>> get_g_V_hasXageX_propertiesXnameX() {
            return (Traversal<Vertex, VertexProperty<String>>) g.V().has("age").<String>properties("name");
        }

        @Override
        public Traversal<VertexProperty<String>, String> get_g_injectXg_VX1X_propertiesXnameX_nextX_value(final Object v1Id) {
            return g.<VertexProperty<String>>inject((VertexProperty) g.V(v1Id).properties("name").next()).value();
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_hasLabelXpersonX_properties_dedup_value() {
            return g.V().hasLabel("person").properties().dedup().value();
        }
    }
}

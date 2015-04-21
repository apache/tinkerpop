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
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.UseEngine;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class PropertiesTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXname_ageX_value();

    public abstract Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXage_nameX_value();

    public abstract Traversal<Vertex, Object> get_g_V_hasXageX_properties_hasXid_nameIdX_value(final Object nameId);

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_propertiesXname_ageX_value() {
        Arrays.asList(get_g_V_hasXageX_propertiesXage_nameX_value(), get_g_V_hasXageX_propertiesXname_ageX_value()).forEach(traversal -> {
            printTraversalForm(traversal);
            checkResults(Arrays.asList("marko", 29, "vadas", 27, "josh", 32, "peter", 35), traversal);
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_properties_hasXid_nameIdX_value() {
        final Traversal<Vertex,Object> traversal = get_g_V_hasXageX_properties_hasXid_nameIdX_value(convertToVertexPropertyId("marko", "name").next());
        printTraversalForm(traversal);
        checkResults(Collections.singletonList("marko"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_properties_hasXid_nameIdAsStringX_value() {
        final Traversal<Vertex,Object> traversal = get_g_V_hasXageX_properties_hasXid_nameIdX_value(convertToVertexPropertyId("marko", "name").next().toString());
        printTraversalForm(traversal);
        checkResults(Collections.singletonList("marko"), traversal);
    }

    @UseEngine(TraversalEngine.Type.STANDARD)
    @UseEngine(TraversalEngine.Type.COMPUTER)
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
    }
}


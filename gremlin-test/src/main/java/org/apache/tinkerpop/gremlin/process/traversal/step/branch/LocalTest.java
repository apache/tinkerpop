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
package org.apache.tinkerpop.gremlin.process.traversal.step.branch;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.CREW;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.as;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.properties;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class LocalTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasXlabel_personX_asXaX_localXoutXcreatedX_asXbXX_selectXa_bX_byXnameX_byXidX();

    public abstract Traversal<Vertex, Long> get_g_V_localXoutE_countX();

    public abstract Traversal<Vertex, String> get_g_VX1X_localXoutEXknowsX_limitX1XX_inV_name(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_V_localXbothEXcreatedX_limitX1XX_otherV_name();

    public abstract Traversal<Vertex, Edge> get_g_VX4X_localXbothEX1_createdX_limitX1XX(final Object v4Id);

    public abstract Traversal<Vertex, Edge> get_g_VX4X_localXbothEXknows_createdX_limitX1XX(final Object v4Id);

    public abstract Traversal<Vertex, String> get_g_VX4X_localXbothE_limitX1XX_otherV_name(final Object v4Id);

    public abstract Traversal<Vertex, String> get_g_VX4X_localXbothE_limitX2XX_otherV_name(final Object v4Id);

    public abstract Traversal<Vertex, String> get_g_V_localXinEXknowsX_limitX2XX_outV_name();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_localXmatchXproject__created_person__person_name_nameX_selectXname_projectX_by_byXnameX();

    @Test
    @LoadGraphWith(CREW)
    public void g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value() {
        final Traversal<Vertex, String> traversal = get_g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("brussels", "san diego", "centreville", "dulles", "baltimore", "bremen", "aachen", "kaiserslautern"), traversal);

    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXlabel_personX_asXaX_localXoutXcreatedX_asXbXX_selectXa_bX_byXnameX_byXidX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasXlabel_personX_asXaX_localXoutXcreatedX_asXbXX_selectXa_bX_byXnameX_byXidX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            final Map<String, Object> map = traversal.next();
            counter++;
            assertEquals(2, map.size());
            if (map.get("a").equals("marko")) {
                assertEquals(convertToVertexId("lop"), map.get("b"));
            } else if (map.get("a").equals("josh")) {
                assertTrue(convertToVertexId("lop").equals(map.get("b")) || convertToVertexId("ripple").equals(map.get("b")));
            } else if (map.get("a").equals("peter")) {
                assertEquals(convertToVertexId("lop"), map.get("b"));
            } else {
                fail("The following map should not have been returned: " + map);
            }
        }
        assertEquals(4, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_localXoutE_countX() {
        final Traversal<Vertex, Long> traversal = get_g_V_localXoutE_countX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(3l, 0l, 0l, 0l, 1l, 2l), traversal);

    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX4X_localXbothEXknows_createdX_limitX1XX() {
        final Traversal<Vertex, Edge> traversal = get_g_VX4X_localXbothEXknows_createdX_limitX1XX(convertToVertexId("josh"));
        printTraversalForm(traversal);
        final Edge edge = traversal.next();
        assertTrue(edge.label().equals("created") || edge.label().equals("knows"));
        assertTrue(edge.value("weight").equals(1.0d) || edge.value("weight").equals(0.4d));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX4X_localXbothE_limitX1XX_otherV_name() {
        final Traversal<Vertex, String> traversal = get_g_VX4X_localXbothE_limitX1XX_otherV_name(convertToVertexId("josh"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next();
            assertTrue(name.equals("marko") || name.equals("ripple") || name.equals("lop"));
        }
        assertEquals(1, counter);
        assertFalse(traversal.hasNext());

    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX4X_localXbothE_limitX2XX_otherV_name() {
        final Traversal<Vertex, String> traversal = get_g_VX4X_localXbothE_limitX2XX_otherV_name(convertToVertexId("josh"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next();
            assertTrue(name.equals("marko") || name.equals("ripple") || name.equals("lop"));
        }
        assertEquals(2, counter);
        assertFalse(traversal.hasNext());
    }


    @Test
    @LoadGraphWith(MODERN)
    public void g_V_localXinEXknowsX_limitX2XX_outV_name() {
        final Traversal<Vertex, String> traversal = get_g_V_localXinEXknowsX_limitX2XX_outV_name();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertEquals(traversal.next(), "marko");
        }
        assertFalse(traversal.hasNext());
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX4X_localXbothEX1_createdX_limitX1XX() {
        final Traversal<Vertex, Edge> traversal = get_g_VX4X_localXbothEX1_createdX_limitX1XX(convertToVertexId("josh"));
        printTraversalForm(traversal);
        final Edge edge = traversal.next();
        assertEquals("created", edge.label());
        assertTrue(edge.value("weight").equals(1.0d) || edge.value("weight").equals(0.4d));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_localXoutEXknowsX_limitX1XX_inV_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_localXoutEXknowsX_limitX1XX_inV_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
        final String name = traversal.next();
        assertTrue(name.equals("vadas") || name.equals("josh"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_localXbothEXcreatedX_limitX1XX_otherV_name() {
        final Traversal<Vertex, String> traversal = get_g_V_localXbothEXcreatedX_limitX1XX_otherV_name();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next();
            assertTrue(name.equals("marko") || name.equals("lop") || name.equals("josh") || name.equals("ripple") || name.equals("peter"));
        }
        assertEquals(5, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_localXmatchXproject__created_person__person_name_nameX_selectXname_projectX_by_byXnameX() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_localXmatchXproject__created_person__person_name_nameX_selectXname_projectX_by_byXnameX();
        printTraversalForm(traversal);
        checkResults(makeMapList(2,
                "name", "marko", "project", "lop",
                "name", "josh", "project", "lop",
                "name", "peter", "project", "lop",
                "name", "josh", "project", "ripple"), traversal);
        assertFalse(traversal.hasNext());
    }

    public static class Traversals extends LocalTest {

        @Override
        public Traversal<Vertex, String> get_g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value() {
            return g.V().local(properties("location").order().by(T.value, Order.asc).range(0, 2)).value();
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXlabel_personX_asXaX_localXoutXcreatedX_asXbXX_selectXa_bX_byXnameX_byXidX() {
            return g.V().has(T.label, "person").as("a").local(out("created").as("b")).select("a", "b").by("name").by(T.id);
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_localXoutE_countX() {
            return g.V().local(outE().count());
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_localXoutEXknowsX_limitX1XX_inV_name(final Object v1Id) {
            return g.V(v1Id).local(outE("knows").limit(1)).inV().values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_localXbothEXcreatedX_limitX1XX_otherV_name() {
            return g.V().local(bothE("created").limit(1)).otherV().values("name");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX4X_localXbothEX1_createdX_limitX1XX(final Object v4Id) {
            return g.V(v4Id).local(bothE("created").limit(1));
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX4X_localXbothEXknows_createdX_limitX1XX(final Object v4Id) {
            return g.V(v4Id).local(bothE("knows", "created").limit(1));
        }

        @Override
        public Traversal<Vertex, String> get_g_VX4X_localXbothE_limitX1XX_otherV_name(final Object v4Id) {
            return g.V(v4Id).local(bothE().limit(1)).otherV().values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX4X_localXbothE_limitX2XX_otherV_name(final Object v4Id) {
            return g.V(v4Id).local(bothE().limit(2)).otherV().values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_localXinEXknowsX_limitX2XX_outV_name() {
            return g.V().local(inE("knows").limit(2)).outV().values("name");
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_localXmatchXproject__created_person__person_name_nameX_selectXname_projectX_by_byXnameX() {
            return g.V().local(__.match(
                    as("project").in("created").as("person"),
                    as("person").values("name").as("name")))
                    .<String>select("name", "project").by().by("name");
        }
    }
}

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
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ConnectedComponent;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class ConnectedComponentTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_V_connectedComponent_hasXcomponentX();

    public abstract Traversal<Vertex, Map<String,Object>> get_g_V_hasLabelXsoftwareX_connectedComponent_project_byXnameX_byXcomponentX();

    public abstract Traversal<Vertex, Vertex> get_g_V_dedup_connectedComponent_hasXcomponentX();

    public abstract Traversal<Vertex, Map<String,Object>> get_g_V_connectedComponent_withXedges_bothEXknowsXX_withXpropertyName_clusterX_project_byXnameX_byXclusterX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_connectedComponent_hasXcomponentX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_connectedComponent_hasXcomponentX();
        printTraversalForm(traversal);
        assertEquals(6, IteratorUtils.count(traversal));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_dedup_connectedComponent_hasXcomponentX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_dedup_connectedComponent_hasXcomponentX();
        printTraversalForm(traversal);
        assertEquals(6, IteratorUtils.count(traversal));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXsoftwareX_connectedComponent_project_byXnameX_byXcomponentX() {
        final Traversal<Vertex, Map<String,Object>> traversal = get_g_V_hasLabelXsoftwareX_connectedComponent_project_byXnameX_byXcomponentX();
        printTraversalForm(traversal);
        int counter = 0;

        String softwareComponent = null;

        while (traversal.hasNext()) {
            final Map<String,Object> m = traversal.next();
            final String name = m.get("name").toString();

            // since the two returned values are in the same component just need to grab the first one and use that
            // as a comparison to the other
            if (null == softwareComponent)
                softwareComponent = m.get("component").toString();

            assertNotNull(softwareComponent);

            switch (name) {
                case "lop":
                case "ripple":
                    counter++;
                    assertEquals(softwareComponent, m.get("component"));
                    break;
            }
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_connectedComponent_withXEDGES_bothEXknowsXX_withXPROPERTY_NAME_clusterX_project_byXnameX_byXclusterX() {
        final Traversal<Vertex, Map<String,Object>> traversal = get_g_V_connectedComponent_withXedges_bothEXknowsXX_withXpropertyName_clusterX_project_byXnameX_byXclusterX();
        printTraversalForm(traversal);
        int counter = 0;

        String softwareComponent = null;

        // sort to make sure that "peter" is always last
        final Iterator<Map<String,Object>> itty = traversal.toList().stream().sorted((o1, o2) -> o1.get("name").toString().equals("peter") ? 1 : -1).iterator();

        while (itty.hasNext()) {
            final Map<String,Object> m = itty.next();
            final String name = m.get("name").toString();

            // since the three of the returned values are in the same component just need to grab the first one and
            // use that as a comparison to the others and then for the fourth returned item "peter" just needs to not
            // be in the same component
            if (null == softwareComponent)
                softwareComponent = m.get("cluster").toString();

            assertNotNull(softwareComponent);

            switch (name) {
                case "marko":
                case "vadas":
                case "josh":
                    counter++;
                    assertEquals(softwareComponent, m.get("cluster"));
                    break;
                case "peter":
                    counter++;
                    assertNotEquals(softwareComponent, m.get("cluster"));
                    break;
            }
        }
        assertEquals(4, counter);
    }

    public static class Traversals extends ConnectedComponentTest {
        @Override
        public Traversal<Vertex, Vertex> get_g_V_connectedComponent_hasXcomponentX() {
            return g.V().connectedComponent().has(ConnectedComponent.component);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_dedup_connectedComponent_hasXcomponentX() {
            return g.V().dedup().connectedComponent().has(ConnectedComponent.component);
        }

        @Override
        public Traversal<Vertex, Map<String,Object>> get_g_V_hasLabelXsoftwareX_connectedComponent_project_byXnameX_byXcomponentX() {
            return g.V().hasLabel("software").connectedComponent().project("name","component").by("name").by(ConnectedComponent.component);
        }
        @Override
        public Traversal<Vertex, Map<String,Object>> get_g_V_connectedComponent_withXedges_bothEXknowsXX_withXpropertyName_clusterX_project_byXnameX_byXclusterX() {
            return g.V().hasLabel("person").connectedComponent().with(ConnectedComponent.edges, bothE("knows")).with(ConnectedComponent.propertyName, "cluster").project("name","cluster").by("name").by("cluster");
        }
    }
}

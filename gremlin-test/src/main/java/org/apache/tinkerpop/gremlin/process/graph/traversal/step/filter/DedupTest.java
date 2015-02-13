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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.filter;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.Scope;
import org.apache.tinkerpop.gremlin.process.T;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.graph.traversal.__.bothE;
import static org.apache.tinkerpop.gremlin.process.graph.traversal.__.dedup;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public abstract class DedupTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_both_dedup_name();

    public abstract Traversal<Vertex, String> get_g_V_both_hasXlabel_softwareX_dedup_byXlangX_name();

    public abstract Traversal<Vertex, String> get_g_V_both_propertiesXnameX_orderXa_bX_dedup_value();

    public abstract Traversal<Vertex, Map<String, Set<Double>>> get_g_V_group_byXlabelX_byXbothE_valuesXweightX_foldX_byXdedupXlocalXX_cap();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_both_dedup_name() {
        final Traversal<Vertex, String> traversal = get_g_V_both_dedup_name();
        printTraversalForm(traversal);
        final List<String> names = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(6, names.size());
        assertTrue(names.contains("marko"));
        assertTrue(names.contains("vadas"));
        assertTrue(names.contains("lop"));
        assertTrue(names.contains("josh"));
        assertTrue(names.contains("ripple"));
        assertTrue(names.contains("peter"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_both_hasXlabel_softwareX_dedup_byXlangX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_both_hasXlabel_softwareX_dedup_byXlangX_name();
        printTraversalForm(traversal);
        final List<String> names = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(1, names.size());
        assertTrue(names.contains("lop") || names.contains("ripple"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_both_name_orderXa_bX_dedup() {
        final Traversal<Vertex, String> traversal = get_g_V_both_propertiesXnameX_orderXa_bX_dedup_value();
        printTraversalForm(traversal);
        final List<String> names = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(6, names.size());
        assertEquals("josh", names.get(0));
        assertEquals("lop", names.get(1));
        assertEquals("marko", names.get(2));
        assertEquals("peter", names.get(3));
        assertEquals("ripple", names.get(4));
        assertEquals("vadas", names.get(5));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_group_byXlabelX_byXbothE_valuesXweightX_foldX_byXdedupXlocalXX_cap() {
        final Traversal<Vertex, Map<String, Set<Double>>> traversal =
                get_g_V_group_byXlabelX_byXbothE_valuesXweightX_foldX_byXdedupXlocalXX_cap();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<String, Set<Double>> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, map.size());
        assertEquals(3, map.get("software").size());
        assertEquals(4, map.get("person").size());
        assertEquals(new HashSet<>(Arrays.asList(0.2, 0.4, 1.0)), map.get("software"));
        assertEquals(new HashSet<>(Arrays.asList(0.2, 0.4, 0.5, 1.0)), map.get("person"));
    }

    public static class StandardTest extends DedupTest {

        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_dedup_name() {
            return g.V().both().dedup().values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_hasXlabel_softwareX_dedup_byXlangX_name() {
            return g.V().both().has(T.label, "software").dedup().by("lang").values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_propertiesXnameX_orderXa_bX_dedup_value() {
            return g.V().both().<String>properties("name").order().by((a, b) -> a.value().compareTo(b.value())).dedup().value();
        }

        @Override
        public Traversal<Vertex, Map<String, Set<Double>>> get_g_V_group_byXlabelX_byXbothE_valuesXweightX_foldX_byXdedupXlocalXX_cap() {
            return g.V().group().by(T.label).by(bothE().values("weight").fold()).by(dedup(Scope.local)).cap();
        }
    }

    public static class ComputerTest extends StandardTest {

        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_dedup_name() {
            return super.get_g_V_both_dedup_name()/*.submit(g.compute())*/; // TODO
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_hasXlabel_softwareX_dedup_byXlangX_name() {
            return super.get_g_V_both_hasXlabel_softwareX_dedup_byXlangX_name()/*.submit(g.compute())*/; // TODO
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_propertiesXnameX_orderXa_bX_dedup_value() {
            return super.get_g_V_both_propertiesXnameX_orderXa_bX_dedup_value()/*.submit(g.compute())*/; // TODO
        }

        @Override
        public Traversal<Vertex, Map<String, Set<Double>>> get_g_V_group_byXlabelX_byXbothE_valuesXweightX_foldX_byXdedupXlocalXX_cap() {
            return super.get_g_V_group_byXlabelX_byXbothE_valuesXweightX_foldX_byXdedupXlocalXX_cap().submit(g.compute());
        }
    }
}

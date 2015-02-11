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

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Daniel Kuppitz (daniel at thinkaurelius.com)
 */
public abstract class DedupTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, String> get_g_V_both_dedup_name();

    public abstract Traversal<Vertex, String> get_g_V_both_hasXlabel_softwareX_dedup_byXlangX_name();

    public abstract Traversal<Vertex, String> get_g_V_both_propertiesXnameX_orderXa_bX_dedup_value();

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

    public static class StandardTest extends DedupTest {

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
    }
}

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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class SideEffectCapTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_hasXageX_groupCountXaX_byXnameX_out_capXaX();

    public abstract Traversal<Vertex, Map<String, Map<Object, Long>>> get_g_V_chooseXlabel_person__age_groupCountXaX__name_groupCountXbXX_capXa_bX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_groupCountXaX_byXnameX_out_capXaX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_hasXageX_groupCountXaX_byXnameX_out_capXaX();
        printTraversalForm(traversal);
        Map<String, Long> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(map.get("marko"), new Long(1l));
        assertEquals(map.get("vadas"), new Long(1l));
        assertEquals(map.get("peter"), new Long(1l));
        assertEquals(map.get("josh"), new Long(1l));
        assertEquals(map.size(), 4);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_chooseXlabel_person__age_groupCountXaX__name_groupCountXbXX_capXa_bX() {
        final Traversal<Vertex, Map<String, Map<Object, Long>>> traversal = get_g_V_chooseXlabel_person__age_groupCountXaX__name_groupCountXbXX_capXa_bX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<String, Map<Object, Long>> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, map.size());
        assertTrue(map.containsKey("a"));
        assertTrue(map.containsKey("b"));
        assertEquals(4, map.get("a").size());
        assertEquals(2, map.get("b").size());
        assertEquals(1l, map.get("a").get(27).longValue());
        assertEquals(1l, map.get("a").get(29).longValue());
        assertEquals(1l, map.get("a").get(32).longValue());
        assertEquals(1l, map.get("a").get(35).longValue());
        assertEquals(1l, map.get("b").get("ripple").longValue());
        assertEquals(1l, map.get("b").get("lop").longValue());
        assertFalse(traversal.hasNext());
    }

    public static class Traversals extends SideEffectCapTest {
        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_hasXageX_groupCountXaX_byXnameX_out_capXaX() {
            return g.V().has("age").groupCount("a").by("name").out().cap("a");
        }

        @Override
        public Traversal<Vertex, Map<String, Map<Object, Long>>> get_g_V_chooseXlabel_person__age_groupCountXaX__name_groupCountXbXX_capXa_bX() {
            return g.V().choose(has(T.label, "person"),
                    values("age").groupCount("a"),
                    values("name").groupCount("b")).cap("a", "b");
        }
    }
}

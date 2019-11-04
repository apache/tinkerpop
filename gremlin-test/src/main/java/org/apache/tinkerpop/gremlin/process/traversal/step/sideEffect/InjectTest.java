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
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class InjectTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_VX1X_out_injectXv2X_name(final Object v1Id, final Object v2Id);

    public abstract Traversal<Vertex, Path> get_g_VX1X_out_name_injectXdanielX_asXaX_mapXlengthX_path(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_VX1X_injectXg_VX4XX_out_name(final Object v1Id, final Object v4Id);

    public abstract Traversal<Integer, Integer> get_g_injectXnull_1_3_nullX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_injectXv2X_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_out_injectXv2X_name(convertToVertexId("marko"), convertToVertexId("vadas"));
        printTraversalForm(traversal);
        final Map<String, Long> counter = new HashMap<>();
        while (traversal.hasNext()) {
            MapHelper.incr(counter, traversal.next(), 1l);
        }
        assertEquals(3, counter.size());
        assertEquals(1l, counter.get("josh").longValue());
        assertEquals(1l, counter.get("lop").longValue());
        assertEquals(2l, counter.get("vadas").longValue());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_name_injectXdanielX_asXaX_mapXlengthX_path() {
        final Traversal<Vertex, Path> traversal = get_g_VX1X_out_name_injectXdanielX_asXaX_mapXlengthX_path(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Path path = traversal.next();
            if (path.get("a").equals("daniel")) {
                assertEquals(2, path.size());
                assertEquals(6, (int) path.get(1));
            } else {
                assertEquals(4, path.size());
                assertEquals(path.<String>get("a").length(), (int) path.get(3));
            }
        }
        assertEquals(4, counter);
    }

    @Test
    public void g_injectXnull_1_3_nullX() {
        final Traversal<Integer, Integer> traversal = get_g_injectXnull_1_3_nullX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(null, 1, 3, null), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_injectXg_VX4XX_out_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_injectXg_VX4XX_out_name(convertToVertexId("marko"), convertToVertexId("josh"));
        printTraversalForm(traversal);
        checkResults(Arrays.asList("ripple", "lop", "lop", "vadas", "josh"), traversal);
    }

    public static class Traversals extends InjectTest {

        @Override
        public Traversal<Vertex, String> get_g_VX1X_out_injectXv2X_name(final Object v1Id, final Object v2Id) {
            return g.V(v1Id).out().inject(g.V(v2Id).next()).values("name");
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_out_name_injectXdanielX_asXaX_mapXlengthX_path(final Object v1Id) {
            return g.V(v1Id).out().<String>values("name").inject("daniel").as("a").map(t -> t.get().length()).path();
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_injectXg_VX4XX_out_name(final Object v1Id, final Object v4Id) {
            return g.V(v1Id).inject(g.V(v4Id).next()).out().values("name");
        }

        @Override
        public Traversal<Integer, Integer> get_g_injectXnull_1_3_nullX() {
            return g.inject(null, 1, 3, null);
        }
    }
}

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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class UnionTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_unionXout__inX_name();

    public abstract Traversal<Vertex, String> get_g_VX1X_unionXrepeatXoutX_timesX2X__outX_name(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_V_chooseXlabel_is_person__unionX__out_lang__out_nameX__in_labelX();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_chooseXlabel_is_person__unionX__out_lang__out_nameX__in_labelX_groupCount();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_unionXrepeatXunionXoutXcreatedX__inXcreatedXX_timesX2X__repeatXunionXinXcreatedX__outXcreatedXX_timesX2XX_label_groupCount();

    public abstract Traversal<Vertex, Number> get_g_VX1_2X_unionXoutE_count__inE_count__outE_weight_sumX(final Object v1Id, final Object v2Id);

    public abstract Traversal<Vertex, Number> get_g_VX1_2X_localXunionXoutE_count__inE_count__outE_weight_sumXX(final Object v1Id, final Object v2Id);

    public abstract Traversal<Vertex, Number> get_g_VX1_2X_localXunionXcountXX(final Object v1Id, final Object v2Id);

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_unionXout__inX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_unionXout__inX_name();
        printTraversalForm(traversal);
        checkResults(new HashMap<String, Long>() {{
            put("marko", 3l);
            put("lop", 3l);
            put("peter", 1l);
            put("ripple", 1l);
            put("josh", 3l);
            put("vadas", 1l);
        }}, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_unionXrepeatXoutX_timesX2X__outX_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_unionXrepeatXoutX_timesX2X__outX_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
        checkResults(new HashMap<String, Long>() {{
            put("lop", 2l);
            put("ripple", 1l);
            put("josh", 1l);
            put("vadas", 1l);
        }}, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_chooseXlabel_eq_person__unionX__out_lang__out_nameX__in_labelX() {
        final Traversal<Vertex, String> traversal = get_g_V_chooseXlabel_is_person__unionX__out_lang__out_nameX__in_labelX();
        printTraversalForm(traversal);
        checkResults(new HashMap<String, Long>() {{
            put("lop", 3l);
            put("ripple", 1l);
            put("java", 4l);
            put("josh", 1l);
            put("vadas", 1l);
            put("person", 4l);
        }}, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_chooseXlabel_eq_person__unionX__out_lang__out_nameX__in_labelX_groupCount() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_chooseXlabel_is_person__unionX__out_lang__out_nameX__in_labelX_groupCount();
        printTraversalForm(traversal);
        final Map<String, Long> groupCount = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(3l, groupCount.get("lop").longValue());
        assertEquals(1l, groupCount.get("ripple").longValue());
        assertEquals(4l, groupCount.get("java").longValue());
        assertEquals(1l, groupCount.get("josh").longValue());
        assertEquals(1l, groupCount.get("vadas").longValue());
        assertEquals(4l, groupCount.get("person").longValue());
        assertEquals(6, groupCount.size());

    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_unionXrepeatXunionXoutXcreatedX__inXcreatedXX_timesX2X__repeatXunionXinXcreatedX__outXcreatedXX_timesX2XX_label_groupCount() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_unionXrepeatXunionXoutXcreatedX__inXcreatedXX_timesX2X__repeatXunionXinXcreatedX__outXcreatedXX_timesX2XX_label_groupCount();
        printTraversalForm(traversal);
        final Map<String, Long> groupCount = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(12l, groupCount.get("software").longValue());
        assertEquals(20l, groupCount.get("person").longValue());
        assertEquals(2, groupCount.size());

    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1_2X_localXunionXoutE_count__inE_count__outE_weight_sumXX() {
        final Traversal<Vertex, Number> traversal = get_g_VX1_2X_localXunionXoutE_count__inE_count__outE_weight_sumXX(convertToVertexId("marko"), convertToVertexId("vadas"));
        printTraversalForm(traversal);
        checkResults(Arrays.asList(0l, 0l, 0, 3l, 1l, 1.9d), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1_2X_localXunionXcountXX() {
        final Traversal<Vertex, Number> traversal = get_g_VX1_2X_localXunionXcountXX(convertToVertexId("marko"), convertToVertexId("vadas"));
        printTraversalForm(traversal);
        checkResults(Arrays.asList(1L, 1L), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1_2X_unionXoutE_count__inE_count__outE_weight_sumX() {
        final Traversal<Vertex, Number> traversal = get_g_VX1_2X_unionXoutE_count__inE_count__outE_weight_sumX(convertToVertexId("marko"), convertToVertexId("vadas"));
        printTraversalForm(traversal);
        checkResults(Arrays.asList(3l, 1.9d, 1l), traversal);
    }

    public static class Traversals extends UnionTest {

        @Override
        public Traversal<Vertex, String> get_g_V_unionXout__inX_name() {
            return g.V().union(out(), in()).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_unionXrepeatXoutX_timesX2X__outX_name(final Object v1Id) {
            return g.V(v1Id).union(repeat(out()).times(2), out()).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_chooseXlabel_is_person__unionX__out_lang__out_nameX__in_labelX() {
            return g.V().choose(label().is("person"), union(out().values("lang"), out().values("name")), in().label());
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_chooseXlabel_is_person__unionX__out_lang__out_nameX__in_labelX_groupCount() {
            return (Traversal) g.V().choose(label().is("person"), union(out().values("lang"), out().values("name")), in().label()).groupCount();
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_unionXrepeatXunionXoutXcreatedX__inXcreatedXX_timesX2X__repeatXunionXinXcreatedX__outXcreatedXX_timesX2XX_label_groupCount() {
            return (Traversal) g.V().union(
                    repeat(union(
                            out("created"),
                            in("created"))).times(2),
                    repeat(union(
                            in("created"),
                            out("created"))).times(2)).label().groupCount();
        }

        @Override
        public Traversal<Vertex, Number> get_g_VX1_2X_unionXoutE_count__inE_count__outE_weight_sumX(final Object v1Id, final Object v2Id) {
            return g.V(v1Id, v2Id).union(outE().count(), inE().count(), (Traversal) outE().values("weight").sum());
        }

        @Override
        public Traversal<Vertex, Number> get_g_VX1_2X_localXunionXoutE_count__inE_count__outE_weight_sumXX(final Object v1Id, final Object v2Id) {
            return g.V(v1Id, v2Id).local(union(outE().count(), inE().count(), (Traversal) outE().values("weight").sum()));
        }

        @Override
        public Traversal<Vertex, Number> get_g_VX1_2X_localXunionXcountXX(final Object v1Id, final Object v2Id) {
            return g.V(v1Id, v2Id).local(union((Traversal) count()));
        }
    }
}

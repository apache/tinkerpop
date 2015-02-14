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
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.engine.StandardTraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.graph.traversal.__.*;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class RangeTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_out_limitX2X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_localXoutE_limitX1X_inVX_limitX3X();

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_repeatXbothX_timesX3X_rangeX5_11X();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXsoftwareX_asXsX_localXinEXcreatedX_valuesXweightX_fold_limitXlocal_1XX_asXwX_select_byXnameX_by();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXsoftwareX_asXsX_localXinEXcreatedX_valuesXweightX_fold_rangeXlocal_1_3XX_asXwX_select_byXnameX_by();

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_limitX2X() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out_limitX2X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            traversal.next();
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_localXoutE_limitX1X_inVX_limitX3X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_localXoutE_limitX1X_inVX_limitX3X();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            traversal.next();
        }
        assertEquals(3, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next().value("name");
            assertTrue(name.equals("lop") || name.equals("ripple"));
        }
        assertEquals(1, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next().value("name");
            assertTrue(name.equals("lop") || name.equals("ripple"));
        }
        assertEquals(1, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next().value("name");
            assertTrue(name.equals("marko") || name.equals("josh") || name.equals("peter"));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next().value("name");
            assertTrue(name.equals("marko") || name.equals("josh") || name.equals("peter"));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeatXbothX_timesX3X_rangeX5_11X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_repeatXbothX_timesX3X_rangeX5_11X();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            traversal.next();
            counter++;
        }
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXsoftwareX_asXsX_localXinEXcreatedX_valuesXweightX_fold_limitXlocal_1XX_asXwX_select_byXnameX_by() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasLabelXsoftwareX_asXsX_localXinEXcreatedX_valuesXweightX_fold_limitXlocal_1XX_asXwX_select_byXnameX_by();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            final Map<String, Object> map = traversal.next();
            final List<Double> weights = (List<Double>) map.get("w");
            if (map.get("s").equals("lop")) {
                assertEquals(1, weights.size());
                assertTrue(Arrays.asList(0.4, 0.2).contains(weights.get(0)));
            } else if (map.get("s").equals("ripple")) {
                assertEquals(1, weights.size());
                assertEquals(1.0, weights.get(0), 0.0);
            } else {
                assertTrue(false);
            }
            counter++;
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXsoftwareX_asXsX_localXinEXcreatedX_valuesXweightX_fold_rangeXlocal_1_3XX_asXwX_select_byXnameX_by() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasLabelXsoftwareX_asXsX_localXinEXcreatedX_valuesXweightX_fold_rangeXlocal_1_3XX_asXwX_select_byXnameX_by();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            final Map<String, Object> map = traversal.next();
            final List<Double> weights = (List<Double>) map.get("w");
            if (map.get("s").equals("lop")) {
                assertEquals(2, weights.size());
                assertTrue(Arrays.asList(0.2, 0.4).contains(weights.get(0)));
                assertTrue(Arrays.asList(0.2, 0.4).contains(weights.get(1)));
                assertNotEquals(weights.get(0), weights.get(1));
            } else if (map.get("s").equals("ripple")) {
                assertEquals(0, weights.size());
            } else {
                assertTrue(false);
            }
            counter++;
        }
        assertEquals(2, counter);
    }

    public static class StandardTest extends RangeTest {

        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_limitX2X(final Object v1Id) {
            return g.V(v1Id).out().limit(2);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_localXoutE_limitX1X_inVX_limitX3X() {
            return g.V().local(outE().limit(1)).inV().limit(3);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV(final Object v1Id) {
            return g.V(v1Id).out("knows").outE("created").range(0, 1).inV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X(final Object v1Id) {
            return g.V(v1Id).out("knows").out("created").range(0, 1);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X(final Object v1Id) {
            return g.V(v1Id).out("created").in("created").range(1, 3);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV(final Object v1Id) {
            return g.V(v1Id).out("created").inE("created").range(1, 3).outV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXbothX_timesX3X_rangeX5_11X() {
            return g.V().repeat(both()).times(3).range(5, 11);
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXsoftwareX_asXsX_localXinEXcreatedX_valuesXweightX_fold_limitXlocal_1XX_asXwX_select_byXnameX_by() {
            return g.V().hasLabel("software").as("s").local(inE("created").values("weight").fold().limit(Scope.local, 1)).as("w").select().by("name").by();
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXsoftwareX_asXsX_localXinEXcreatedX_valuesXweightX_fold_rangeXlocal_1_3XX_asXwX_select_byXnameX_by() {
            return g.V().hasLabel("software").as("s").local(inE("created").values("weight").fold().range(Scope.local, 1, 3)).as("w").select().by("name").by();
        }
    }

    public static class ComputerTest extends StandardTest {

        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_limitX2X(Object v1Id) {
            g.engine(StandardTraversalEngine.instance()); // TODO
            return super.get_g_VX1X_out_limitX2X(v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_localXoutE_limitX1X_inVX_limitX3X() {
            g.engine(StandardTraversalEngine.instance()); // TODO
            return super.get_g_V_localXoutE_limitX1X_inVX_limitX3X();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV(Object v1Id) {
            g.engine(StandardTraversalEngine.instance()); // TODO
            return super.get_g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV(v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X(Object v1Id) {
            g.engine(StandardTraversalEngine.instance()); // TODO
            return super.get_g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X(v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X(Object v1Id) {
            g.engine(StandardTraversalEngine.instance()); // TODO
            return super.get_g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X(v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV(Object v1Id) {
            g.engine(StandardTraversalEngine.instance()); // TODO
            return super.get_g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV(v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXbothX_timesX3X_rangeX5_11X() {
            g.engine(StandardTraversalEngine.instance()); // TODO
            return super.get_g_V_repeatXbothX_timesX3X_rangeX5_11X();
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXsoftwareX_asXsX_localXinEXcreatedX_valuesXweightX_fold_limitXlocal_1XX_asXwX_select_byXnameX_by() {
            g.engine(StandardTraversalEngine.instance()); // TODO: the traversal should work in computer mode, but throws a ClassCastException
            return super.get_g_V_hasLabelXsoftwareX_asXsX_localXinEXcreatedX_valuesXweightX_fold_limitXlocal_1XX_asXwX_select_byXnameX_by();
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXsoftwareX_asXsX_localXinEXcreatedX_valuesXweightX_fold_rangeXlocal_1_3XX_asXwX_select_byXnameX_by() {
            g.engine(StandardTraversalEngine.instance()); // TODO: the traversal should work in computer mode, but throws a ClassCastException
            return super.get_g_V_hasLabelXsoftwareX_asXsX_localXinEXcreatedX_valuesXweightX_fold_rangeXlocal_1_3XX_asXwX_select_byXnameX_by();
        }
    }
}
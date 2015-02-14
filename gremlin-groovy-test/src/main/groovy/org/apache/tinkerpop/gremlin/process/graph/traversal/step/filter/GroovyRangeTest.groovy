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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.filter

import org.apache.tinkerpop.gremlin.process.Scope
import org.apache.tinkerpop.gremlin.process.Traversal
import org.apache.tinkerpop.gremlin.process.graph.traversal.__
import org.apache.tinkerpop.gremlin.process.traversal.engine.StandardTraversalEngine
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyRangeTest {

    public static class StandardTest extends RangeTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_limitX2X(final Object v1Id) {
            g.V(v1Id).out.limit(2)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_localXoutE_limitX1X_inVX_limitX3X() {
            g.V.local(__.outE.limit(3)).inV.limit(3)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV(final Object v1Id) {
            g.V(v1Id).out('knows').outE('created')[0].inV()
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X(final Object v1Id) {
            g.V(v1Id).out('knows').out('created')[0]
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X(final Object v1Id) {
            g.V(v1Id).out('created').in('created')[1..3]
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV(final Object v1Id) {
            g.V(v1Id).out('created').inE('created')[1..3].outV
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXbothX_timesX3X_rangeX5_11X() {
            g.V().repeat(__.both).times(3)[5..11];
        }

        @Override
        Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXsoftwareX_asXsX_localXinEXcreatedX_valuesXweightX_fold_limitXlocal_1XX_asXwX_select_byXnameX_by() {
            g.V().hasLabel('software').as('s').local(__.inE('created').values('weight').fold().limit(Scope.local, 1)).as('w').select().by('name').by()
        }

        @Override
        Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXsoftwareX_asXsX_localXinEXcreatedX_valuesXweightX_fold_rangeXlocal_1_2XX_asXwX_select_byXnameX_by() {
            g.V().hasLabel('software').as('s').local(__.inE('created').values('weight').fold().range(Scope.local, 1, 2)).as('w').select().by('name').by()
        }
    }

    public static class ComputerTest extends RangeTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_limitX2X(final Object v1Id) {
            g.engine(StandardTraversalEngine.instance()) // TODO
            g.V(v1Id).out.limit(2)
            //ComputerTestHelper.compute("g.V(v1Id).out.limit(2)", g)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_localXoutE_limitX1X_inVX_limitX3X() {
            g.engine(StandardTraversalEngine.instance()) // TODO
            g.V.local(__.outE.limit(3)).inV.limit(3)
            //ComputerTestHelper.compute("g.V.local(__.outE.limit(3)).inV.limit(3)", g)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV(final Object v1Id) {
            g.engine(StandardTraversalEngine.instance()) // TODO
            g.V(v1Id).out('knows').outE('created')[0].inV()
            //ComputerTestHelper.compute("g.V(v1Id).out('knows').outE('created')[0].inV()", g)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X(final Object v1Id) {
            g.engine(StandardTraversalEngine.instance()) // TODO
            g.V(v1Id).out('knows').out('created')[0]
            //ComputerTestHelper.compute("g.V(v1Id).out('knows').out('created')[0]", g)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X(final Object v1Id) {
            g.engine(StandardTraversalEngine.instance()) // TODO
            g.V(v1Id).out('created').in('created')[1..3]
            //ComputerTestHelper.compute("g.V(v1Id).out('created').in('created')[1..3]", g)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV(final Object v1Id) {
            g.engine(StandardTraversalEngine.instance()) // TODO
            g.V(v1Id).out('created').inE('created')[1..3].outV
            //ComputerTestHelper.compute("g.V(v1Id).out('created').inE('created')[1..3].outV", g)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXbothX_timesX3X_rangeX5_11X() {
            g.engine(StandardTraversalEngine.instance()) // TODO
            g.V().repeat(__.both).times(3)[5..11]
            //ComputerTestHelper.compute("g.V().repeat(__.both).times(3)[5..11]", g)
        }

        @Override
        Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXsoftwareX_asXsX_localXinEXcreatedX_valuesXweightX_fold_limitXlocal_1XX_asXwX_select_byXnameX_by() {
            g.engine(StandardTraversalEngine.instance())
            // TODO: the traversal should work in computer mode, but throws a ClassCastException
            g.V().hasLabel('software').as('s').local(__.inE('created').values('weight').fold().limit(Scope.local, 1)).as('w').select().by('name').by()
            //ComputerTestHelper.compute("g.V().hasLabel('software').as('s').local(__.inE('created').values('weight').fold().limit(Scope.local, 1)).as('p').select().by('name').by()", g)
        }

        @Override
        Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXsoftwareX_asXsX_localXinEXcreatedX_valuesXweightX_fold_rangeXlocal_1_2XX_asXwX_select_byXnameX_by() {
            g.engine(StandardTraversalEngine.instance())
            // TODO: the traversal should work in computer mode, but throws a ClassCastException
            g.V().hasLabel('software').as('s').local(__.inE('created').values('weight').fold().range(Scope.local, 1, 2)).as('w').select().by('name').by()
            //ComputerTestHelper.compute("g.V().hasLabel('software').as('s').local(__.inE('created').values('weight').fold().range(Scope.local, 1, 2)).as('p').select().by('name').by()", g)
        }
    }
}

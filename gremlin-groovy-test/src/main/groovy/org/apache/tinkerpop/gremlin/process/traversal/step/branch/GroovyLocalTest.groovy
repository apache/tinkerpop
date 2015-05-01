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
package org.apache.tinkerpop.gremlin.process.traversal.step.branch

import org.apache.tinkerpop.gremlin.process.UseEngine
import org.apache.tinkerpop.gremlin.process.computer.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Order
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyLocalTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends LocalTest {

        @Override
        public Traversal<Vertex, String> get_g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value() {
            g.V.local(__.properties('location').order.by(T.value, Order.incr).limit(2)).value
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXlabel_personX_asXaX_localXoutXcreatedX_asXbXX_selectXa_bX_byXnameX_byXidX() {
            g.V.has(T.label, 'person').as('a').local(__.out('created').as('b')).select('a', 'b').by('name').by(T.id)
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_localXoutE_countX() {
            g.V.local(__.outE.count());
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_localXoutEXknowsX_limitX1XX_inV_name(final Object v1Id) {
            g.V(v1Id).local(__.outE('knows').limit(1)).inV.name
        }

        @Override
        public Traversal<Vertex, String> get_g_V_localXbothEXcreatedX_limitX1XX_otherV_name() {
            g.V().local(__.bothE('created').limit(1)).otherV.name
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX4X_localXbothEX1_createdX_limitX1XX(final Object v4Id) {
            g.V(v4Id).local(__.bothE('created').limit(1))
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX4X_localXbothEXknows_createdX_limitX1XX(final Object v4Id) {
            g.V(v4Id).local(__.bothE('knows', 'created').limit(1))
        }

        @Override
        public Traversal<Vertex, String> get_g_VX4X_localXbothE_limitX1XX_otherV_name(final Object v4Id) {
            g.V(v4Id).local(__.bothE.limit(1)).otherV.name
        }

        @Override
        public Traversal<Vertex, String> get_g_VX4X_localXbothE_limitX2XX_otherV_name(final Object v4Id) {
            g.V(v4Id).local(__.bothE.limit(2).otherV).name
        }

        @Override
        public Traversal<Vertex, String> get_g_V_localXinEXknowsX_limitX2XX_outV_name() {
            g.V().inE('knows').local(__.limit(2).outV).name
        }
    }

    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class ComputerTraversals extends LocalTest {

        @Override
        public Traversal<Vertex, String> get_g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value() {
            ComputerTestHelper.compute("g.V.local(__.properties('location').order.by(T.value,Order.incr).limit(2)).value", g);
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXlabel_personX_asXaX_localXoutXcreatedX_asXbXX_selectXa_bX_byXnameX_byXidX() {
            ComputerTestHelper.compute("g.V.has(T.label, 'person').as('a').local(__.out('created').as('b')).select('a', 'b').by('name').by(T.id)", g);
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_localXoutE_countX() {
            ComputerTestHelper.compute("g.V.local(__.outE.count())", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_localXoutEXknowsX_limitX1XX_inV_name(final Object v1Id) {
            ComputerTestHelper.compute("g.V(v1Id).local(__.outE('knows').limit(1)).inV.name", g, "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_localXbothEXcreatedX_limitX1XX_otherV_name() {
            ComputerTestHelper.compute("g.V().local(__.bothE('created').limit(1)).otherV.name", g);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX4X_localXbothEX1_createdX_limitX1XX(final Object v4Id) {
            ComputerTestHelper.compute("g.V(v4Id).local(__.bothE('created').limit(1))", g, "v4Id", v4Id);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX4X_localXbothEXknows_createdX_limitX1XX(final Object v4Id) {
            ComputerTestHelper.compute("g.V(v4Id).local(__.bothE('knows', 'created').limit(1))", g, "v4Id", v4Id);
        }

        @Override
        public Traversal<Vertex, String> get_g_VX4X_localXbothE_limitX1XX_otherV_name(final Object v4Id) {
            ComputerTestHelper.compute("g.V(v4Id).local(__.bothE.limit(1)).otherV.name", g, "v4Id", v4Id);
        }

        @Override
        public Traversal<Vertex, String> get_g_VX4X_localXbothE_limitX2XX_otherV_name(final Object v4Id) {
            ComputerTestHelper.compute("g.V(v4Id).local(__.bothE.limit(2)).otherV.name", g, "v4Id", v4Id);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_localXinEXknowsX_limitX2XX_outV_name() {
            ComputerTestHelper.compute("g.V().local(__.inE('knows').limit(2).outV).name", g);
        }
    }

}

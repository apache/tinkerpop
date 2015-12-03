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
package org.apache.tinkerpop.gremlin.process.traversal.step.map

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalScriptHelper
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyAddEdgeTest {

    public static class Traversals extends AddEdgeTest {

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_asXaX_outXcreatedX_addEXcreatedByX_toXaX(final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).as('a').out('created').addE('createdBy').to('a')", g, "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_asXaX_outXcreatedX_addEXcreatedByX_toXaX_propertyXweight_2X(
                final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).as('a').out('created').addE('createdBy').to('a').property('weight', 2.0d)", g, "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_aggregateXxX_asXaX_selectXxX_unfold_addEXexistsWithX_toXaX_propertyXtime_nowX() {
            TraversalScriptHelper.compute("g.V.aggregate('x').as('a').select('x').unfold.addE('existsWith').to('a').property('time', 'now')", g)
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_asXbX_addEXcodeveloperX_fromXaX_toXbX_propertyXyear_2009X() {
            TraversalScriptHelper.compute("g.V.as('a').out('created').in('created').where(neq('a')).as('b').addE('co-developer').from('a').to('b').property('year', 2009)", g)
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_asXaX_inXcreatedX_addEXcreatedByX_fromXaX_propertyXyear_2009X_propertyXacl_publicX() {
            TraversalScriptHelper.compute("g.V.as('a').in('created').addE('createdBy').from('a').property('year', 2009).property('acl', 'public')", g);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_aX(final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).as('a').out('created').addOutE('createdBy', 'a')", g, "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_a_weight_2X(
                final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).as('a').out('created').addOutE('createdBy', 'a', 'weight', 2.0d)", g, "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Edge> get_g_withSideEffectXx__g_V_toListX_addOutEXexistsWith_x_time_nowX() {
            TraversalScriptHelper.compute("g.withSideEffect('x',g.V.toList()).V.addOutE('existsWith', 'x', 'time', 'now')", g)
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_asXbX_selectXa_bX_addInEXa_codeveloper_b_year_2009X() {
            TraversalScriptHelper.compute("g.V.as('a').out('created').in('created').where(neq('a')).as('b').select('a','b').addInE('a', 'co-developer', 'b', 'year', 2009)", g)
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_asXaX_inXcreatedX_addInEXcreatedBy_a_year_2009_acl_publicX() {
            TraversalScriptHelper.compute("g.V.as('a').in('created').addInE('createdBy', 'a', 'year', 2009, 'acl', 'public')", g)
        }
    }
}

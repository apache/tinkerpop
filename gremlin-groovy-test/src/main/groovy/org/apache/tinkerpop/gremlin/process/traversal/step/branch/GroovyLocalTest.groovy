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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyLocalTest {

    public static class Traversals extends LocalTest {

        @Override
        public Traversal<Vertex, String> get_g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.local(__.properties('location').order.by(value,incr).limit(2)).value");
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXlabel_personX_asXaX_localXoutXcreatedX_asXbXX_selectXa_bX_byXnameX_byXidX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has(T.label, 'person').as('a').local(__.out('created').as('b')).select('a', 'b').by('name').by(T.id)");
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_localXoutE_countX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.local(__.outE.count())");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_localXoutEXknowsX_limitX1XX_inV_name(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).local(__.outE('knows').limit(1)).inV.name", "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_localXbothEXcreatedX_limitX1XX_otherV_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().local(__.bothE('created').limit(1)).otherV.name");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX4X_localXbothEX1_createdX_limitX1XX(final Object v4Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v4Id).local(__.bothE('created').limit(1))", "v4Id", v4Id);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX4X_localXbothEXknows_createdX_limitX1XX(final Object v4Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v4Id).local(__.bothE('knows', 'created').limit(1))", "v4Id", v4Id);
        }

        @Override
        public Traversal<Vertex, String> get_g_VX4X_localXbothE_limitX1XX_otherV_name(final Object v4Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v4Id).local(__.bothE.limit(1)).otherV.name", "v4Id", v4Id);
        }

        @Override
        public Traversal<Vertex, String> get_g_VX4X_localXbothE_limitX2XX_otherV_name(final Object v4Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v4Id).local(__.bothE.limit(2)).otherV.name", "v4Id", v4Id);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_localXinEXknowsX_limitX2XX_outV_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().local(__.inE('knows').limit(2).outV).name");
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_localXmatchXproject__created_person__person_name_nameX_selectXname_projectX_by_byXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """g.V.local(match(
                    __.as('project').in('created').as('person'),
                    __.as('person').values('name').as('name'))).
                     select('name', 'project').by.by('name')""")
        }
    }
}

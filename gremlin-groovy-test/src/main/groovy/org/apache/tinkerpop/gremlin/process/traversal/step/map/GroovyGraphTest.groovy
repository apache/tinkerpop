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
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyGraphTest {

    public static class Traversals extends GraphTest {

        @Override
        public Traversal<Vertex, String> get_g_VX1X_V_valuesXnameX(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).V.name", "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_outXknowsX_V_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out('knows').V.name")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXname_GarciaX_inXsungByX_asXsongX_V_hasXname_Willie_DixonX_inXwrittenByX_whereXeqXsongXX_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('name','Garcia').in('sungBy').as('song').V.has('name','Willie_Dixon').in('writtenBy').where(eq('song')).name")
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_hasLabelXpersonX_asXpX_VXsoftwareX_addInEXuses_pX() {
            def software = g.V().hasLabel("software").toList()
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().hasLabel('person').as('p').V(software).addE('uses').from('p')", "software", software)
        }
    }
}

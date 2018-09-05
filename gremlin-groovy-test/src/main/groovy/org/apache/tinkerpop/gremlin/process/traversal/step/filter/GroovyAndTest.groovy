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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyAndTest {

    public static class Traversals extends AndTest {

        @Override
        public Traversal<Vertex, String> get_g_V_andXhasXage_gt_27X__outE_count_gte_2X_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.and(has('age',gt(27)), outE().count.is(gte(2l))).name")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_andXoutE__hasXlabel_personX_and_hasXage_gte_32XX_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.and(outE(), has(label, 'person') & has('age',gte(32))).name")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXaX_outXknowsX_and_outXcreatedX_inXcreatedX_asXaX_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.as('a').out('knows') & out('created').in('created').as('a').name")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXaX_andXselectXaX_selectXaXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().as('a').and(__.select('a'), __.select('a'))");
        }

        @Override
        Traversal<Vertex, Vertex> get_g_V_hasXname_markoX_and_hasXname_markoX_and_hasXname_markoX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().has('name', 'marko').and().has('name', 'marko').and().has('name', 'marko')")
        }
    }
}

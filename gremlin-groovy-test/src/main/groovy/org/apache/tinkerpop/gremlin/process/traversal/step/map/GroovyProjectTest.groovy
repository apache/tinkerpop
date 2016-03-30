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
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyProjectTest {

    public static class Traversals extends ProjectTest {
        @Override
        public Traversal<Vertex, Map<String, Number>> get_g_V_hasLabelXpersonX_projectXa_bX_byXoutE_countX_byXageX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.hasLabel('person').project('a','b').by(outE().count).by('age')")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_outXcreatedX_projectXa_bX_byXnameX_byXinXcreatedX_countX_order_byXselectXbX__decrX_selectXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out('created').project('a', 'b').by('name').by(__.in('created').count).order.by(select('b'),decr).select('a')")
        }
    }
}

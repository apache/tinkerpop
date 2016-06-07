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
public abstract class GroovyUnfoldTest {

    public static class Traversals extends UnfoldTest {

        @Override
        public Traversal<Vertex, Edge> get_g_V_localXoutE_foldX_unfold() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.local(__.outE.fold).unfold")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_valueMap_unfold_mapXkeyX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.valueMap.unfold.map { it.key }")
        }

        @Override
        Traversal<Vertex, String> get_g_VX1X_repeatXboth_simplePathX_untilXhasIdX6XX_path_byXnameX_unfold(Object v1Id, Object v6Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).repeat(__.both.simplePath).until(hasId(v6Id)).path.by('name').unfold", "v1Id", v1Id, "v6Id", v6Id)
        }
    }
}

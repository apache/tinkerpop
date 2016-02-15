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

import org.apache.tinkerpop.gremlin.process.traversal.Path
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public abstract class GroovyLoopsTest {

    public static class Traversals extends LoopsTest {

        @Override
        Traversal<Vertex, Path> get_g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_or_loops_isX3XX_hasXname_peterX_path_byXnameX(
                final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).repeat(__.both.simplePath).until(has('name', 'peter').or.loops.is(3)).has('name', 'peter').path.by('name')", "v1Id", v1Id)
        }

        @Override
        Traversal<Vertex, Path> get_g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_or_loops_isX2XX_hasXname_peterX_path_byXnameX(
                final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).repeat(__.both.simplePath).until(has('name', 'peter').or.loops.is(2)).has('name', 'peter').path.by('name')", "v1Id", v1Id)
        }

        @Override
        Traversal<Vertex, Path> get_g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_and_loops_isX3XX_hasXname_peterX_path_byXnameX(
                final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).repeat(__.both.simplePath).until(has('name', 'peter').and.loops.is(3)).has('name', 'peter').path.by('name')", "v1Id", v1Id)
        }

        @Override
        Traversal<Vertex, String> get_g_V_emitXhasXname_markoX_or_loops_isX2XX_repeatXoutX_valuesXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.emit(has('name', 'marko').or.loops.is(2)).repeat(__.out).name")
        }
    }
}

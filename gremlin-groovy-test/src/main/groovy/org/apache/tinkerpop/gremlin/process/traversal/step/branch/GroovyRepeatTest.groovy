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

import org.apache.tinkerpop.gremlin.process.traversal.Path
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Vertex

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyRepeatTest {

    public static class Traversals extends RepeatTest {

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_timesX2X_emit_path() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.repeat(__.out).times(2).emit.path")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_repeatXoutX_timesX2X_repeatXinX_timesX2X_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.repeat(__.out).times(2).repeat(__.in).times(2).name")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_timesX2X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.repeat(__.out).times(2)")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_timesX2X_emit() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.repeat(__.out).times(2).emit")
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_timesX2X_repeatXoutX_name(Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).times(2).repeat(__.out).name", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_emit_repeatXoutX_timesX2X_path() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.emit.repeat(__.out).times(2).path")
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_emit_timesX2X_repeatXoutX_path() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.emit.times(2).repeat(__.out).path")
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_emitXhasXlabel_personXX_repeatXoutX_name(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).emit(has(T.label, 'person')).repeat(__.out).name", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXgroupCountXmX_byXnameX_outX_timesX2X_capXmX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.repeat(groupCount('m').by('name').out).times(2).cap('m')")
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_repeatXbothX_timesX10X_asXaX_out_asXbX_selectXa_bX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.repeat(both()).times(10).as('a').out().as('b').select('a', 'b')");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_repeatXoutX_untilXoutE_count_isX0XX_name(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).repeat(out()).until(__.outE.count.is(0)).name", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Map<Integer, Long>> get_g_VX1X_repeatXgroupCountXmX_byXloopsX_outX_timesX3X_capXmX(
                final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).repeat(groupCount('m').by(loops()).out()).times(3).cap('m')", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXbothX_untilXname_eq_marko_or_loops_gt_1X_groupCount_byXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.repeat(both()).until{it.get().value('name').equals('lop') || it.loops() > 1}.groupCount.by('name')")
        }
    }
}

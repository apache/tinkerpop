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
public abstract class GroovyCountTest {

    public static class Traversals extends CountTest {
        @Override
        public Traversal<Vertex, Long> get_g_V_count() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.count()")
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_out_count() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out.count")
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_both_both_count() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.both.both.count()")
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX3X_count() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().repeat(__.out).times(3).count()")
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX8X_count() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.repeat(__.out).times(8).count()")
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX5X_asXaX_outXwrittenByX_asXbX_selectXa_bX_count() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.repeat(out()).times(5).as('a').out('writtenBy').as('b').select('a', 'b').count()")
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_hasXnoX_count() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('no').count")
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_fold_countXlocalX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.fold.count(local)")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_whereXinXknowsX_outXcreatedX_count_is_0XX_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.where(__.in('knows').out('created').count.is(0)).name")
        }
    }
}

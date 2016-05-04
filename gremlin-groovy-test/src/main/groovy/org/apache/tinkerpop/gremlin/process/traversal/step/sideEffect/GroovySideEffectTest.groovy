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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovySideEffectTest {

    public static class Traversals extends SideEffectTest {

        @Override
        public Traversal<Vertex, String> get_g_VX1X_sideEffectXstore_aX_name(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", """g.withSideEffect('a') { [] }.V(v1Id).sideEffect {
                it.sideEffects('a').clear();
                it.sideEffects('a').add(it.get());
            }.name""", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_out_sideEffectXincr_cX_name(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", """g.withSideEffect('c') { [0] }.V(v1Id).out.sideEffect {
                temp = it.sideEffects('c')[0];
                it.sideEffects('c').clear();
                it.sideEffects('c').add(temp + 1);
            }.name""", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_out_sideEffectXX_name(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).out().sideEffect {}.name", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_withSideEffectXa__linkedhashmapX_V_out_groupCountXaX_byXlabelX_out_out_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.withSideEffect('a'){[:] as LinkedHashMap}.V.out.groupCount('a').by(label).out.out.cap('a')");
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_withSideEffectsXa__linkedhashmapX_withSideEffectXb__arraylist__addAllX_withSideEffectXc__arrayList__addAllX_V_groupXaX_byXlabelX_byXcountX_sideEffectXb__1_2_3X_out_out_out_sideEffectXc__bob_danielX_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                   g.withSideEffect('a'){[:] as LinkedHashMap}
                    .withSideEffect('b',{[:] as ArrayList},addAll)
                    .withSideEffect('c',{[:] as ArrayList},addAll)
                    .V.group('a').by(label).by(count())
                    .sideEffect{it.sideEffects('b', [1,2,3] as LinkedList)}
                    .out.out.out
                    .sideEffect{it.sideEffect('c',['bob','daniel'] as LinkedList)}
                    .cap('a');
                """);
        }

        @Override
        public Traversal<Vertex, Integer> get_g_withSideEffectXa_0_sumX_V_out_sideEffectXsideEffectsXa_bulkXX_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.withSideEffect('a', 0, sum).V.out.sideEffect{it.sideEffects('a', it.bulk() as int)}.cap('a')")
        }

        @Override
        public Traversal<Vertex, Integer> get_g_withSideEffectXa_0X_V_out_sideEffectXsideEffectsXa_1XX_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.withSideEffect('a', 0).V.out.sideEffect{it.sideEffects('a', 1)}.cap('a')")
        }
    }
}

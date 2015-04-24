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
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine
import org.apache.tinkerpop.gremlin.process.UseEngine
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovySideEffectTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends SideEffectTest {

        @Override
        public Traversal<Vertex, String> get_g_VX1X_sideEffectXstore_aX_name(final Object v1Id) {
            g.withSideEffect('a') { [] }.V(v1Id).sideEffect {
                it.sideEffects('a').clear();
                it.sideEffects('a').add(it.get());
            }.name;
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_out_sideEffectXincr_cX_name(final Object v1Id) {
            g.withSideEffect('c') { [0] }.V(v1Id).out.sideEffect {
                def temp = it.sideEffects('c')[0];
                it.sideEffects('c').clear();
                it.sideEffects('c').add(temp + 1);
            }.name
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_out_sideEffectXX_name(final Object v1Id) {
            g.V(v1Id).out().sideEffect {}.name
        }
    }
}

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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect

import org.apache.tinkerpop.gremlin.process.Traversal
import org.apache.tinkerpop.gremlin.process.graph.traversal.__
import org.apache.tinkerpop.gremlin.process.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.StoreTest
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyStoreTest {

    public static class StandardTest extends StoreTest {

        @Override
        public Traversal<Vertex, Collection> get_g_V_storeXaX_byXnameX_out_capXaX() {
            g.V().store('a').by('name').out().cap('a');
        }

        @Override
        public Traversal<Vertex, Collection> get_g_VX1X_storeXaX_byXnameX_out_storeXaX_byXnameX_name_capXaX(
                final Object v1Id) {
            g.V(v1Id).store('a').by('name').out().store('a').by('name').name.cap('a');
        }

        @Override
        public Traversal<Vertex, Set<String>> get_g_V_withSideEffectXa_setX_both_name_storeXaX() {
            g.V.withSideEffect('a') { [] as Set }.both.name.store('a')
        }


        @Override
        public Traversal<Vertex, Collection> get_g_V_storeXaX_byXoutEXcreatedX_countX_out_out_storeXaX_byXinEXcreatedX_weight_sumX() {
            g.V.store('a').by(__.outE('created').count).out.out.store('a').by(__.inE('created').weight.sum);
        }
    }

    public static class ComputerTest extends StoreTest {

        @Override
        public Traversal<Vertex, Collection> get_g_V_storeXaX_byXnameX_out_capXaX() {
            ComputerTestHelper.compute("g.V().store('a').by('name').out().cap('a')", g)
        }

        @Override
        public Traversal<Vertex, Collection> get_g_VX1X_storeXaX_byXnameX_out_storeXaX_byXnameX_name_capXaX(
                final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).store('a').by('name').out().store('a').by('name').name.cap('a')", g)
        }

        @Override
        public Traversal<Vertex, Set<String>> get_g_V_withSideEffectXa_setX_both_name_storeXaX() {
            ComputerTestHelper.compute("g.V.withSideEffect('a'){[] as Set}.both.name.store('a')", g);
        }

        @Override
        public Traversal<Vertex, Collection> get_g_V_storeXaX_byXoutEXcreatedX_countX_out_out_storeXaX_byXinEXcreatedX_weight_sumX() {
            ComputerTestHelper.compute("g.V.store('a').by(__.outE('created').count).out.out.store('a').by(__.inE('created').weight.sum)", g);
        }
    }
}

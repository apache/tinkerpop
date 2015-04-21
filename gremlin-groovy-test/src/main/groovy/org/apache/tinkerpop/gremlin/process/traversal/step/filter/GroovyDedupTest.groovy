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

import org.apache.tinkerpop.gremlin.process.*
import org.apache.tinkerpop.gremlin.process.computer.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.traversal.Scope
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.junit.Test

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.dedup

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public abstract class GroovyDedupTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends DedupTest {
        @Override
        public Traversal<Vertex, String> get_g_V_both_dedup_name() {
            g.V.both.dedup.name
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_hasXlabel_softwareX_dedup_byXlangX_name() {
            g.V.both.has(T.label, 'software').dedup.by('lang').name
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_name_orderXa_bX_dedup() {
            g.V().both().properties('name').order.by { a, b -> a.value() <=> b.value() }.dedup.value
        }

        @Override
        Traversal<Vertex, Map<String, Set<Double>>> get_g_V_group_byXlabelX_byXbothE_valuesXweightX_foldX_byXdedupXlocalXX() {
            g.V().group().by(T.label).by(bothE().values('weight').fold()).by(dedup(Scope.local))
        }
    }

    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class ComputerTraversals extends DedupTest {
        @Override
        @Test
        @org.junit.Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_both_dedup_name() {
        }

        @Override
        @Test
        @org.junit.Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_both_hasXlabel_softwareX_dedup_byXlangX_name() {
        }

        @Override
        @Test
        @org.junit.Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_both_name_orderXa_bX_dedup() {
        }

        @Override
        public Traversal<Vertex, Map<String, Set<Double>>> get_g_V_group_byXlabelX_byXbothE_valuesXweightX_foldX_byXdedupXlocalXX() {
            ComputerTestHelper.compute("g.V().group().by(T.label).by(bothE().values('weight').fold()).by(dedup(Scope.local))", g);
        }

        @Override
        Traversal<Vertex, String> get_g_V_both_dedup_name() {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        Traversal<Vertex, String> get_g_V_both_hasXlabel_softwareX_dedup_byXlangX_name() {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        Traversal<Vertex, String> get_g_V_both_name_orderXa_bX_dedup() {
            // override with nothing until the test itself is supported
            return null
        }
    }
}

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

import org.apache.tinkerpop.gremlin.process.computer.GroovyTestHelper
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public abstract class GroovyDedupTest {

    public static class Traversals extends DedupTest {
        /*@Override
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
        }*/

        @Override
        public Traversal<Vertex, String> get_g_V_both_dedup_name() {
            GroovyTestHelper.compute("g.V.both.dedup.name", g);
        }

        @Override
        public Traversal<Vertex, Map<String, Set<Double>>> get_g_V_group_byXlabelX_byXbothE_valuesXweightX_foldX_byXdedupXlocalXX() {
            GroovyTestHelper.compute("g.V().group().by(T.label).by(bothE().values('weight').fold()).by(dedup(Scope.local))", g);
        }


        @Override
        public Traversal<Vertex, String> get_g_V_both_hasXlabel_softwareX_dedup_byXlangX_name() {
            GroovyTestHelper.compute("g.V.both.has(T.label, 'software').dedup.by('lang').name", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_name_orderXa_bX_dedup() {
            return GroovyTestHelper.compute("g.V().both().properties('name').order.by { a, b -> a.value() <=> b.value() }.dedup.value", g);
        }
    }
}

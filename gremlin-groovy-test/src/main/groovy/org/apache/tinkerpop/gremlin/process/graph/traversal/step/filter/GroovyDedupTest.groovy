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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.filter

import org.apache.tinkerpop.gremlin.process.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.Scope
import org.apache.tinkerpop.gremlin.process.T
import org.apache.tinkerpop.gremlin.process.Traversal
import org.apache.tinkerpop.gremlin.structure.Vertex

import static org.apache.tinkerpop.gremlin.process.graph.traversal.__.bothE
import static org.apache.tinkerpop.gremlin.process.graph.traversal.__.dedup

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public abstract class GroovyDedupTest {

    public static class StandardTest extends DedupTest {
        @Override
        public Traversal<Vertex, String> get_g_V_both_dedup_name() {
            g.V.both.dedup.name
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_hasXlabel_softwareX_dedup_byXlangX_name() {
            g.V.both.has(T.label, 'software').dedup.by('lang').name
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_propertiesXnameX_orderXa_bX_dedup_value() {
            g.V().both().properties('name').order.by { a, b -> a.value() <=> b.value() }.dedup.value
        }

        @Override
        Traversal<Vertex, Map<String, Set<Double>>> get_g_V_group_byXlabelX_byXbothE_valuesXweightX_foldX_byXdedupXlocalXX_cap() {
            g.V().group().by(T.label).by(bothE().values('weight').fold()).by(dedup(Scope.local)).cap()
        }
    }

    public static class ComputerTest extends DedupTest {
        @Override
        public Traversal<Vertex, String> get_g_V_both_dedup_name() {
            g.V.both.dedup.name // TODO
            //ComputerTestHelper.compute("g.V.both.dedup.name", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_hasXlabel_softwareX_dedup_byXlangX_name() {
            g.V.both.has(T.label, 'software').dedup.by('lang').name // TODO
            //ComputerTestHelper.compute("g.V.both.has(T.label,'software').dedup.by('lang').name", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_propertiesXnameX_orderXa_bX_dedup_value() {
            g.V().both().properties('name').order.by { a, b -> a.value() <=> b.value() }.dedup.value // TODO
            //ComputerTestHelper.compute("g.V.both.properties('name').order.by { a, b -> a.value() <=> b.value() }.dedup.value", g);
        }

        @Override
        Traversal<Vertex, Map<String, Set<Double>>> get_g_V_group_byXlabelX_byXbothE_valuesXweightX_foldX_byXdedupXlocalXX_cap() {
            ComputerTestHelper.compute("g.V().group().by(T.label).by(bothE().values('weight').fold()).by(dedup(Scope.local)).cap()", g);
        }
    }
}

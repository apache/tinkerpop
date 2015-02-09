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
package com.tinkerpop.gremlin.process.graph.traversal.step.filter

import com.tinkerpop.gremlin.process.T
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.ComputerTestHelper
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.DedupTest
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (daniel at thinkaurelius.com)
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
    }

    public static class ComputerTestImpl extends DedupTest {
        @Override
        public Traversal<Vertex, String> get_g_V_both_dedup_name() {
            ComputerTestHelper.compute("g.V.both.dedup.name", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_hasXlabel_softwareX_dedup_byXlangX_name() {
            ComputerTestHelper.compute("g.V.both.has(T.label,'software').dedup.by('lang').name", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_propertiesXnameX_orderXa_bX_dedup_value() {
            ComputerTestHelper.compute("g.V.both.properties('name').order.by { a, b -> a.value() <=> b.value() }.dedup.value", g);
        }
    }
}

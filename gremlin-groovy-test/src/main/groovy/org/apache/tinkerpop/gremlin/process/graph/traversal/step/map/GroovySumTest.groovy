/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.map

import org.apache.tinkerpop.gremlin.process.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.Scope
import org.apache.tinkerpop.gremlin.process.Traversal
import org.apache.tinkerpop.gremlin.structure.Vertex

import static org.apache.tinkerpop.gremlin.process.graph.traversal.__.bothE
import static org.apache.tinkerpop.gremlin.process.graph.traversal.__.sum

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public abstract class GroovySumTest {

    public static class StandardTest extends SumTest {

        @Override
        public Traversal<Vertex, Double> get_g_V_valuesXageX_sum() {
            g.V().values('age').sum()
        }

        @Override
        public Traversal<Vertex, Map<String, Number>> get_g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_valuesXweightX_foldX_byXsumXlocalXX() {
            g.V().hasLabel('software').group().by('name').by(bothE().values('weight').fold()).by(sum(Scope.local)).cap()
        }
    }

    public static class ComputerTest extends SumTest {

        @Override
        public Traversal<Vertex, Double> get_g_V_valuesXageX_sum() {
            ComputerTestHelper.compute("g.V.age.mean", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Number>> get_g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_valuesXweightX_foldX_byXsumXlocalXX() {
            ComputerTestHelper.compute("g.V().hasLabel('software').group().by('name').by(bothE().values('weight').fold()).by(sum(Scope.local)).cap()", g)
        }
    }
}
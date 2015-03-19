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

import org.apache.tinkerpop.gremlin.process.computer.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.traversal.Scope
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine
import org.apache.tinkerpop.gremlin.process.UseEngine
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
import org.apache.tinkerpop.gremlin.structure.Vertex

import static __.bothE
import static __.max

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyMaxTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends MaxTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_max() {
            g.V.age.max
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_repeatXbothX_timesX5X_age_max() {
            g.V.repeat(__.both).times(5).age.max
        }

        @Override
        public Traversal<Vertex, Map<String, Number>> get_g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_valuesXweightX_foldX_byXmaxXlocalXX() {
            g.V().hasLabel('software').group().by('name').by(bothE().values('weight').fold()).by(max(Scope.local))
        }
    }

    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class ComputerTraversals extends MaxTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_max() {
            ComputerTestHelper.compute("g.V.age.max", g)
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_repeatXbothX_timesX5X_age_max() {
            ComputerTestHelper.compute("g.V.repeat(__.both).times(5).age.max", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Number>> get_g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_valuesXweightX_foldX_byXmaxXlocalXX() {
            ComputerTestHelper.compute("g.V().hasLabel('software').group().by('name').by(bothE().values('weight').fold()).by(max(Scope.local))", g)
        }
    }
}

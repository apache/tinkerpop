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
package com.apache.tinkerpop.gremlin.process.graph.traversal.step.branch

import com.apache.tinkerpop.gremlin.process.Traversal
import com.apache.tinkerpop.gremlin.process.ComputerTestHelper
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.branch.BranchTest
import com.apache.tinkerpop.gremlin.structure.Vertex

import com.apache.tinkerpop.gremlin.process.graph.traversal.__
/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyBranchTest {

    public static class StandardTest extends BranchTest {

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabel_eq_person__a_bX_optionXa__ageX_optionXb__langX_optionXb__nameX() {
            g.V.branch { it.label() == 'person' ? 'a' : 'b' }
                    .option('a', __.age)
                    .option('b', __.lang)
                    .option('b', __.name)
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabelX_optionXperson__ageX_optionXsoftware__langX_optionXsoftware__nameX() {
            g.V.branch(__.label)
                    .option('person', __.age)
                    .option('software', __.lang)
                    .option('software', __.name);
        }
    }

    public static class ComputerTest extends BranchTest {

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabel_eq_person__a_bX_optionXa__ageX_optionXb__langX_optionXb__nameX() {
            ComputerTestHelper.compute("g.V.branch { it.label() == 'person' ? 'a' : 'b' }.option('a', __.age).option('b', __.lang).option('b',__.name)", g);
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabelX_optionXperson__ageX_optionXsoftware__langX_optionXsoftware__nameX() {
            ComputerTestHelper.compute("""
            g.V.branch(__.label)
                    .option('person', __.age)
                    .option('software', __.lang)
                    .option('software', __.name)
            """, g)
        }
    }
}

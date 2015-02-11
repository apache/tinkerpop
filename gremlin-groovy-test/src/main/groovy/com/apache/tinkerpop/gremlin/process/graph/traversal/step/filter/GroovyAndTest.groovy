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
package com.apache.tinkerpop.gremlin.process.graph.traversal.step.filter

import com.apache.tinkerpop.gremlin.process.ComputerTestHelper
import com.apache.tinkerpop.gremlin.process.T
import com.apache.tinkerpop.gremlin.process.Traversal
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.filter.AndTest
import com.apache.tinkerpop.gremlin.structure.Vertex

import static com.apache.tinkerpop.gremlin.process.graph.traversal.__.has
import static com.apache.tinkerpop.gremlin.process.graph.traversal.__.outE
import static com.apache.tinkerpop.gremlin.structure.Compare.gt
import static com.apache.tinkerpop.gremlin.structure.Compare.gte

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyAndTest {

    public static class StandardTest extends AndTest {

        @Override
        public Traversal<Vertex, String> get_g_V_andXhasXage_gt_27X__outE_count_gt_2X_name() {
            g.V.and(has('age', gt, 27), outE().count.is(gte, 2l)).name
        }

        @Override
        public Traversal<Vertex, String> get_g_V_andXoutE__hasXlabel_personX_and_hasXage_gte_32XX_name() {
            g.V.and(outE(), has(T.label, 'person') & has('age', gte, 32)).name
        }
    }

    public static class ComputerTest extends AndTest {

        @Override
        public Traversal<Vertex, String> get_g_V_andXhasXage_gt_27X__outE_count_gt_2X_name() {
            ComputerTestHelper.compute("g.V.and(has('age', gt, 27), outE().count.is(gte, 2l)).name", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_andXoutE__hasXlabel_personX_and_hasXage_gte_32XX_name() {
            ComputerTestHelper.compute("g.V.and(outE(), has(T.label, 'person') & has('age', gte, 32)).name", g)
        }
    }
}

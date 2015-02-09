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
package com.tinkerpop.gremlin.process.graph.traversal.step.branch

import com.tinkerpop.gremlin.process.T
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.traversal.__
import com.tinkerpop.gremlin.process.ComputerTestHelper
import com.tinkerpop.gremlin.process.graph.traversal.step.branch.ChooseTest
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class GroovyChooseTest {

    public static class StandardTest extends ChooseTest {

        @Override
        public Traversal<Vertex, String> get_g_V_chooseXname_length_5XoutXinX_name() {
            g.V.choose({ it.name.length() == 5 },
                    __.out,
                    __.in).name;
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_chooseX0X_optionX0__outX_name(Object v1Id) {
            g.V(v1Id).choose { 0 }.option(0, __.out.name)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXlabel_personX_chooseXname_lengthX_optionX5__inX_optionX4__outX_optionX3__bothX_name() {
            g.V.has(T.label, 'person').choose {
                it.name.length()
            }.option(5, __.in).option(4, __.out).option(3, __.both).name
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_chooseXout_countX_optionX2L__nameX_optionX3L__valueMapX() {
            g.V.choose(__.out.count).option(2L, __.values('name')).option(3L, __.valueMap())
        }
    }

    public static class ComputerTest extends ChooseTest {

        @Override
        public Traversal<Vertex, String> get_g_V_chooseXname_length_5XoutXinX_name() {
            ComputerTestHelper.compute("""g.V.choose({ it.name.length() == 5 },
                    __.out(),
                    __.in).name""", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_chooseX0X_optionX0__outX_name(Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).choose { 0 }.option(0, __.out.name)", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXlabel_personX_chooseXname_lengthX_optionX5__inX_optionX4__outX_optionX3__bothX_name() {
            ComputerTestHelper.compute("g.V.has(T.label,'person').choose { it.name.length() }.option(5, __.in).option(4, __.out).option(3, __.both).name", g);
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_chooseXout_countX_optionX2L__nameX_optionX3L__valueMapX() {
            ComputerTestHelper.compute("g.V.choose(__.out.count).option(2L, __.values('name')).option(3L, __.valueMap())", g);
        }
    }
}

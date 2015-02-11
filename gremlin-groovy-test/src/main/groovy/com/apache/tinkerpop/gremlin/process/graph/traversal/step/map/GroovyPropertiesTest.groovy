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
package com.apache.tinkerpop.gremlin.process.graph.traversal.step.map

import com.apache.tinkerpop.gremlin.process.Traversal
import com.apache.tinkerpop.gremlin.process.ComputerTestHelper
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.map.PropertiesTest
import com.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyPropertiesTest {

    public static class StandardTest extends PropertiesTest {

        @Override
        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXname_ageX_value() {
            g.V.has('age').properties('name', 'age').value;
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXage_nameX_value() {
            g.V.has('age').properties('age', 'name').value;
        }
    }

    public static class ComputerTest extends PropertiesTest {

        @Override
        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXname_ageX_value() {
            ComputerTestHelper.compute("g.V.has('age').properties('name', 'age').value", g);
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXage_nameX_value() {
            ComputerTestHelper.compute("g.V.has('age').properties('age', 'name').value", g);
        }
    }

}

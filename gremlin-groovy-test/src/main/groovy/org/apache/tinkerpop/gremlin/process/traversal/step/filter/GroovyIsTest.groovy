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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.computer.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine
import org.apache.tinkerpop.gremlin.process.UseEngine
import static org.apache.tinkerpop.gremlin.structure.P.*;
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public abstract class GroovyIsTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends IsTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_valuesXageX_isX32X() {
            return g.V().values('age').is(32);
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_valuesXageX_isXlte_30X() {
            return g.V().values('age').is(lte(30));
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_valuesXageX_isXgte_29X_isXlt_34X() {
            return g.V().values('age').is(gte(29)).is(lt(34));
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXinXcreatedX_count_isX1XX_valuesXnameX() {
            return g.V().has(__.in('created').count().is(1l)).values('name');
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXinXcreatedX_count_isXgte_2XX_valuesXnameX() {
            return g.V().has(__.in('created').count().is(gte(2l))).values('name');
        }
    }

    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class ComputerTraversals extends IsTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_valuesXageX_isX32X() {
            ComputerTestHelper.compute("g.V().values('age').is(32)", g)
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_valuesXageX_isXlte_30X() {
            ComputerTestHelper.compute("g.V().values('age').is(lte(30))", g)
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_valuesXageX_isXgte_29X_isXlt_34X() {
            ComputerTestHelper.compute("g.V().values('age').is(gte(29)).is(lt(34))", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXinXcreatedX_count_isX1XX_valuesXnameX() {
            ComputerTestHelper.compute("g.V().has(__.in('created').count().is(1l)).values('name')", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXinXcreatedX_count_isXgte_2XX_valuesXnameX() {
            ComputerTestHelper.compute("g.V().has(__.in('created').count().is(gte(2l))).values('name')", g)
        }
    }
}

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
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public abstract class GroovyIsTest {

    public static class Traversals extends IsTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_valuesXageX_isX32X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.age.is(32)")
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_valuesXageX_isXlte_30X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.age.is(lte(30))")
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_valuesXageX_isXgte_29X_isXlt_34X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.age.is(gte(29)).is(lt(34))")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_whereXinXcreatedX_count_isX1XX_valuesXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.where(__.in('created').count.is(1)).name")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_whereXinXcreatedX_count_isXgte_2XX_valuesXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.where(__.in('created').count.is(gte(2l))).name")
        }
    }
}

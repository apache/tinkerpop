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

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.ComputerTestHelper
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.HasNotTest
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class GroovyHasNotTest {

    public static class StandardTest extends HasNotTest {
        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasNotXprop(final Object v1Id, final String propertyKey) {
            g.V(v1Id).hasNot(propertyKey)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasNotXprop(final String propertyKey) {
            g.V.hasNot(propertyKey)
        }
    }

    public static class ComputerTest extends HasNotTest {
        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasNotXprop(final Object v1Id, final String propertyKey) {
            ComputerTestHelper.compute("g.V(${v1Id}).hasNot('${propertyKey}')", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasNotXprop(final String propertyKey) {
            ComputerTestHelper.compute("g.V.hasNot('${propertyKey}')", g);
        }
    }
}

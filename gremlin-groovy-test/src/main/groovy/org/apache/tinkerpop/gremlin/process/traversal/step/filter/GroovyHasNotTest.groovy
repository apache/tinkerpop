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

import org.apache.tinkerpop.gremlin.process.UseEngine
import org.apache.tinkerpop.gremlin.process.computer.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class GroovyHasNotTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends HasNotTest {
        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasNotXprop(final Object v1Id, final String propertyKey) {
            g.V(v1Id).hasNot(propertyKey)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasNotXprop(final String propertyKey) {
            g.V.hasNot(propertyKey)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasNotXoutXcreatedXX() {
            return g.V().hasNot(__.out('created')).values('name');
        }
    }

    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class ComputerTraversals extends HasNotTest {
        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasNotXprop(final Object v1Id, final String propertyKey) {
            ComputerTestHelper.compute("g.V(v1Id).hasNot('${propertyKey}')", g, "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasNotXprop(final String propertyKey) {
            ComputerTestHelper.compute("g.V.hasNot('${propertyKey}')", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasNotXoutXcreatedXX() {
            ComputerTestHelper.compute("return g.V().hasNot(__.out('created')).values('name')", g);
        }
    }
}

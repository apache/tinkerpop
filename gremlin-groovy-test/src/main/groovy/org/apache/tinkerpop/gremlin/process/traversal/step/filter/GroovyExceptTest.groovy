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
import org.apache.tinkerpop.gremlin.process.traversal.Path
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public abstract class GroovyExceptTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends ExceptTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_exceptXg_v2X(final Object v1Id, final Object v2Id) {
            g.V(v1Id).out.except(g.V(v2Id).next())
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_aggregateXxX_out_exceptXxX(final Object v1Id) {
            g.V(v1Id).out.aggregate('x').out.except('x')
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_outXcreatedX_inXcreatedX_exceptXg_v1X_name(final Object v1Id) {
            g.V(v1Id).out('created').in('created').except(g.V(v1Id).next()).values('name')
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_exceptXg_V_toListX() {
            g.V.out.except(g.V.toList())
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_exceptXX() {
            g.V.out.except([])
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_repeatXbothEXcreatedX_exceptXeX_aggregateXeX_otherVX_emit_path(
                final Object v1Id) {
            g.V(v1Id).repeat(__.bothE('created').except('e').aggregate('e').otherV).emit.path
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_repeatXbothEXcreatedX_dedup_otherVX_emit_path(
                final Object v1Id) {
            g.V(v1Id).repeat(__.bothE('created').dedup.otherV).emit.path
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_exceptXaX_name(final Object v1Id) {
            g.V(v1Id).as('a').out('created').in('created').except('a').name
        }
    }

    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class ComputerTraversals extends ExceptTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_exceptXg_v2X(final Object v1Id, final Object v2Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out.except(g.V(${v2Id}).next())", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_aggregateXxX_out_exceptXxX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out.aggregate('x').out.except('x')", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_outXcreatedX_inXcreatedX_exceptXg_v1X_name(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out('created').in('created').except(g.V(${v1Id}).next()).values('name')", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_exceptXg_V_toListX() {
            ComputerTestHelper.compute("g.V.out.except(g.V.toList())", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_exceptXX() {
            ComputerTestHelper.compute("g.V.out.except([])", g);
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_repeatXbothEXcreatedX_exceptXeX_aggregateXeX_otherVX_emit_path(
                final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).repeat(__.bothE('created').except('e').aggregate('e').otherV).emit.path", g);
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_repeatXbothEXcreatedX_dedup_otherVX_emit_path(
                final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).repeat(__.bothE('created').dedup.otherV).emit.path", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_exceptXaX_name(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).as('a').out('created').in('created').except('a').name", g);
        }
    }
}

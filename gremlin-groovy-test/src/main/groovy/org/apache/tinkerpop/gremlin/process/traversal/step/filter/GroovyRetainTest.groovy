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
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyRetainTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends RetainTest {
        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_retainXg_v2X(final Object v1Id, final Object v2Id) {
            g.V(v1Id).out.retain(g.V(v2Id).next())
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_aggregateXxX_out_retainXxX(final Object v1Id) {
            g.V(v1Id).out.aggregate('x').out.retain('x')
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_retainXaX_name(final Object v1Id) {
            g.V(v1Id).as('a').out('created').in('created').retain('a').name
        }
    }

    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class ComputerTraversals extends RetainTest {
        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_retainXg_v2X(final Object v1Id, final Object v2Id) {
            ComputerTestHelper.compute("g.V(v1Id).out.retain(g.V(v2Id).next())", g, "v1Id", v1Id, "v2Id", v2Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_aggregateXxX_out_retainXxX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(v1Id).out.aggregate('x').out.retain('x')", g, "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_retainXaX_name(final Object v1Id) {
            ComputerTestHelper.compute("g.V(v1Id).as('a').out('created').in('created').retain('a').name", g, "v1Id", v1Id);
        }
    }
}

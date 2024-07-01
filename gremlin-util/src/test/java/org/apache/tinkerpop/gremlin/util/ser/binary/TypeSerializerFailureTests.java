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

package org.apache.tinkerpop.gremlin.util.ser.binary;

import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyPath;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferencePath;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class TypeSerializerFailureTests {

    private final GraphBinaryWriter writer = new GraphBinaryWriter();
    private final UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    private static final NettyBufferFactory bufferFactory = new NettyBufferFactory();

    @Parameterized.Parameters(name = "Value={0}")
    public static Collection input() {
        final ReferenceVertex vertex = new ReferenceVertex("a vertex", null);

        final BulkSet<Object> bulkSet = new BulkSet<>();
        bulkSet.add(vertex, 1L);

        final MutableMetrics metrics = new MutableMetrics("a metric", null);

        final Tree<Vertex> tree = new Tree<>();
        tree.put(vertex, null);

        // Provide instances that are malformed for serialization to fail
        return Arrays.asList(
                vertex,
                bulkSet,
                Collections.singletonList(vertex),
                new ReferenceEdge("an edge", null, vertex, vertex),
                Lambda.supplier(null),
                metrics,
                new DefaultTraversalMetrics(1L, Collections.singletonList(metrics)),
                new DefaultRemoteTraverser<>(new Object(), 1L),
                tree,
                new ReferenceVertexProperty<>("a prop", null, "value"),
                new InvalidPath()
        );
    }

    @Parameterized.Parameter(value = 0)
    public Object value;

    @Test
    public void shouldReleaseMemoryWhenFails() {
        final Buffer buffer = bufferFactory.create(allocator.buffer());
        try {
            writer.write(value, buffer);
            fail("Should throw exception");
        } catch (IOException | RuntimeException e) {
            // We are the owner of the buffer, we should release it
            buffer.release();
        }

        // Make sure all allocations where released
        assertEquals(0, allocator.metric().usedHeapMemory());
    }

    public static class InvalidPath extends ReferencePath {
        public InvalidPath() {
            super(EmptyPath.instance());
        }

        @Override
        public List<Object> objects() {
            return Collections.singletonList(new Object());
        }
    }
}

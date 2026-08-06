/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.util.ser.binary.types;

import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.TraversalStrategySerializer;
import org.apache.tinkerpop.gremlin.util.TestTraversalStrategies.DummyTraversalStrategy;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TraversalStrategySerializerTest {
    private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private final NettyBufferFactory bufferFactory = new NettyBufferFactory();

    @Test
    public void shouldRejectUnregisteredTraversalStrategy() throws Exception {
        final GraphBinaryWriter writer = new GraphBinaryWriter();
        final GraphBinaryReader reader = new GraphBinaryReader();
        final Buffer buffer = bufferFactory.create(allocator.buffer());
        buffer.writeByte(DataType.TRAVERSALSTRATEGY.getCodeByte());
        buffer.writeByte(0);
        writer.writeValue("java.lang.Runtime", buffer, false);
        writer.writeValue(Collections.emptyMap(), buffer, false);
        buffer.readerIndex(0);

        try {
            reader.read(buffer);
            fail("Should have rejected an unregistered traversal strategy");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(IllegalArgumentException.class));
            assertEquals("TraversalStrategy class is not allowed: java.lang.Runtime", ex.getMessage());
        }
    }

    @Test
    public void shouldReadWriteRegisteredTraversalStrategy() throws Exception {
        final TypeSerializerRegistry registry = TypeSerializerRegistry.build().
                addAllowedTraversalStrategy(DummyTraversalStrategy.class).create();
        assertReadWriteRegisteredTraversalStrategy(registry);
    }

    @Test
    public void shouldMergeAllowedTraversalStrategyWithRegisteredSerializer() throws Exception {
        final TypeSerializerRegistry registry = TypeSerializerRegistry.build().
                add(TraversalStrategy.class, new TraversalStrategySerializer()).
                addAllowedTraversalStrategy(DummyTraversalStrategy.class).create();
        assertReadWriteRegisteredTraversalStrategy(registry);
    }

    private void assertReadWriteRegisteredTraversalStrategy(final TypeSerializerRegistry registry) throws Exception {
        final GraphBinaryWriter writer = new GraphBinaryWriter(registry);
        final GraphBinaryReader reader = new GraphBinaryReader(registry);
        final Buffer buffer = bufferFactory.create(allocator.buffer());

        writer.write(DummyTraversalStrategy.instance(), buffer);
        buffer.readerIndex(0);
        final TraversalStrategyProxy strategy = reader.read(buffer);

        assertEquals(DummyTraversalStrategy.class, strategy.getStrategyClass());
    }
}

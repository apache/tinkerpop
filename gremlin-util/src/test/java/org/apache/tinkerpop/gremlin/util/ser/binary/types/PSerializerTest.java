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
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.PSerializer;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * {@code PSerializer} is not currently wired into {@link TypeSerializerRegistry}'s default entries on master, so
 * it is exercised directly here rather than through {@link GraphBinaryReader#read}.
 */
public class PSerializerTest {

    private static final NettyBufferFactory bufferFactory = new NettyBufferFactory();

    private final PSerializer<P> serializer = new PSerializer<>(DataType.BIGDECIMAL /* placeholder, unused here */, P.class);
    private final GraphBinaryReader reader = new GraphBinaryReader();
    private final GraphBinaryWriter writer = new GraphBinaryWriter();

    @Test
    public void shouldRejectOversizedArgumentCount() {
        final Buffer buffer = bufferFactory.create(ByteBufAllocator.DEFAULT.buffer());
        try {
            writer.writeValue("eq", buffer, false);
            buffer.writeInt(Integer.MAX_VALUE);
            serializer.readValue(buffer, reader, false);
            fail("read of a P value with an oversized argument count must be refused");
        } catch (IOException expected) {
        } finally {
            buffer.release();
        }
    }

    @Test
    public void shouldRejectNegativeArgumentCount() {
        final Buffer buffer = bufferFactory.create(ByteBufAllocator.DEFAULT.buffer());
        try {
            writer.writeValue("eq", buffer, false);
            buffer.writeInt(-1);
            serializer.readValue(buffer, reader, false);
            fail("read of a P value with a negative argument count must be refused");
        } catch (IOException expected) {
        } finally {
            buffer.release();
        }
    }

    @Test
    public void shouldReadValidPredicate() throws IOException {
        final Buffer buffer = bufferFactory.create(ByteBufAllocator.DEFAULT.buffer());
        try {
            writer.writeValue("eq", buffer, false);
            writer.writeValue(1, buffer, false);
            writer.write(10, buffer);

            final P<?> result = serializer.readValue(buffer, reader, false);
            assertEquals(P.eq(10), result);
        } finally {
            buffer.release();
        }
    }
}

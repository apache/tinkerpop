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
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.pdt.PrimitiveProviderDefinedType;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PrimitiveProviderDefinedTypeSerializerTest {

    private static final GraphBinaryReader reader = new GraphBinaryReader();
    private static final GraphBinaryWriter writer = new GraphBinaryWriter();
    private static final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private static final NettyBufferFactory bufferFactory = new NettyBufferFactory();

    private Buffer writeAndRead(final Object value) throws IOException {
        final Buffer buffer = bufferFactory.create(allocator.buffer());
        writer.write(value, buffer);
        buffer.readerIndex(0);
        return buffer;
    }

    @Test
    public void shouldRoundTripSimplePrimitivePdt() throws IOException {
        final PrimitiveProviderDefinedType pdt = new PrimitiveProviderDefinedType("Uint32", "42");

        final Buffer buffer = writeAndRead(pdt);
        final PrimitiveProviderDefinedType result = reader.read(buffer);

        assertEquals(pdt, result);
    }

    @Test
    public void shouldRoundTripEmptyValue() throws IOException {
        final PrimitiveProviderDefinedType pdt = new PrimitiveProviderDefinedType("Empty", "");

        final Buffer buffer = writeAndRead(pdt);
        final PrimitiveProviderDefinedType result = reader.read(buffer);

        assertEquals(pdt, result);
    }

    @Test
    public void shouldPreserveLeadingZeros() throws IOException {
        final PrimitiveProviderDefinedType pdt = new PrimitiveProviderDefinedType("Uint32", "007");

        final Buffer buffer = writeAndRead(pdt);
        final PrimitiveProviderDefinedType result = reader.read(buffer);

        assertEquals("007", result.getValue());
    }

    @Test
    public void shouldPreserveLargeValues() throws IOException {
        final PrimitiveProviderDefinedType pdt = new PrimitiveProviderDefinedType("Uint32", "4294967295");

        final Buffer buffer = writeAndRead(pdt);
        final PrimitiveProviderDefinedType result = reader.read(buffer);

        assertEquals("4294967295", result.getValue());
    }

    @Test
    public void shouldPreserveNonNumericStrings() throws IOException {
        final PrimitiveProviderDefinedType pdt = new PrimitiveProviderDefinedType("TinkerId", "abc-def-123");

        final Buffer buffer = writeAndRead(pdt);
        final PrimitiveProviderDefinedType result = reader.read(buffer);

        assertEquals("abc-def-123", result.getValue());
    }

    @Test
    public void shouldHandleNullPrimitivePdt() throws IOException {
        final Buffer buffer = bufferFactory.create(allocator.buffer());
        writer.write(null, buffer);
        buffer.readerIndex(0);
        final Object result = reader.read(buffer);
        assertNull(result);
    }
}

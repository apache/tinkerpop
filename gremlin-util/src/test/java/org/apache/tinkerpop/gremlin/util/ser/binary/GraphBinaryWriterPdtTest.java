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
package org.apache.tinkerpop.gremlin.util.ser.binary;

import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefined;
import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedType;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class GraphBinaryWriterPdtTest {

    private static final GraphBinaryReader reader = new GraphBinaryReader();
    private static final GraphBinaryWriter writer = new GraphBinaryWriter();
    private static final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private static final NettyBufferFactory bufferFactory = new NettyBufferFactory();

    @ProviderDefined
    static class TestPoint {
        int x;
        int y;

        TestPoint(int x, int y) {
            this.x = x;
            this.y = y;
        }
    }

    static class UnannotatedType {
        int value = 1;
    }

    @Test
    public void shouldAutoConvertAnnotatedObjectToPdt() throws IOException {
        final Buffer buffer = bufferFactory.create(allocator.buffer());
        writer.write(new TestPoint(1, 2), buffer);
        buffer.readerIndex(0);

        final ProviderDefinedType result = reader.read(buffer);
        assertEquals("TestPoint", result.getName());
        assertEquals(1, result.getFields().get("x"));
        assertEquals(2, result.getFields().get("y"));
    }

    @Test
    public void shouldThrowActionableMessageForUnannotatedType() {
        final Buffer buffer = bufferFactory.create(allocator.buffer());
        final IOException ex = assertThrows(IOException.class, () -> writer.write(new UnannotatedType(), buffer));
        assertTrue(ex.getMessage().contains("@ProviderDefined"));
        assertTrue(ex.getMessage().contains("UnannotatedType"));
    }

    @Test
    public void shouldNotDoubleWrapProviderDefinedType() throws IOException {
        final Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("x", 1);
        fields.put("y", 2);
        final ProviderDefinedType pdt = new ProviderDefinedType("TestPoint", fields);

        final Buffer buffer = bufferFactory.create(allocator.buffer());
        writer.write(pdt, buffer);
        buffer.readerIndex(0);

        final ProviderDefinedType result = reader.read(buffer);
        assertEquals(pdt, result);
    }
}

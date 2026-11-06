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
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.io.pdt.CompositePDT;
import org.apache.tinkerpop.gremlin.structure.io.pdt.CompositePDTAdapter;
import org.apache.tinkerpop.gremlin.structure.io.pdt.PDTRegistry;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CompositePDTSerializerTest {

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
    public void shouldRoundTripSimplePdt() throws IOException {
        final Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("x", 1);
        fields.put("y", "hello");
        final CompositePDT pdt = new CompositePDT("com.example.Point", fields);

        final Buffer buffer = writeAndRead(pdt);
        final CompositePDT result = reader.read(buffer);

        assertEquals(pdt, result);
    }

    @Test
    public void shouldRoundTripPdtWithNullFieldValue() throws IOException {
        final Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("name", "test");
        fields.put("value", null);
        final CompositePDT pdt = new CompositePDT("com.example.Nullable", fields);

        final Buffer buffer = writeAndRead(pdt);
        final CompositePDT result = reader.read(buffer);

        assertEquals(pdt, result);
    }

    @Test
    public void shouldRoundTripNestedPdt() throws IOException {
        final Map<String, Object> innerFields = new LinkedHashMap<>();
        innerFields.put("street", "123 Main");
        final CompositePDT inner = new CompositePDT("com.example.Address", innerFields);

        final Map<String, Object> outerFields = new LinkedHashMap<>();
        outerFields.put("name", "Alice");
        outerFields.put("address", inner);
        final CompositePDT outer = new CompositePDT("com.example.Person", outerFields);

        final Buffer buffer = writeAndRead(outer);
        final CompositePDT result = reader.read(buffer);

        assertEquals(outer, result);
    }

    @Test
    public void shouldRoundTripPdtInsideList() throws IOException {
        final Map<String, Object> fields = Collections.singletonMap("id", 42);
        final CompositePDT pdt = new CompositePDT("com.example.Item", fields);
        final List<Object> list = Arrays.asList(pdt, "other");

        final Buffer buffer = writeAndRead(list);
        final List<Object> result = reader.read(buffer);

        assertEquals(list, result);
    }

    @Test
    public void shouldRoundTripPdtInsideMapValue() throws IOException {
        final Map<String, Object> fields = Collections.singletonMap("val", 99L);
        final CompositePDT pdt = new CompositePDT("com.example.Wrapper", fields);
        final Map<String, Object> map = new HashMap<>();
        map.put("key", pdt);

        final Buffer buffer = writeAndRead(map);
        final Map<String, Object> result = reader.read(buffer);

        assertEquals(map, result);
    }

    @Test(expected = IOException.class)
    public void shouldThrowOnEmptyNameDuringRead() throws IOException {
        final Buffer buffer = bufferFactory.create(allocator.buffer());
        // Write type code for COMPOSITE_PDT
        buffer.writeByte(0xF0);
        // Write value_flag = 0 (not null)
        buffer.writeByte(0x00);
        // Write a fully-qualified empty string: type_code STRING (0x03), value_flag 0, length 0
        buffer.writeByte(0x03);
        buffer.writeByte(0x00);
        buffer.writeInt(0);
        // Write a fully-qualified map: type_code MAP (0x0A), value_flag 0, length 0
        buffer.writeByte(0x0A);
        buffer.writeByte(0x00);
        buffer.writeInt(0);

        buffer.readerIndex(0);
        reader.read(buffer);
    }

    @Test(expected = IOException.class)
    public void shouldThrowOnNonStringKeyInFieldsMap() throws IOException {
        final Buffer buffer = bufferFactory.create(allocator.buffer());
        // Write type code for COMPOSITE_PDT (0xF0), value_flag 0
        buffer.writeByte(0xF0);
        buffer.writeByte(0x00);
        // Write fully-qualified String name: type STRING (0x03), flag 0, length 4, "test"
        buffer.writeByte(0x03);
        buffer.writeByte(0x00);
        buffer.writeInt(4);
        buffer.writeBytes(new byte[]{'t', 'e', 's', 't'});
        // Write fully-qualified Map: type MAP (0x0A), flag 0, length 1 (one entry)
        buffer.writeByte(0x0A);
        buffer.writeByte(0x00);
        buffer.writeInt(1);
        // Key: INT type (0x01), flag 0, value 42
        buffer.writeByte(0x01);
        buffer.writeByte(0x00);
        buffer.writeInt(42);
        // Value: STRING type (0x03), flag 0, length 3, "val"
        buffer.writeByte(0x03);
        buffer.writeByte(0x00);
        buffer.writeInt(3);
        buffer.writeBytes(new byte[]{'v', 'a', 'l'});

        buffer.readerIndex(0);
        reader.read(buffer);
    }

    @Test
    public void shouldHandleNullPdt() throws IOException {
        final Buffer buffer = bufferFactory.create(allocator.buffer());
        writer.write(null, buffer);
        buffer.readerIndex(0);
        final Object result = reader.read(buffer);
        assertNull(result);
    }

    @Test
    public void shouldAutoHydrateWhenRegistryConfigured() throws IOException {
        final PDTRegistry pdtRegistry = PDTRegistry.empty();
        pdtRegistry.register(new CompositePDTAdapter<Map<String, Object>>() {
            @Override
            public String typeName() { return "com.example.Point"; }

            @Override
            public Class<Map<String, Object>> targetClass() { return (Class) Map.class; }

            @Override
            public Map<String, Object> fromFields(final Map<String, Object> fields) {
                final Map<String, Object> result = new LinkedHashMap<>(fields);
                result.put("hydrated", true);
                return result;
            }

            @Override
            public Map<String, Object> toFields(final Map<String, Object> value) { return value; }
        });

        final GraphBinaryReader hydratingReader = new GraphBinaryReader(
                TypeSerializerRegistry.INSTANCE, pdtRegistry);

        final Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("x", 1);
        fields.put("y", 2);
        final CompositePDT pdt = new CompositePDT("com.example.Point", fields);

        final Buffer buffer = writeAndRead(pdt);
        final Object result = hydratingReader.read(buffer);

        // Should be the hydrated map, not a CompositePDT
        assertEquals(true, ((Map<?, ?>) result).get("hydrated"));
        assertEquals(1, ((Map<?, ?>) result).get("x"));
        assertEquals(2, ((Map<?, ?>) result).get("y"));
    }

    @Test
    public void shouldNotHydrateWhenNoRegistryConfigured() throws IOException {
        final Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("x", 1);
        final CompositePDT pdt = new CompositePDT("com.example.Point", fields);

        final Buffer buffer = writeAndRead(pdt);
        final CompositePDT result = reader.read(buffer);

        assertEquals(pdt, result);
    }
}

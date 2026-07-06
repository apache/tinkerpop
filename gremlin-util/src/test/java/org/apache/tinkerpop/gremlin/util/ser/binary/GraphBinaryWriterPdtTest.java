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
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.io.pdt.CompositePDT;
import org.apache.tinkerpop.gremlin.structure.io.pdt.CompositePDTAdapter;
import org.apache.tinkerpop.gremlin.structure.io.pdt.PDTRegistry;
import org.apache.tinkerpop.gremlin.structure.io.pdt.PrimitivePDT;
import org.apache.tinkerpop.gremlin.structure.io.pdt.PrimitivePDTAdapter;
import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefined;
import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedTypeAdapter;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

    static class UnannotatedTypeAdapter implements CompositePDTAdapter<UnannotatedType> {
        @Override public String typeName() { return "UnannotatedType"; }
        @Override public Class<UnannotatedType> targetClass() { return UnannotatedType.class; }
        @Override public Map<String, Object> toFields(final UnannotatedType obj) {
            final Map<String, Object> m = new LinkedHashMap<>();
            m.put("value", obj.value);
            return m;
        }
        @Override public UnannotatedType fromFields(final Map<String, Object> fields) {
            final UnannotatedType t = new UnannotatedType();
            t.value = (int) fields.get("value");
            return t;
        }
    }

    @ProviderDefined(name = "AnnotatedName")
    static class AnnotatedDual {
        int x = 7;
    }

    static class AnnotatedDualAdapter implements CompositePDTAdapter<AnnotatedDual> {
        @Override public String typeName() { return "AdapterName"; }
        @Override public Class<AnnotatedDual> targetClass() { return AnnotatedDual.class; }
        @Override public Map<String, Object> toFields(final AnnotatedDual obj) {
            final Map<String, Object> m = new LinkedHashMap<>();
            m.put("viaAdapter", obj.x * 10);
            return m;
        }
        @Override public AnnotatedDual fromFields(final Map<String, Object> fields) {
            return new AnnotatedDual();
        }
    }

    @Test
    public void shouldAutoConvertAnnotatedObjectToPdt() throws IOException {
        final Buffer buffer = bufferFactory.create(allocator.buffer());
        writer.write(new TestPoint(1, 2), buffer);
        buffer.readerIndex(0);

        final CompositePDT result = reader.read(buffer);
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

    /**
     * Verifies that a type registered via a {@link ProviderDefinedTypeAdapter} (without the {@link ProviderDefined}
     * annotation) can be dehydrated on the write path by a registry-aware {@link GraphBinaryWriter} and then
     * hydrated back by the reader through the same registry.
     */
    @Test
    public void shouldDehydrateRegisteredButUnannotatedTypeViaAdapterOnWritePath() throws IOException {
        final PDTRegistry pdtRegistry = PDTRegistry.empty();
        pdtRegistry.register(new UnannotatedTypeAdapter());

        final GraphBinaryWriter registryWriter = new GraphBinaryWriter(TypeSerializerRegistry.INSTANCE, pdtRegistry);
        final GraphBinaryReader registryReader = new GraphBinaryReader(TypeSerializerRegistry.INSTANCE, pdtRegistry);

        final UnannotatedType original = new UnannotatedType();
        original.value = 42;

        final Buffer buffer = bufferFactory.create(allocator.buffer());
        registryWriter.write(original, buffer);
        buffer.readerIndex(0);

        final UnannotatedType result = registryReader.read(buffer);
        assertEquals(42, result.value);
    }

    /**
     * A registered adapter takes precedence over the {@link ProviderDefined} annotation when dehydrating on
     * the write path, consistent with GremlinLang.argAsString. AnnotatedDual is both annotated and has a
     * registered CompositePDTAdapter; the adapter's type name and fields must win.
     */
    @Test
    public void shouldPreferRegisteredAdapterOverAnnotationOnWritePath() throws IOException {
        final PDTRegistry pdtRegistry = PDTRegistry.empty();
        pdtRegistry.register(new AnnotatedDualAdapter());

        final GraphBinaryWriter registryWriter = new GraphBinaryWriter(TypeSerializerRegistry.INSTANCE, pdtRegistry);

        final Buffer buffer = bufferFactory.create(allocator.buffer());
        registryWriter.write(new AnnotatedDual(), buffer);
        buffer.readerIndex(0);

        // Read with a registry-free reader to inspect the raw serialized form (no hydration).
        final CompositePDT result = reader.read(buffer);
        assertEquals("AdapterName", result.getName());
        assertEquals(70, result.getFields().get("viaAdapter"));
        assertFalse(result.getFields().containsKey("x"));
    }

    @Test
    public void shouldNotDoubleWrapCompositePDT() throws IOException {
        final Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("x", 1);
        fields.put("y", 2);
        final CompositePDT pdt = new CompositePDT("TestPoint", fields);

        final Buffer buffer = bufferFactory.create(allocator.buffer());
        writer.write(pdt, buffer);
        buffer.readerIndex(0);

        final CompositePDT result = reader.read(buffer);
        assertEquals(pdt, result);
    }

    // === Primitive PDT Adapter write-path tests ===

    static class Uint32 {
        final long value;
        Uint32(long value) { this.value = value; }
    }

    static class Uint32Adapter implements PrimitivePDTAdapter<Uint32> {
        @Override public String typeName() { return "Uint32"; }
        @Override public Class<Uint32> targetClass() { return Uint32.class; }
        @Override public String toValue(Uint32 obj) { return Long.toString(obj.value); }
        @Override public Uint32 fromValue(String value) { return new Uint32(Long.parseLong(value)); }
    }

    @Test
    public void shouldDehydratePrimitiveAdapterOnWritePathAndHydrateBack() throws IOException {
        final PDTRegistry pdtRegistry = PDTRegistry.empty();
        pdtRegistry.register(new Uint32Adapter());

        final GraphBinaryWriter registryWriter = new GraphBinaryWriter(TypeSerializerRegistry.INSTANCE, pdtRegistry);
        final GraphBinaryReader registryReader = new GraphBinaryReader(TypeSerializerRegistry.INSTANCE, pdtRegistry);

        final Uint32 original = new Uint32(12345L);

        final Buffer buffer = bufferFactory.create(allocator.buffer());
        registryWriter.write(original, buffer);
        buffer.readerIndex(0);

        final Uint32 result = registryReader.read(buffer);
        assertEquals(12345L, result.value);
    }

    @Test
    public void shouldRoundTripPrimitivePDTWithoutRegistry() throws IOException {
        final PrimitivePDT pdt = new PrimitivePDT("Uint32", "99");

        final Buffer buffer = bufferFactory.create(allocator.buffer());
        writer.write(pdt, buffer);
        buffer.readerIndex(0);

        final PrimitivePDT result = reader.read(buffer);
        assertEquals(pdt, result);
    }

    @Test
    public void shouldRoundTripPrimitiveNestedInComposite() throws IOException {
        final PDTRegistry pdtRegistry = PDTRegistry.empty();
        pdtRegistry.register(new Uint32Adapter());
        pdtRegistry.register(new UnannotatedTypeAdapter());

        final GraphBinaryWriter registryWriter = new GraphBinaryWriter(TypeSerializerRegistry.INSTANCE, pdtRegistry);
        final GraphBinaryReader registryReader = new GraphBinaryReader(TypeSerializerRegistry.INSTANCE, pdtRegistry);

        // Build a composite PDT with a nested primitive value
        final Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("value", 7);
        fields.put("id", new PrimitivePDT("Uint32", "42"));
        final CompositePDT compositePdt = new CompositePDT("UnannotatedType", fields);

        final Buffer buffer = bufferFactory.create(allocator.buffer());
        registryWriter.write(compositePdt, buffer);
        buffer.readerIndex(0);

        // The reader hydrates the composite (via UnannotatedTypeAdapter) and the nested primitive
        // should have been hydrated to Uint32 by the registry's hydrateValue recursion
        final Object result = registryReader.read(buffer);
        assertTrue(result instanceof UnannotatedType);
        // Note: UnannotatedTypeAdapter only maps "value" field to an int, so the hydrated "id" field
        // ends up being handled during the composite adapter's fromFields. Since the adapter
        // only reads "value", we verify the composite round-tripped correctly.
        assertEquals(7, ((UnannotatedType) result).value);
    }
}

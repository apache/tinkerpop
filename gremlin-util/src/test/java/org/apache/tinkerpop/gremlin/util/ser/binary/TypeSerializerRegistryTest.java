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

import org.apache.tinkerpop.gremlin.util.ser.binary.types.sample.SamplePerson;
import org.apache.tinkerpop.gremlin.util.ser.binary.types.sample.SamplePersonSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertSame;

public class TypeSerializerRegistryTest {

    @Test
    public void shouldResolveToUserProvidedForInterfaces_1() throws IOException {
        final TypeSerializer<VertexProperty> expected = new TestVertexPropertySerializer();
        final TypeSerializerRegistry registry = TypeSerializerRegistry.build()
                .add(VertexProperty.class, expected).create();

        assertSame(expected, registry.getSerializer(VertexProperty.class));
        assertSame(expected, registry.getSerializer(DataType.VERTEXPROPERTY));
    }

    @Test
    public void shouldResolveToUserProvidedForInterfaces_2() throws IOException {
        final TypeSerializer<Property> expected = new TestPropertySerializer();
        final TypeSerializerRegistry registry = TypeSerializerRegistry.build()
                .add(Property.class, expected).create();

        assertSame(expected, registry.getSerializer(Property.class));
        assertSame(expected, registry.getSerializer(DataType.PROPERTY));
    }

    @Test
    public void shouldResolveToUserProvidedForClasses() throws Exception {
        final TypeSerializer<UUID> expected = new TestUUIDSerializer();
        final TypeSerializerRegistry registry = TypeSerializerRegistry.build()
                .add(UUID.class, expected).create();

        assertSame(expected, registry.getSerializer(UUID.class));
        assertSame(expected, registry.getSerializer(DataType.UUID));
    }

    @Test
    public void shouldResolveToTheFirstSerializerForInterfaces() throws IOException {
        final TypeSerializer<VertexProperty> expectedForVertexProperty = new TestVertexPropertySerializer();
        final TypeSerializer<Property> expectedForProperty = new TestPropertySerializer();
        final TypeSerializerRegistry registry = TypeSerializerRegistry.build()
                .add(VertexProperty.class, expectedForVertexProperty)
                .add(Property.class, expectedForProperty).create();

        assertSame(expectedForVertexProperty, registry.getSerializer(VertexProperty.class));
        assertSame(expectedForProperty, registry.getSerializer(Property.class));
        assertSame(expectedForVertexProperty, registry.getSerializer(DataType.VERTEXPROPERTY));
        assertSame(expectedForProperty, registry.getSerializer(DataType.PROPERTY));
    }

    @Test
    public void shouldUseFallbackResolverWhenThereIsNoMatch() {
        final int[] called = {0};
        final TypeSerializerRegistry registry = TypeSerializerRegistry.build()
                .withFallbackResolver(t -> {
                    called[0]++;
                    return null;
                }).create();

        String message = null;
        try {
            registry.getSerializer(SamplePerson.class);
        } catch (IOException ex) {
            message = ex.getMessage();
        }

        assertEquals("Serializer for type org.apache.tinkerpop.gremlin.util.ser.binary.types.sample.SamplePerson not found", message);
        assertEquals(1, called[0]);
    }

    @Test
    public void shouldUseFallbackResolverReturnValue() throws IOException {
        TypeSerializer expected = new SamplePersonSerializer();
        final int[] called = {0};
        final TypeSerializerRegistry registry = TypeSerializerRegistry.build()
                .withFallbackResolver(t -> {
                    called[0]++;
                    return expected;
                }).create();

        TypeSerializer<SamplePerson> serializer = registry.getSerializer(SamplePerson.class);
        assertEquals(1, called[0]);
        assertSame(expected, serializer);
    }

    private static class TestVertexPropertySerializer extends TestBaseTypeSerializer<VertexProperty> {

        @Override
        public DataType getDataType() {
            return DataType.VERTEXPROPERTY;
        }
    }

    private static class TestPropertySerializer extends TestBaseTypeSerializer<Property> {

        @Override
        public DataType getDataType() {
            return DataType.PROPERTY;
        }
    }

    private static class TestUUIDSerializer extends TestBaseTypeSerializer<UUID> {

        @Override
        public DataType getDataType() {
            return DataType.UUID;
        }
    }

    private static abstract class TestBaseTypeSerializer<T> implements TypeSerializer<T> {
        @Override
        public T read(Buffer buffer, GraphBinaryReader context) {
            return null;
        }

        @Override
        public T readValue(Buffer buffer, GraphBinaryReader context, boolean nullable) {
            return null;
        }

        @Override
        public void write(T value, Buffer buffer, GraphBinaryWriter context) {

        }

        @Override
        public void writeValue(T value, Buffer buffer, GraphBinaryWriter context, boolean nullable) {

        }
    }
}

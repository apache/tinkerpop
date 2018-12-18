package org.apache.tinkerpop.gremlin.driver.ser.binary;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Test;

import java.util.UUID;

import static junit.framework.TestCase.assertSame;

public class TypeSerializerRegistryTest {

    @Test
    public void shouldResolveToUserProvidedForInterfaces_1() throws SerializationException {
        final TypeSerializer<VertexProperty> expected = new TestVertexPropertySerializer();
        final TypeSerializerRegistry registry = TypeSerializerRegistry.build()
                .add(VertexProperty.class, expected).create();

        assertSame(expected, registry.getSerializer(VertexProperty.class));
        assertSame(expected, registry.getSerializer(DataType.VERTEXPROPERTY));
    }

    @Test
    public void shouldResolveToUserProvidedForInterfaces_2() throws SerializationException {
        final TypeSerializer<Property> expected = new TestPropertySerializer();
        final TypeSerializerRegistry registry = TypeSerializerRegistry.build()
                .add(Property.class, expected).create();

        assertSame(expected, registry.getSerializer(Property.class));
        assertSame(expected, registry.getSerializer(DataType.PROPERTY));
    }

    @Test
    public void shouldResolveToUserProvidedForClasses() throws SerializationException {
        final TypeSerializer<UUID> expected = new TestUUIDSerializer();
        final TypeSerializerRegistry registry = TypeSerializerRegistry.build()
                .add(UUID.class, expected).create();

        assertSame(expected, registry.getSerializer(UUID.class));
        assertSame(expected, registry.getSerializer(DataType.UUID));
    }

    @Test
    public void shouldResolveToTheFirstSerializerForInterfaces() throws SerializationException {
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
        public T read(ByteBuf buffer, GraphBinaryReader context) {
            return null;
        }

        @Override
        public T readValue(ByteBuf buffer, GraphBinaryReader context, boolean nullable) {
            return null;
        }

        @Override
        public ByteBuf write(T value, ByteBufAllocator allocator, GraphBinaryWriter context) {
            return null;
        }

        @Override
        public ByteBuf writeValue(T value, ByteBufAllocator allocator, GraphBinaryWriter context, boolean nullable) {
            return null;
        }
    }
}

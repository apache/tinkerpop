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
package org.apache.tinkerpop.gremlin.structure.io.binary;

import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.BigDecimalSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.BigIntegerSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.BulkSetSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.ByteBufferSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.CharSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.CustomTypeSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.DurationSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.EdgeSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.EnumSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.GraphSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.ListSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.MapEntrySerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.MapSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.DateTimeSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.PathSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.PropertySerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.SetSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.SingleTypeSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.StringSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.TransformSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.TraversalExplanationSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.TreeSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.UUIDSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.VertexPropertySerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.VertexSerializer;
import org.javatuples.Pair;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class TypeSerializerRegistry {

    public static Builder build() {
        return new Builder();
    }

    private static final RegistryEntry[] defaultEntries = new RegistryEntry[] {
            new RegistryEntry<>(Integer.class, SingleTypeSerializer.IntSerializer),
            new RegistryEntry<>(Long.class, SingleTypeSerializer.LongSerializer),
            new RegistryEntry<>(String.class, new StringSerializer()),
            new RegistryEntry<>(Double.class, SingleTypeSerializer.DoubleSerializer),
            new RegistryEntry<>(Float.class, SingleTypeSerializer.FloatSerializer),
            new RegistryEntry<>(BulkSet.class, new BulkSetSerializer()),  // needs to register before list so ListSerializer is registered as LIST by overwrite
            new RegistryEntry<>(List.class, new ListSerializer()),
            new RegistryEntry<>(Map.class, new MapSerializer()),
            new RegistryEntry<>(Set.class, new SetSerializer()),
            new RegistryEntry<>(UUID.class, new UUIDSerializer()),
            new RegistryEntry<>(Edge.class, new EdgeSerializer()),
            new RegistryEntry<>(Path.class, new PathSerializer()),
            new RegistryEntry<>(VertexProperty.class, new VertexPropertySerializer()), // needs to register before the less specific Property
            new RegistryEntry<>(Property.class, new PropertySerializer()),
            new RegistryEntry<>(Graph.class, new GraphSerializer()),
            new RegistryEntry<>(Vertex.class, new VertexSerializer()),
            new RegistryEntry<>(Direction.class, EnumSerializer.DirectionSerializer),
            new RegistryEntry<>(T.class, EnumSerializer.TSerializer),
            new RegistryEntry<>(Merge.class, EnumSerializer.MergeSerializer),
            new RegistryEntry<>(BigDecimal.class, new BigDecimalSerializer()),
            new RegistryEntry<>(BigInteger.class, new BigIntegerSerializer()),
            new RegistryEntry<>(Byte.class, SingleTypeSerializer.ByteSerializer),
            new RegistryEntry<>(ByteBuffer.class, new ByteBufferSerializer()),
            new RegistryEntry<>(Short.class, SingleTypeSerializer.ShortSerializer),
            new RegistryEntry<>(Boolean.class, SingleTypeSerializer.BooleanSerializer),
            new RegistryEntry<>(Tree.class, new TreeSerializer()),
            new RegistryEntry<>(Marker.class, SingleTypeSerializer.MarkerSerializer),

            // TransformSerializer implementations
            new RegistryEntry<>(Map.Entry.class, new MapEntrySerializer()),
            new RegistryEntry<>(TraversalExplanation.class, new TraversalExplanationSerializer()),

            new RegistryEntry<>(Character.class, new CharSerializer()),
            new RegistryEntry<>(Duration.class, new DurationSerializer()),
            new RegistryEntry<>(OffsetDateTime.class, new DateTimeSerializer())
    };

    public static final TypeSerializerRegistry INSTANCE = build().create();

    public static class Builder {
        private final List<RegistryEntry> list = new LinkedList<>();
        private Function<Class<?>, TypeSerializer<?>> fallbackResolver;

        /**
         * Adds a serializer for a built-in type.
         * <p>
         *     Note that when providing a serializer for an Interface ({@link VertexProperty}, {@link Property},
         *     {@link Vertex}, ...), the serializer resolution from the registry will be defined by the order that
         *     it was provided. In this case, you should provide the type serializer starting from most specific to
         *     less specific or to put it in other words, start from derived types and then parent types, e.g.,
         *     {@link VertexProperty} before {@link Property}.
         * </p>
         */
        public <DT> Builder add(final Class<DT> type, final TypeSerializer<DT> serializer) {
            if (serializer.getDataType() == DataType.CUSTOM) {
                throw new IllegalArgumentException("DataType can not be CUSTOM, use addCustomType() method instead");
            }

            if (serializer.getDataType() == DataType.UNSPECIFIED_NULL) {
                throw new IllegalArgumentException("Adding a serializer for a UNSPECIFIED_NULL is not permitted");
            }

            if (serializer instanceof CustomTypeSerializer) {
                throw new IllegalArgumentException(
                        "CustomTypeSerializer implementations are reserved for customtypes");
            }

            list.add(new RegistryEntry<>(type, serializer));
            return this;
        }

        /**
         * Adds a serializer for a custom type.
         */
        public <DT> Builder addCustomType(final Class<DT> type, final CustomTypeSerializer<DT> serializer) {
            if (serializer == null) {
                throw new NullPointerException("serializer can not be null");
            }

            if (serializer.getDataType() != DataType.CUSTOM) {
                throw new IllegalArgumentException("Custom serializer must use CUSTOM data type");
            }

            if (serializer.getTypeName() == null) {
                throw new NullPointerException("serializer custom type name can not be null");
            }

            list.add(new RegistryEntry<>(type, serializer));
            return this;
        }

        /**
         * Provides a way to resolve the type serializer to use when there isn't any direct match.
         */
        public Builder withFallbackResolver(final Function<Class<?>, TypeSerializer<?>> fallbackResolver) {
            this.fallbackResolver = fallbackResolver;
            return this;
        }

        /**
         * Add {@link CustomTypeSerializer} by way of an {@link IoRegistry}. The registry entries should be bound to
         * {@link GraphBinaryIo}.
         */
        public Builder addRegistry(final IoRegistry registry) {
            if (null == registry) throw new IllegalArgumentException("The registry cannot be null");

            final List<Pair<Class, CustomTypeSerializer>> classSerializers = registry.find(GraphBinaryIo.class, CustomTypeSerializer.class);
            for (Pair<Class,CustomTypeSerializer> cs : classSerializers) {
                addCustomType(cs.getValue0(), cs.getValue1());
            }

            return this;
        }

        /**
         * Creates a new {@link TypeSerializerRegistry} instance based on the serializers added.
         */
        public TypeSerializerRegistry create() {
            return new TypeSerializerRegistry(list, fallbackResolver);
        }
    }

    private static class RegistryEntry<DT> {
        private final Class<DT> type;
        private final TypeSerializer<DT> typeSerializer;

        private RegistryEntry(Class<DT> type, TypeSerializer<DT> typeSerializer) {
            this.type = type;
            this.typeSerializer = typeSerializer;
        }

        public Class<DT> getType() {
            return type;
        }

        public DataType getDataType() {
            return typeSerializer.getDataType();
        }

        public String getCustomTypeName() {
            if (getDataType() != DataType.CUSTOM) {
                return null;
            }

            final CustomTypeSerializer customTypeSerializer = (CustomTypeSerializer) typeSerializer;
            return customTypeSerializer.getTypeName();
        }

        public TypeSerializer<DT> getTypeSerializer() {
            return typeSerializer;
        }
    }

    private final Map<Class<?>, TypeSerializer<?>> serializers = new HashMap<>();
    private final Map<Class<?>, TypeSerializer<?>> serializersByInterface = new LinkedHashMap<>();
    private final Map<DataType, TypeSerializer<?>> serializersByDataType = new HashMap<>();
    private final Map<String, CustomTypeSerializer> serializersByCustomTypeName = new HashMap<>();
    private Function<Class<?>, TypeSerializer<?>> fallbackResolver;

    /**
     * Stores serializers by class, where the class resolution involved a lookup or special conditions.
     * This is used for interface implementations and enums.
     */
    private final ConcurrentHashMap<Class<?>, TypeSerializer<?>> serializersByImplementation = new ConcurrentHashMap<>();

    private TypeSerializerRegistry(final Collection<RegistryEntry> entries,
                                   final Function<Class<?>, TypeSerializer<?>> fallbackResolver) {
        final Set<Class> providedTypes = new HashSet<>(entries.size());

        // Include user-provided entries first
        for (RegistryEntry entry : entries) {
            put(entry);
            providedTypes.add(entry.type);
        }

        // Followed by the defaults
        Arrays.stream(defaultEntries).filter(e -> !providedTypes.contains(e.type)).forEach(this::put);

        this.fallbackResolver = fallbackResolver;
    }

    private void put(final RegistryEntry entry) {
        final Class type = entry.getType();
        final TypeSerializer serializer = entry.getTypeSerializer();

        if (type == null) {
            throw new NullPointerException("Type can not be null");
        }

        if (serializer == null) {
            throw new NullPointerException("Serializer instance can not be null");
        }

        if (serializer.getDataType() == null && !(serializer instanceof TransformSerializer)) {
            throw new NullPointerException("Serializer data type can not be null");
        }

        if (!type.isInterface() && !Modifier.isAbstract(type.getModifiers())) {
            // Direct class match
            serializers.put(type, serializer);
        } else {
            // Interface or abstract class can be assigned by provided type
            serializersByInterface.put(type, serializer);
        }

        if (serializer.getDataType() == DataType.CUSTOM) {
            serializersByCustomTypeName.put(entry.getCustomTypeName(), (CustomTypeSerializer) serializer);
        } else if (serializer.getDataType() != null) {
            serializersByDataType.put(serializer.getDataType(), serializer);
        }
    }

    public <DT> TypeSerializer<DT> getSerializer(final Class<DT> type) throws IOException {
        TypeSerializer<?> serializer = serializers.get(type);

        if (null == serializer) {
            // Try to obtain the serializer by the type interface implementation or superclass,
            // when previously accessed.
            serializer = serializersByImplementation.get(type);
        }

        if (serializer != null) {
            return (TypeSerializer) serializer;
        }

        // Use different lookup techniques and cache the lookup result when successful

        if (Enum.class.isAssignableFrom(type)) {
            // maybe it's a enum - enums with bodies are weird in java, they are subclasses of themselves, so
            // Columns.values will be of type Column$2.
            serializer = serializers.get(type.getSuperclass());
        }

        if (null == serializer) {
            // Lookup by interface
            for (Map.Entry<Class<?>, TypeSerializer<?>> entry : serializersByInterface.entrySet()) {
                if (entry.getKey().isAssignableFrom(type)) {
                    serializer = entry.getValue();
                    break;
                }
            }
        }

        if (null == serializer && fallbackResolver != null) {
            serializer = fallbackResolver.apply(type);
        }

        validateInstance(serializer, type.getTypeName());

        // Store the lookup match to avoid looking it up in the future
        serializersByImplementation.put(type, serializer);

        return (TypeSerializer<DT>) serializer;
    }

    public <DT> TypeSerializer<DT> getSerializer(final DataType dataType) throws IOException {
        if (dataType == DataType.CUSTOM) {
            throw new IllegalArgumentException("Custom type serializers can not be retrieved using this method");
        }

        return validateInstance(serializersByDataType.get(dataType), dataType.toString());
    }

    /**
     * Gets the serializer for a given custom type name.
     */
    public <DT> CustomTypeSerializer<DT> getSerializerForCustomType(final String name) throws IOException {
        final CustomTypeSerializer serializer = serializersByCustomTypeName.get(name);

        if (serializer == null) {
            throw new IOException(String.format("Serializer for custom type '%s' not found", name));
        }

        return serializer;
    }

    private static TypeSerializer validateInstance(final TypeSerializer serializer, final String typeName) throws IOException {
        if (serializer == null) {
            throw new IOException(String.format("Serializer for type %s not found", typeName));
        }

        return serializer;
    }
}

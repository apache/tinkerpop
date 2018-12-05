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
package org.apache.tinkerpop.gremlin.driver.ser.binary;

import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.BigDecimalSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.BigIntegerSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.BindingSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.ByteCodeSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.ClassSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.CustomTypeSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.DateSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.EdgeSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.EnumSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.LambdaSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.ListSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.MapSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.PSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.PathSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.PropertySerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.SetSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.SingleTypeSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.StringSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.TraverserSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.UUIDSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.VertexPropertySerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.VertexSerializer;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.function.Lambda;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class TypeSerializerRegistry {
    public static final TypeSerializerRegistry INSTANCE = build().create();

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {
        private final List<RegistryEntry> list = new LinkedList<>(Arrays.asList(
                new RegistryEntry<>(SackFunctions.Barrier.class, EnumSerializer.BarrierSerializer),
                new RegistryEntry<>(VertexProperty.Cardinality.class, EnumSerializer.CardinalitySerializer),
                new RegistryEntry<>(Column.class, EnumSerializer.ColumnSerializer),
                new RegistryEntry<>(Direction.class, EnumSerializer.DirectionSerializer),
                new RegistryEntry<>(Operator.class, EnumSerializer.OperatorSerializer),
                new RegistryEntry<>(Order.class, EnumSerializer.OrderSerializer),
                new RegistryEntry<>(TraversalOptionParent.Pick.class, EnumSerializer.PickSerializer),
                new RegistryEntry<>(Pop.class, EnumSerializer.PopSerializer),
                new RegistryEntry<>(P.class, new PSerializer<>()),
                new RegistryEntry<>(AndP.class, new PSerializer<>()),
                new RegistryEntry<>(OrP.class, new PSerializer<>()),
                new RegistryEntry<>(Scope.class, EnumSerializer.ScopeSerializer),
                new RegistryEntry<>(T.class, EnumSerializer.TSerializer),
                new RegistryEntry<>(String.class, new StringSerializer()),
                new RegistryEntry<>(UUID.class, new UUIDSerializer()),
                new RegistryEntry<>(Map.class, new MapSerializer()),
                new RegistryEntry<>(List.class, new ListSerializer()),
                new RegistryEntry<>(Set.class, new SetSerializer()),
                new RegistryEntry<>(Edge.class, new EdgeSerializer()),
                new RegistryEntry<>(VertexProperty.class, new VertexPropertySerializer()),
                new RegistryEntry<>(Property.class, new PropertySerializer()),
                new RegistryEntry<>(Path.class, new PathSerializer()),
                new RegistryEntry<>(Vertex.class, new VertexSerializer()),
                new RegistryEntry<>(Lambda.class, new LambdaSerializer()),
                new RegistryEntry<>(Integer.class, SingleTypeSerializer.IntSerializer),
                new RegistryEntry<>(Long.class, SingleTypeSerializer.LongSerializer),
                new RegistryEntry<>(Double.class, SingleTypeSerializer.DoubleSerializer),
                new RegistryEntry<>(Float.class, SingleTypeSerializer.FloatSerializer),
                new RegistryEntry<>(Short.class, SingleTypeSerializer.ShortSerializer),
                new RegistryEntry<>(Boolean.class, SingleTypeSerializer.BooleanSerializer),
                new RegistryEntry<>(Byte.class, SingleTypeSerializer.ByteSerializer),
                new RegistryEntry<>(BigInteger.class, new BigIntegerSerializer()),
                new RegistryEntry<>(BigDecimal.class, new BigDecimalSerializer()),
                new RegistryEntry<>(Class.class, new ClassSerializer()),
                new RegistryEntry<>(Date.class, new DateSerializer(DataType.TIMESTAMP)),
                new RegistryEntry<>(Date.class, new DateSerializer(DataType.DATE)),
                new RegistryEntry<>(Traverser.class, new TraverserSerializer()),
                new RegistryEntry<>(Bytecode.class, new ByteCodeSerializer()),
                new RegistryEntry<>(Bytecode.Binding.class, new BindingSerializer())));

        /**
         * Adds a serializer for a built-in type.
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
         * Creates a new {@link TypeSerializerRegistry} instance based on the serializers added.
         */
        public TypeSerializerRegistry create() {
            return new TypeSerializerRegistry(list);
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

            CustomTypeSerializer customTypeSerializer = (CustomTypeSerializer) typeSerializer;
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

    /**
     * Stores serializers by class, where the class resolution involved a lookup or special conditions.
     * This is used for interface implementations and enums.
     */
    private final ConcurrentHashMap<Class<?>, TypeSerializer<?>> serializersByImplementation = new ConcurrentHashMap<>();

    private TypeSerializerRegistry(final Collection<RegistryEntry> entries) {
        for (RegistryEntry entry : entries) {
            put(entry);
        }
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

        if (serializer.getDataType() == null) {
            throw new NullPointerException("Serializer data type can not be null");
        }

        if (!type.isInterface()) {
            // Direct class match
            serializers.put(type, serializer);
        } else {
            // Interface can be assigned by provided type
            serializersByInterface.put(type, serializer);
        }

        if (serializer.getDataType() != DataType.CUSTOM) {
            serializersByDataType.put(serializer.getDataType(), serializer);
        } else {
            serializersByCustomTypeName.put(entry.getCustomTypeName(), (CustomTypeSerializer) serializer);
        }
    }

    public <DT> TypeSerializer<DT> getSerializer(final Class<DT> type) throws SerializationException {
        TypeSerializer<?> serializer = serializers.get(type);

        if (null == serializer) {
            // Try to obtain the serializer by the type interface implementation or superclass,
            // when previously accessed.
            serializer = serializersByImplementation.get(type);
        }

        if (null == serializer && Enum.class.isAssignableFrom(type)) {
            // maybe it's a enum - enums with bodies are weird in java, they are subclasses of themselves, so
            // Columns.values will be of type Column$2.
            serializer = serializers.get(type.getSuperclass());

            // In case it matched, store the match
            if (serializer != null) {
                serializersByImplementation.put(type, serializer);
            }
        }

        if (null == serializer) {
            // Lookup by interface
            for (Map.Entry<Class<?>, TypeSerializer<?>> entry : serializersByInterface.entrySet()) {
                if (entry.getKey().isAssignableFrom(type)) {
                    serializer = entry.getValue();
                    // Store the type-to-interface match, to avoid looking it up in the future
                    serializersByImplementation.put(type, serializer);
                    break;
                }
            }
        }

        return validateInstance(serializer, type.getTypeName());
    }

    public <DT> TypeSerializer<DT> getSerializer(final DataType dataType) throws SerializationException {
        if (dataType == DataType.CUSTOM) {
            throw new IllegalArgumentException("Custom type serializers can not be retrieved using this method");
        }

        return validateInstance(serializersByDataType.get(dataType), dataType.toString());
    }

    /**
     * Gets the serializer for a given custom type name.
     */
    public <DT> CustomTypeSerializer<DT> getSerializerForCustomType(final String name) throws SerializationException {
        final CustomTypeSerializer serializer = serializersByCustomTypeName.get(name);

        if (serializer == null) {
            throw new SerializationException(String.format("Serializer for custom type '%s' not found", name));
        }

        return serializer;
    }

    private static TypeSerializer validateInstance(final TypeSerializer serializer, final String typeName) throws SerializationException {
        if (serializer == null) {
            throw new SerializationException(String.format("Serializer for type %s not found", typeName));
        }

        return serializer;
    }
}

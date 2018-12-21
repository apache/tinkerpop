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
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.*;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.function.Lambda;

import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class TypeSerializerRegistry {

    public static Builder build() {
        return new Builder();
    }

    private static final RegistryEntry[] defaultEntries = new RegistryEntry[] {
            new RegistryEntry<>(Integer.class, SingleTypeSerializer.IntSerializer),
            new RegistryEntry<>(Long.class, SingleTypeSerializer.LongSerializer),
            new RegistryEntry<>(String.class, new StringSerializer()),
            new RegistryEntry<>(Date.class, DateSerializer.DateSerializer),
            new RegistryEntry<>(Timestamp.class, DateSerializer.TimestampSerializer),
            new RegistryEntry<>(Class.class, new ClassSerializer()),
            new RegistryEntry<>(Double.class, SingleTypeSerializer.DoubleSerializer),
            new RegistryEntry<>(Float.class, SingleTypeSerializer.FloatSerializer),
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
            new RegistryEntry<>(SackFunctions.Barrier.class, EnumSerializer.BarrierSerializer),
            new RegistryEntry<>(Bytecode.Binding.class, new BindingSerializer()),
            new RegistryEntry<>(Bytecode.class, new ByteCodeSerializer()),
            new RegistryEntry<>(VertexProperty.Cardinality.class, EnumSerializer.CardinalitySerializer),
            new RegistryEntry<>(Column.class, EnumSerializer.ColumnSerializer),
            new RegistryEntry<>(Direction.class, EnumSerializer.DirectionSerializer),
            new RegistryEntry<>(Operator.class, EnumSerializer.OperatorSerializer),
            new RegistryEntry<>(Order.class, EnumSerializer.OrderSerializer),
            new RegistryEntry<>(TraversalOptionParent.Pick.class, EnumSerializer.PickSerializer),
            new RegistryEntry<>(Pop.class, EnumSerializer.PopSerializer),
            new RegistryEntry<>(Lambda.class, new LambdaSerializer()),
            new RegistryEntry<>(P.class, new PSerializer<>(DataType.P, P.class)),
            new RegistryEntry<>(AndP.class, new PSerializer<>(DataType.P, AndP.class)),
            new RegistryEntry<>(OrP.class, new PSerializer<>(DataType.P, OrP.class)),
            new RegistryEntry<>(TextP.class, new PSerializer<>(DataType.TEXTP, TextP.class)),
            new RegistryEntry<>(Scope.class, EnumSerializer.ScopeSerializer),
            new RegistryEntry<>(T.class, EnumSerializer.TSerializer),
            new RegistryEntry<>(Traverser.class, new TraverserSerializer()),
            new RegistryEntry<>(BigDecimal.class, new BigDecimalSerializer()),
            new RegistryEntry<>(BigInteger.class, new BigIntegerSerializer()),
            new RegistryEntry<>(Byte.class, SingleTypeSerializer.ByteSerializer),
            new RegistryEntry<>(ByteBuffer.class, new ByteBufferSerializer()),
            new RegistryEntry<>(Short.class, SingleTypeSerializer.ShortSerializer),
            new RegistryEntry<>(Boolean.class, SingleTypeSerializer.BooleanSerializer),
            new RegistryEntry<>(TraversalStrategy.class, new TraversalStrategySerializer()),
            new RegistryEntry<>(BulkSet.class, new BulkSetSerializer()),
            new RegistryEntry<>(Tree.class, new TreeSerializer()),
            new RegistryEntry<>(Metrics.class, new MetricsSerializer()),

            // TransformSerializer implementations
            new RegistryEntry<>(Map.Entry.class, new MapEntrySerializer()),
            new RegistryEntry<>(TraversalExplanation.class, new TraversalExplanationSerializer()),

            new RegistryEntry<>(Character.class, new CharSerializer()),
            new RegistryEntry<>(Duration.class, new DurationSerializer()),
            new RegistryEntry<>(InetAddress.class, new InetAddressSerializer()),
            new RegistryEntry<>(Inet4Address.class, new InetAddressSerializer<>()),
            new RegistryEntry<>(Inet6Address.class, new InetAddressSerializer<>()),
            new RegistryEntry<>(Instant.class, new InstantSerializer()),
            new RegistryEntry<>(LocalDate.class, new LocalDateSerializer()),
            new RegistryEntry<>(LocalTime.class, new LocalTimeSerializer()),
            new RegistryEntry<>(LocalDateTime.class, new LocalDateTimeSerializer()),
            new RegistryEntry<>(MonthDay.class, new MonthDaySerializer()),
            new RegistryEntry<>(OffsetDateTime.class, new OffsetDateTimeSerializer()),
            new RegistryEntry<>(OffsetTime.class, new OffsetTimeSerializer()),
            new RegistryEntry<>(Period.class, new PeriodSerializer()),
            new RegistryEntry<>(Year.class, SingleTypeSerializer.YearSerializer),
            new RegistryEntry<>(YearMonth.class, new YearMonthSerializer()),
            new RegistryEntry<>(ZonedDateTime.class, new ZonedDateTimeSerializer()),
            new RegistryEntry<>(ZoneOffset.class, new ZoneOffsetSerializer()) };

    public static final TypeSerializerRegistry INSTANCE = build().create();

    public static class Builder {
        private final List<RegistryEntry> list = new LinkedList<>();

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

    /**
     * Stores serializers by class, where the class resolution involved a lookup or special conditions.
     * This is used for interface implementations and enums.
     */
    private final ConcurrentHashMap<Class<?>, TypeSerializer<?>> serializersByImplementation = new ConcurrentHashMap<>();

    private TypeSerializerRegistry(final Collection<RegistryEntry> entries) {
        final Set<Class> providedTypes = new HashSet<>(entries.size());

        // Include user-provided entries first
        for (RegistryEntry entry : entries) {
            put(entry);
            providedTypes.add(entry.type);
        }

        // Followed by the defaults
        Arrays.stream(defaultEntries).filter(e -> !providedTypes.contains(e.type)).forEach(this::put);
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

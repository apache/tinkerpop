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

import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.*;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;

import java.util.*;

public class TypeSerializerRegistry {
    public static final TypeSerializerRegistry INSTANCE = new TypeSerializerRegistry();

    private final Map<Class<?>, TypeSerializer<?>> serializers = new HashMap<>();
    private final Map<Class<?>, TypeSerializer<?>> serializersByInterface = new HashMap<>();
    private final Map<DataType, TypeSerializer<?>> serializersByDataType = new HashMap<>();

    private TypeSerializerRegistry() {
        put(String.class, DataType.STRING, new StringSerializer());
        put(UUID.class, DataType.UUID, new UUIDSerializer());

        put(Map.class, DataType.MAP, new MapSerializer());
        put(List.class, DataType.LIST, new ListSerializer());

        put(Integer.class, DataType.INT, SingleTypeSerializer.IntSerializer);
        put(Long.class, DataType.LONG, SingleTypeSerializer.LongSerializer);
        put(Double.class, DataType.DOUBLE, SingleTypeSerializer.DoubleSerializer);
        put(Float.class, DataType.FLOAT, SingleTypeSerializer.FloatSerializer);
        put(Short.class, DataType.SHORT, SingleTypeSerializer.ShortSerializer);
        put(Boolean.class, DataType.BOOLEAN, SingleTypeSerializer.BooleanSerializer);
        put(Byte.class, DataType.BYTE, SingleTypeSerializer.ByteSerializer);

        put(Bytecode.class, DataType.BYTECODE, new ByteCodeSerializer());
    }

    public <T> TypeSerializerRegistry put(Class<T> type, DataType dataType, TypeSerializer<T> instance) {
        if (type == null) {
            throw new IllegalArgumentException("Type can not be null");
        }

        if (instance == null) {
            throw new IllegalArgumentException("Serializer instance can not be null");
        }

        if (!type.isInterface()) {
            // Direct class match
            serializers.put(type, instance);
        } else {
            // Interface can be assigned by provided type
            serializersByInterface.put(type, instance);
        }


        if (dataType != null) {
            serializersByDataType.put(dataType, instance);
        }

        return this;
    }

    public <T> TypeSerializer<T> getSerializer(Class<T> type) throws SerializationException {
        TypeSerializer<?> serializer = serializers.get(type);

        if (serializer == null) {
            // Find by interface
            for (Map.Entry<Class<?>, TypeSerializer<?>> entry : serializersByInterface.entrySet()) {
                if (entry.getKey().isAssignableFrom(type)) {
                    serializer = entry.getValue();
                    break;
                }
            }
        }

        return validateInstance(serializer, type.getTypeName());
    }

    public <T> TypeSerializer<T> getSerializer(DataType dataType) throws SerializationException {
        return validateInstance(serializersByDataType.get(dataType), dataType.toString());
    }

    private static TypeSerializer validateInstance(TypeSerializer serializer, String typeName) throws SerializationException {
        if (serializer == null) {
            throw new SerializationException(String.format("Serializer for type %s not found", typeName));
        }

        return serializer;
    }
}

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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class TypeSerializerRegistry {
    public static final TypeSerializerRegistry INSTANCE = new TypeSerializerRegistry();

    private final Map<Class<?>, TypeSerializer<?>> serializers = new HashMap<>();
    private final Map<DataType, TypeSerializer<?>> serializersByDataType = new HashMap<>();

    private TypeSerializerRegistry() {
        // No DataType as RequestMessage can't be fully qualified
        put(RequestMessage.class, null, new RequestMessageSerializer());

        put(String.class, DataType.STRING, new StringSerializer());
        put(UUID.class, DataType.UUID, new UUIDSerializer());
        put(HashMap.class, DataType.MAP, new MapSerializer());

        put(Integer.class, DataType.INT, SingleTypeSerializer.IntSerializer);
        put(Long.class, DataType.LONG, SingleTypeSerializer.LongSerializer);
        put(Double.class, DataType.DOUBLE, SingleTypeSerializer.DoubleSerializer);
        put(Float.class, DataType.FLOAT, SingleTypeSerializer.FloatSerializer);

        put(Bytecode.class, DataType.BYTECODE, new ByteCodeSerializer());
    }

    public <T> TypeSerializerRegistry put(Class<T> type, DataType dataType, TypeSerializer<T> instance) {
        serializers.put(type, instance);

        if (dataType != null) {
            serializersByDataType.put(dataType, instance);
        }

        return this;
    }

    public <T> TypeSerializer<T> getSerializer(Class<T> type) throws SerializationException {
        return validateInstance(serializers.get(type), type.getTypeName());
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

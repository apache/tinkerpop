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
package org.apache.tinkerpop.gremlin.driver.ser.binary.types;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.DataType;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;

import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Represents a serializer for types that be represented as a single value and that can be read and write
 * in a single operation.
 */
public class SingleTypeSerializer<T> extends SimpleTypeSerializer<T> {
    public static final SingleTypeSerializer<Integer> IntSerializer =
            new SingleTypeSerializer<>(4, DataType.INT, ByteBuf::readInt, (v, b) -> b.writeInt(v));
    public static final SingleTypeSerializer<Long> LongSerializer =
            new SingleTypeSerializer<>(8, DataType.LONG, ByteBuf::readLong, (v, b) -> b.writeLong(v));
    public static final SingleTypeSerializer<Double> DoubleSerializer =
            new SingleTypeSerializer<>(8, DataType.DOUBLE, ByteBuf::readDouble, (v, b) -> b.writeDouble(v));
    public static final SingleTypeSerializer<Float> FloatSerializer =
            new SingleTypeSerializer<>(4, DataType.FLOAT, ByteBuf::readFloat, (v, b) -> b.writeFloat(v));

    private final int byteLength;
    private final DataType dataType;
    private final Function<ByteBuf, T> readFunc;
    private final BiConsumer<T, ByteBuf> writeFunc;

    private SingleTypeSerializer(int byteLength, DataType dataType, Function<ByteBuf, T> readFunc,
                                 BiConsumer<T, ByteBuf> writeFunc) {
        this.byteLength = byteLength;
        this.dataType = dataType;
        this.readFunc = readFunc;
        this.writeFunc = writeFunc;
    }

    @Override
    public T readValue(ByteBuf buffer, GraphBinaryReader context) throws SerializationException {
        return readFunc.apply(buffer);
    }

    @Override
    DataType getDataType() {
        return dataType;
    }

    @Override
    public ByteBuf writeValueSequence(T value, ByteBufAllocator allocator, GraphBinaryWriter context) {
        ByteBuf buffer = allocator.buffer(byteLength);
        writeFunc.accept(value, buffer);
        return buffer;
    }
}

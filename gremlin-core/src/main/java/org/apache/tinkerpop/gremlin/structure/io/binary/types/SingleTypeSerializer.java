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
package org.apache.tinkerpop.gremlin.structure.io.binary.types;

import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.Marker;

import java.time.Year;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Represents a serializer for types that can be represented as a single value and that can be read and write
 * in a single operation.
 */
public class SingleTypeSerializer<T> extends SimpleTypeSerializer<T> {
    public static final SingleTypeSerializer<Integer> IntSerializer =
            new SingleTypeSerializer<>(DataType.INT, Buffer::readInt, (v, b) -> b.writeInt(v));
    public static final SingleTypeSerializer<Long> LongSerializer =
            new SingleTypeSerializer<>(DataType.LONG, Buffer::readLong, (v, b) -> b.writeLong(v));
    public static final SingleTypeSerializer<Double> DoubleSerializer =
            new SingleTypeSerializer<>(DataType.DOUBLE, Buffer::readDouble, (v, b) -> b.writeDouble(v));
    public static final SingleTypeSerializer<Float> FloatSerializer =
            new SingleTypeSerializer<>(DataType.FLOAT, Buffer::readFloat, (v, b) -> b.writeFloat(v));
    public static final SingleTypeSerializer<Short> ShortSerializer =
            new SingleTypeSerializer<>(DataType.SHORT, Buffer::readShort, (v, b) -> b.writeShort(v));
    public static final SingleTypeSerializer<Boolean> BooleanSerializer =
            new SingleTypeSerializer<>(DataType.BOOLEAN, Buffer::readBoolean, (v, b) -> b.writeBoolean(v));
    public static final SingleTypeSerializer<Byte> ByteSerializer =
            new SingleTypeSerializer<>(DataType.BYTE, Buffer::readByte, (v, b) -> b.writeByte(v));
    public static final SingleTypeSerializer<Marker> MarkerSerializer =
            new SingleTypeSerializer<>(DataType.MARKER, bb -> Marker.of(bb.readByte()), (v, b) -> b.writeByte(v.getValue()));

    private final Function<Buffer, T> readFunc;
    private final BiConsumer<T, Buffer> writeFunc;

    private SingleTypeSerializer(final DataType dataType, final Function<Buffer, T> readFunc,
                                 final BiConsumer<T, Buffer> writeFunc) {
        super(dataType);
        this.readFunc = readFunc;
        this.writeFunc = writeFunc;
    }

    @Override
    public T readValue(final Buffer buffer, final GraphBinaryReader context) {
        return readFunc.apply(buffer);
    }

    @Override
    protected void writeValue(final T value, final Buffer buffer, final GraphBinaryWriter context) {
        writeFunc.accept(value, buffer);
    }
}

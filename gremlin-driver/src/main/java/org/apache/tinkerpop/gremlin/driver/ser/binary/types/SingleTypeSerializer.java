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
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import java.util.function.Function;

public class SingleTypeSerializer<T> extends SimpleTypeSerializer<T> {
    public static final SingleTypeSerializer<Integer> IntSerializer = new SingleTypeSerializer<>(ByteBuf::readInt);
    public static final SingleTypeSerializer<Long> LongSerializer = new SingleTypeSerializer<>(ByteBuf::readLong);
    public static final SingleTypeSerializer<Double> DoubleSerializer = new SingleTypeSerializer<>(ByteBuf::readDouble);
    public static final SingleTypeSerializer<Float> FloatSerializer = new SingleTypeSerializer<>(ByteBuf::readFloat);

    private final Function<ByteBuf, T> func;


    public SingleTypeSerializer(Function<ByteBuf, T> func) {
        this.func = func;
    }

    @Override
    public T readValue(ByteBuf buffer, GraphBinaryReader context) throws SerializationException {
        return func.apply(buffer);
    }
}

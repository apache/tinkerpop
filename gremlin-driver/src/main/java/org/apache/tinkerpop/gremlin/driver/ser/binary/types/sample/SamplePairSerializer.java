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
package org.apache.tinkerpop.gremlin.driver.ser.binary.types.sample;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.DataType;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.driver.ser.binary.TypeSerializer;

import java.nio.charset.StandardCharsets;

class SamplePairSerializer implements TypeSerializer<SamplePair> {
    private final static byte[] dataTypeBuffer = new byte[] { DataType.CUSTOM.getCodeByte() };
    private final byte[] dataTypeNameBuffer = "SAMPLE_PAIR".getBytes(StandardCharsets.UTF_8);

    @Override
    public SamplePair read(ByteBuf buffer, GraphBinaryReader context) throws SerializationException {
        return null;
    }

    @Override
    public SamplePair readValue(ByteBuf buffer, GraphBinaryReader context, boolean nullable) throws SerializationException {
        throw new SerializationException("SamplePairSerializer can't read the value without type information");
    }

    @Override
    public ByteBuf write(SamplePair value, ByteBufAllocator allocator, GraphBinaryWriter context) throws SerializationException {

        ByteBuf valueBuffer = null;

        return allocator.compositeBuffer(3).addComponents(true,
                // Type code
                Unpooled.wrappedBuffer(dataTypeBuffer),
                // Custom type name
                Unpooled.wrappedBuffer(dataTypeNameBuffer),
                // No custom type info in this case
                // Value flag
                valueBuffer
        );
    }

    @Override
    public ByteBuf writeValue(SamplePair value, ByteBufAllocator allocator, GraphBinaryWriter context, boolean nullable) throws SerializationException {
        throw new SerializationException("SamplePairSerializer can't write the value without type information");
    }
}

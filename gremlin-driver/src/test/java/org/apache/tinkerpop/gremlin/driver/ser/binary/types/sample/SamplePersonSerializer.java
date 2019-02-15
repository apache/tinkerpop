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
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.DataType;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.CustomTypeSerializer;

import java.util.Date;

/**
 * A sample custom type serializer.
 */
public final class SamplePersonSerializer implements CustomTypeSerializer<SamplePerson> {
    private final byte[] typeInfoBuffer = new byte[] { 0, 0, 0, 0 };

    @Override
    public String getTypeName() {
        return "sampleProvider.SamplePerson";
    }

    @Override
    public DataType getDataType() {
        return DataType.CUSTOM;
    }

    @Override
    public SamplePerson read(final ByteBuf buffer, final GraphBinaryReader context) throws SerializationException {
        // {custom type info}, {value_flag} and {value}
        // No custom_type_info
        if (buffer.readInt() != 0) {
            throw new SerializationException("{custom_type_info} should not be provided for this custom type");
        }

        final byte valueFlag = buffer.readByte();
        if ((valueFlag & 1) == 1) {
            return null;
        }

        // Read the buffer int, no necessary in this case
        buffer.readInt();

        final String name = context.readValue(buffer, String.class, false);
        final Date birthDate = context.readValue(buffer, Date.class, false);

        return new SamplePerson(name, birthDate);
    }

    @Override
    public SamplePerson readValue(final ByteBuf buffer, final GraphBinaryReader context, final boolean nullable) throws SerializationException {
        throw new SerializationException("SamplePersonSerializer can not read a value without type information");
    }

    @Override
    public void write(final SamplePerson value, final ByteBuf buffer, final GraphBinaryWriter context) throws SerializationException {
        if (value == null) {
            buffer.writeBytes(typeInfoBuffer);
            context.writeValueFlagNull(buffer);
            return;
        }

        buffer.writeBytes(typeInfoBuffer);
        context.writeValueFlagNone(buffer);

        final ByteBuf valueBuffer = buffer.alloc().buffer();
        try {
            context.writeValue(value.getName(), valueBuffer, false);
            context.writeValue(value.getBirthDate(), valueBuffer, false);

            buffer.writeInt(valueBuffer.readableBytes());
            buffer.writeBytes(valueBuffer);
        } finally {
            valueBuffer.release();
        }
    }

    @Override
    public void writeValue(final SamplePerson value, final ByteBuf buffer, final GraphBinaryWriter context, final boolean nullable) throws SerializationException {
        throw new SerializationException("SamplePersonSerializer can not write a value without type information");
    }
}

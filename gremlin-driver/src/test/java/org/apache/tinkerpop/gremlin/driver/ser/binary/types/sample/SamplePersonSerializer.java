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

import java.nio.charset.StandardCharsets;
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

        return readValue(buffer, context, true);
    }

    @Override
    public SamplePerson readValue(final ByteBuf buffer, final GraphBinaryReader context, final boolean nullable) throws SerializationException {
        if (nullable) {
            final byte valueFlag = buffer.readByte();
            if ((valueFlag & 1) == 1) {
                return null;
            }
        }

        // Read the byte length of the value bytes
        final int valueLength = buffer.readInt();

        if (valueLength <= 0) {
            throw new SerializationException(String.format("Unexpected value length: %d", valueLength));
        }

        if (valueLength > buffer.readableBytes()) {
            throw new SerializationException(
                String.format("Not enough readable bytes: %d (expected %d)", valueLength, buffer.readableBytes()));
        }

        final String name = context.readValue(buffer, String.class, false);
        final Date birthDate = context.readValue(buffer, Date.class, false);

        return new SamplePerson(name, birthDate);
    }

    @Override
    public void write(final SamplePerson value, final ByteBuf buffer, final GraphBinaryWriter context) throws SerializationException {
        // Write {custom type info}, {value_flag} and {value}
        buffer.writeBytes(typeInfoBuffer);

        writeValue(value, buffer, context, true);
    }

    @Override
    public void writeValue(final SamplePerson value, final ByteBuf buffer, final GraphBinaryWriter context, final boolean nullable) throws SerializationException {
        if (value == null) {
            if (!nullable) {
                throw new SerializationException("Unexpected null value when nullable is false");
            }

            context.writeValueFlagNull(buffer);
            return;
        }

        if (nullable) {
            context.writeValueFlagNone(buffer);
        }

        final String name = value.getName();

        // value_length = name_byte_length + name_bytes + long
        buffer.writeInt(4 + name.getBytes(StandardCharsets.UTF_8).length + 8);

        context.writeValue(name, buffer, false);
        context.writeValue(value.getBirthDate(), buffer, false);
    }
}

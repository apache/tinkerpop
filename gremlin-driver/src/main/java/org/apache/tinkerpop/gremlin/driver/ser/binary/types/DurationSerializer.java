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

import java.time.Duration;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DurationSerializer extends SimpleTypeSerializer<Duration> {
    public DurationSerializer() {
        super(DataType.DURATION);
    }

    @Override
    protected Duration readValue(final ByteBuf buffer, final GraphBinaryReader context) throws SerializationException {
        return Duration.ofSeconds(buffer.readLong(), buffer.readInt());
    }

    @Override
    protected ByteBuf writeValue(final Duration value, final ByteBufAllocator allocator, final GraphBinaryWriter context) throws SerializationException {
        return allocator.buffer(12).writeLong(value.getSeconds()).writeInt(value.getNano());
    }
}

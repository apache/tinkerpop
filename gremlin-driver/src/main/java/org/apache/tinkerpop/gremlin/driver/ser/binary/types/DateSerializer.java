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

import java.util.Date;

public class DateSerializer extends SimpleTypeSerializer<Date> {
    private final DataType dataType;

    public DateSerializer() {
        this(DataType.DATE);
    }

    public DateSerializer(DataType type) {
        this.dataType = type;
    }

    @Override
    DataType getDataType() {
        return dataType;
    }

    @Override
    Date readValue(ByteBuf buffer, GraphBinaryReader context) {
        return new Date(buffer.readLong());
    }

    @Override
    public ByteBuf writeValueSequence(Date value, ByteBufAllocator allocator, GraphBinaryWriter context) {
        return allocator.buffer(8).writeLong(value.getTime());
    }
}

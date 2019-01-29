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

import java.time.YearMonth;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class YearMonthSerializer extends SimpleTypeSerializer<YearMonth> {
    public YearMonthSerializer() {
        super(DataType.YEARMONTH);
    }

    @Override
    protected YearMonth readValue(final ByteBuf buffer, final GraphBinaryReader context) throws SerializationException {
        return YearMonth.of(buffer.readInt(), buffer.readByte());
    }

    @Override
    protected ByteBuf writeValue(final YearMonth value, final ByteBufAllocator allocator, final GraphBinaryWriter context) throws SerializationException {
        return allocator.buffer(2).writeInt(value.getYear()).writeByte(value.getMonthValue());
    }
}

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

import java.util.List;

public class ListSerializer extends SimpleTypeSerializer<List> {
    private static final CollectionSerializer collectionSerializer = new CollectionSerializer(DataType.LIST);

    public ListSerializer() {
        super(DataType.LIST);
    }

    @Override
    public List readValue(ByteBuf buffer, GraphBinaryReader context) throws SerializationException {
        // The collection is a List<>
        return (List) collectionSerializer.readValue(buffer, context);
    }

    @Override
    public ByteBuf writeValueSequence(List value, ByteBufAllocator allocator, GraphBinaryWriter context) throws SerializationException {
        return collectionSerializer.writeValueSequence(value, allocator, context);
    }
}

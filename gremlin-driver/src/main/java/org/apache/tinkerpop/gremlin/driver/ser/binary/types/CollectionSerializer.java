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
import io.netty.buffer.CompositeByteBuf;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.DataType;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;

import java.util.ArrayList;
import java.util.Collection;

class CollectionSerializer extends SimpleTypeSerializer<Collection> {
    public CollectionSerializer(DataType dataType) {
        super(dataType);
    }

    @Override
    Collection readValue(ByteBuf buffer, GraphBinaryReader context) throws SerializationException {
        final int length = buffer.readInt();

        ArrayList result = new ArrayList(length);
        for (int i = 0; i < length; i++) {
            result.add(context.read(buffer));
        }

        return result;
    }

    @Override
    public ByteBuf writeValueSequence(Collection value, ByteBufAllocator allocator, GraphBinaryWriter context) throws SerializationException {
        CompositeByteBuf result = allocator.compositeBuffer(1 + value.size());
        result.addComponent(true, allocator.buffer(4).writeInt(value.size()));

        for (Object item : value) {
            result.addComponent(
                    true,
                    context.write(item, allocator));
        }

        return result;
    }
}

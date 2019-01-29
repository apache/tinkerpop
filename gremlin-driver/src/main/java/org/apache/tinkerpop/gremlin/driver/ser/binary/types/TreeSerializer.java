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
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;

public class TreeSerializer extends SimpleTypeSerializer<Tree> {
    public TreeSerializer() {
        super(DataType.TREE);
    }

    @Override
    protected Tree readValue(final ByteBuf buffer, final GraphBinaryReader context) throws SerializationException {
        final int length = buffer.readInt();

        final Tree result = new Tree();
        for (int i = 0; i < length; i++) {
            result.put(context.read(buffer), context.readValue(buffer, Tree.class, false));
        }

        return result;
    }

    @Override
    protected ByteBuf writeValue(final Tree value, final ByteBufAllocator allocator, final GraphBinaryWriter context) throws SerializationException {
        final CompositeByteBuf result = allocator.compositeBuffer(1 + value.size() * 2);
        result.addComponent(true, allocator.buffer(4).writeInt(value.size()));

        for (Object key : value.keySet()) {
            result.addComponents(
                    true,
                    context.write(key, allocator),
                    context.writeValue(value.get(key), allocator, false));
        }

        return result;
    }
}

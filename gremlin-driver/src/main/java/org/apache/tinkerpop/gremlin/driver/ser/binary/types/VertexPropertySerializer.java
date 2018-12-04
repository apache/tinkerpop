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
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class VertexPropertySerializer extends SimpleTypeSerializer<VertexProperty> {

    public VertexPropertySerializer() {
        super(DataType.VERTEXPROPERTY);
    }

    @Override
    VertexProperty readValue(final ByteBuf buffer, final GraphBinaryReader context) throws SerializationException {
        final VertexProperty v = new ReferenceVertexProperty<>(context.read(buffer),
                context.readValue(buffer, String.class, false),
                context.read(buffer));

        // discard the parent vertex - we only send "references"
        context.read(buffer);

        // discard the properties - as we only send "references" this should always be null, but will we change our
        // minds some day????
        context.read(buffer);
        return v;
    }

    @Override
    public ByteBuf writeValue(final VertexProperty value, final ByteBufAllocator allocator, final GraphBinaryWriter context) throws SerializationException {
        final CompositeByteBuf result = allocator.compositeBuffer(5);
        result.addComponent(true, context.write(value.id(), allocator));
        result.addComponent(true, context.writeValue(value.label(), allocator, false));
        result.addComponent(true, context.write(value.value(), allocator));

        // we don't serialize the parent vertex even as a "reference", but, let's hold a place for it
        result.addComponent(true, context.write(null, allocator));
        // we don't serialize properties for graph elements. they are "references", but we leave a place holder
        // here as an option for the future as we've waffled this soooooooooo many times now
        result.addComponent(true, context.write(null, allocator));

        return result;
    }
}

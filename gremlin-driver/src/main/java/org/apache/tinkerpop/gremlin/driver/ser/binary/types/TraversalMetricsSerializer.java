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
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TraversalMetricsSerializer extends SimpleTypeSerializer<TraversalMetrics> {
    private static final CollectionSerializer collectionSerializer = new CollectionSerializer(DataType.LIST);

    public TraversalMetricsSerializer() {
        super(DataType.TRAVERSALMETRICS);
    }

    @Override
    protected TraversalMetrics readValue(ByteBuf buffer, GraphBinaryReader context) throws SerializationException {
        Long durationNanos = context.readValue(buffer, Long.class, false);
        final List<MutableMetrics> metrics = new ArrayList<>(collectionSerializer.readValue(buffer, context));
        return new DefaultTraversalMetrics(durationNanos, metrics);
    }

    @Override
    protected ByteBuf writeValue(TraversalMetrics value, ByteBufAllocator allocator, GraphBinaryWriter context) throws SerializationException {
        final CompositeByteBuf result = allocator.compositeBuffer(2);

        try {
            result.addComponent(true, context.writeValue(value.getDuration(TimeUnit.NANOSECONDS), allocator, false));
            result.addComponent(true, collectionSerializer.writeValue(value.getMetrics(), allocator, context));
        } catch (Exception ex) {
            // We should release it as the ByteBuf is not going to be yielded for a reader
            result.release();
            throw ex;
        }

        return result;
    }
}

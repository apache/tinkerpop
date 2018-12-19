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
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MetricsSerializer extends SimpleTypeSerializer<Metrics> {
    private static final CollectionSerializer collectionSerializer = new CollectionSerializer(DataType.LIST);

    public MetricsSerializer() {
        super(DataType.METRICS);
    }

    @Override
    Metrics readValue(ByteBuf buffer, GraphBinaryReader context) throws SerializationException {
        // Consider using a custom implementation, like "DefaultMetrics"
        final MutableMetrics result = new MutableMetrics(
                context.readValue(buffer, String.class, false),
                context.readValue(buffer, String.class, false));
        result.setDuration(context.readValue(buffer, Long.class, false), TimeUnit.NANOSECONDS);
        final Map<String, Long> counts = context.readValue(buffer, Map.class, false);
        counts.forEach(result::setCount);
        final Map<String, Object> annotations = context.readValue(buffer, Map.class, false);
        annotations.forEach(result::setAnnotation);
        Collection<MutableMetrics> nestedMetrics = collectionSerializer.readValue(buffer, context);
        nestedMetrics.forEach(result::addNested);
        return result;
    }

    @Override
    public ByteBuf writeValue(Metrics value, ByteBufAllocator allocator, GraphBinaryWriter context) throws SerializationException {
        return allocator.compositeBuffer(6).addComponents(true,
                context.writeValue(value.getId(), allocator, false),
                context.writeValue(value.getName(), allocator, false),
                context.writeValue(value.getDuration(TimeUnit.NANOSECONDS), allocator, false),
                context.writeValue(value.getCounts(), allocator, false),
                context.writeValue(value.getAnnotations(), allocator, false),
                // Avoid changing type to List
                collectionSerializer.writeValue(value.getNested(), allocator, context));
    }
}

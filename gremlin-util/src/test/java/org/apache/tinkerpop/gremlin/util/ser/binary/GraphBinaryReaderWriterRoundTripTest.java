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
package org.apache.tinkerpop.gremlin.util.ser.binary;

import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.util.ser.AbstractRoundTripTest;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

public class GraphBinaryReaderWriterRoundTripTest extends AbstractRoundTripTest {
    private final GraphBinaryWriter writer = new GraphBinaryWriter();
    private final GraphBinaryReader reader = new GraphBinaryReader();
    private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private static NettyBufferFactory bufferFactory = new NettyBufferFactory();

    private static List<String> skippedTests
            = Arrays.asList("ReferenceVertexProperty");

    @Test
    public void shouldWriteAndRead() throws Exception {
        // some tests are not valid for graphbinary
        if (skippedTests.contains(name)) return;

        // Test it multiple times as the type registry might change its internal state
        for (int i = 0; i < 5; i++) {
            final Buffer buffer = bufferFactory.create(allocator.buffer());
            writer.write(value, buffer);
            buffer.readerIndex(0);
            final Object result = reader.read(buffer);

            Optional.ofNullable(assertion).orElse((Consumer) r -> assertEquals(value, r)).accept(result);
        }
    }
}

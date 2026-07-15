/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.util.ser.binary;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.io.pdt.CompositePDT;
import org.apache.tinkerpop.gremlin.structure.io.pdt.PDTRegistry;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Validates that the default {@link GraphBinaryMessageSerializerV4} auto-wires SPI-discovered
 * {@link org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedTypeAdapter} implementations
 * for hydration without any user configuration.
 */
public class GraphBinaryPdtSpiAutoWiringTest {

    private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

    @Test
    public void shouldAutoHydrateSpiDiscoveredAdapterWithDefaultSerializer() throws SerializationException {
        // The default no-arg constructor should pick up TestPointAdapter via SPI
        final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();

        // Build a response containing a raw CompositePDT matching the test adapter's typeName
        final Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("x", 10);
        fields.put("y", 20);
        final CompositePDT pdt = new CompositePDT("test.Point", fields);

        final ResponseMessage response = ResponseMessage.build()
                .result(Collections.singletonList(pdt))
                .create();

        // Serialize and deserialize — the reader should hydrate the PDT into TestPoint
        final ByteBuf buffer = serializer.writeHeader(response, allocator);
        try {
            final ResponseMessage deserialized = serializer.readChunk(buffer, true);
            assertNotNull(deserialized.getResult().getData());
            assertEquals(1, deserialized.getResult().getData().size());

            final Object result = deserialized.getResult().getData().get(0);
            assertTrue("Expected hydrated TestPoint but got: " + result.getClass().getName(),
                    result instanceof TestPointAdapter.TestPoint);

            final TestPointAdapter.TestPoint point = (TestPointAdapter.TestPoint) result;
            assertEquals(10, point.x);
            assertEquals(20, point.y);
        } finally {
            buffer.release();
        }
    }

    @Test
    public void shouldAutoHydrateSpiDiscoveredAdapterAfterConfigure() throws SerializationException {
        // Verify configure() also includes the SPI registry
        final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();
        serializer.configure(Collections.emptyMap(), Collections.emptyMap());

        final Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("x", 5);
        fields.put("y", 15);
        final CompositePDT pdt = new CompositePDT("test.Point", fields);

        final ResponseMessage response = ResponseMessage.build()
                .result(Collections.singletonList(pdt))
                .create();

        final ByteBuf buffer = serializer.writeHeader(response, allocator);
        try {
            final ResponseMessage deserialized = serializer.readChunk(buffer, true);
            final Object result = deserialized.getResult().getData().get(0);
            assertTrue("Expected hydrated TestPoint after configure()", result instanceof TestPointAdapter.TestPoint);
        } finally {
            buffer.release();
        }
    }

    @Test
    public void shouldUseExplicitPdtRegistryOverSpiDefault() throws SerializationException {
        // An empty registry should NOT hydrate since it has no adapters registered
        final PDTRegistry emptyRegistry = PDTRegistry.empty();
        final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4(
                TypeSerializerRegistry.INSTANCE, emptyRegistry);

        final Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("x", 1);
        fields.put("y", 2);
        final CompositePDT pdt = new CompositePDT("test.Point", fields);

        final ResponseMessage response = ResponseMessage.build()
                .result(Collections.singletonList(pdt))
                .create();

        final ByteBuf buffer = serializer.writeHeader(response, allocator);
        try {
            final ResponseMessage deserialized = serializer.readChunk(buffer, true);
            final Object result = deserialized.getResult().getData().get(0);
            // With empty registry, should remain as raw CompositePDT (no hydration)
            assertTrue("Expected raw CompositePDT with explicit empty registry but got: " + result.getClass().getName(),
                    result instanceof CompositePDT);
        } finally {
            buffer.release();
        }
    }
}

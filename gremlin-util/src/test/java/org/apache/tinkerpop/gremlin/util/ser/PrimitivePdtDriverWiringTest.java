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
package org.apache.tinkerpop.gremlin.util.ser;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV4;
import org.apache.tinkerpop.gremlin.structure.io.pdt.PrimitivePDTAdapter;
import org.apache.tinkerpop.gremlin.structure.io.pdt.PrimitivePDT;
import org.apache.tinkerpop.gremlin.structure.io.pdt.PDTRegistry;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that PrimitivePDT round-trips through the driver message serializer stack when a
 * {@link PrimitivePDTAdapter} is registered in the {@link PDTRegistry} shared
 * between the writer (request dehydration) and reader (response hydration).
 */
public class PrimitivePdtDriverWiringTest {

    private static final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

    // A simple domain type not known to the serializer natively
    static final class Uint32 {
        final long value;
        Uint32(final long value) { this.value = value; }
    }

    static final class Uint32Adapter implements PrimitivePDTAdapter<Uint32> {
        @Override public String typeName() { return "Uint32"; }
        @Override public Class<Uint32> targetClass() { return Uint32.class; }
        @Override public String toValue(final Uint32 obj) { return Long.toString(obj.value); }
        @Override public Uint32 fromValue(final String value) { return new Uint32(Long.parseLong(value)); }
    }

    /**
     * Proves a raw adapter-registered object (Uint32) round-trips through the GraphBinary message
     * serializer: writer dehydrates it to PrimitivePDT on the request path, reader hydrates it
     * back on the response path.
     */
    @Test
    public void shouldRoundTripPrimitivePdtThroughGraphBinaryMessageSerializer() throws SerializationException {
        final PDTRegistry pdtRegistry = PDTRegistry.empty();
        pdtRegistry.register(new Uint32Adapter());

        final GraphBinaryMessageSerializerV4 serializer =
                new GraphBinaryMessageSerializerV4(TypeSerializerRegistry.INSTANCE, pdtRegistry);

        // Simulate a response containing a raw Uint32 object (server-side dehydration)
        final Uint32 original = new Uint32(42L);
        final ResponseMessage response = ResponseMessage.build()
                .code(HttpResponseStatus.OK)
                .result(Arrays.asList(original))
                .create();

        final ByteBuf buffer = serializer.serializeResponseAsBinary(response, allocator);
        final ResponseMessage deserialized = serializer.deserializeBinaryResponse(buffer);

        final List<?> data = deserialized.getResult().getData();
        assertNotNull(data);
        assertEquals(1, data.size());
        assertTrue("Expected Uint32 but got " + data.get(0).getClass().getName(),
                data.get(0) instanceof Uint32);
        assertEquals(42L, ((Uint32) data.get(0)).value);
    }

    /**
     * Proves a raw {@link PrimitivePDT} round-trips through GraphBinary without
     * requiring an adapter (the PDT itself is natively serializable).
     */
    @Test
    public void shouldRoundTripRawPrimitivePDTThroughGraphBinary() throws SerializationException {
        final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();

        final PrimitivePDT pdt = new PrimitivePDT("CustomId", "abc-123");
        final ResponseMessage response = ResponseMessage.build()
                .code(HttpResponseStatus.OK)
                .result(Arrays.asList(pdt))
                .create();

        final ByteBuf buffer = serializer.serializeResponseAsBinary(response, allocator);
        final ResponseMessage deserialized = serializer.deserializeBinaryResponse(buffer);

        final List<?> data = deserialized.getResult().getData();
        assertNotNull(data);
        assertEquals(1, data.size());
        assertTrue(data.get(0) instanceof PrimitivePDT);
        assertEquals(pdt, data.get(0));
    }

    /**
     * Proves a raw adapter-registered object hydrates correctly through GraphSON response
     * deserialization when the registry is configured on the mapper.
     */
    @Test
    public void shouldHydratePrimitivePdtThroughGraphSONResponsePath() throws SerializationException {
        final PDTRegistry pdtRegistry = PDTRegistry.empty();
        pdtRegistry.register(new Uint32Adapter());

        final GraphSONMapper.Builder mapperBuilder = GraphSONMapper.build()
                .version(GraphSONVersion.V4_0)
                .addCustomModule(GraphSONXModuleV4.build())
                .pdtRegistry(pdtRegistry);

        final GraphSONMessageSerializerV4 serializer = new GraphSONMessageSerializerV4(mapperBuilder);

        // Serialize a response containing a PrimitivePDT (as the server would send)
        final PrimitivePDT pdt = new PrimitivePDT("Uint32", "99");
        final ResponseMessage response = ResponseMessage.build()
                .code(HttpResponseStatus.OK)
                .result(Arrays.asList(pdt))
                .create();

        final ByteBuf buffer = serializer.serializeResponseAsBinary(response, allocator);
        final ResponseMessage deserialized = serializer.deserializeBinaryResponse(buffer);

        final List<?> data = deserialized.getResult().getData();
        assertNotNull(data);
        assertEquals(1, data.size());
        // GraphSON deserializer returns PrimitivePDT with hydrated object attached
        assertTrue("Expected PrimitivePDT but got " + data.get(0).getClass().getName(),
                data.get(0) instanceof PrimitivePDT);
        final PrimitivePDT result = (PrimitivePDT) data.get(0);
        assertNotNull("Hydrated object should be attached", result.getHydrated());
        assertTrue(result.getHydrated() instanceof Uint32);
        assertEquals(99L, ((Uint32) result.getHydrated()).value);
    }

    /**
     * Proves that the same registry used for GraphBinary write (dehydration) is also used for read
     * (hydration), verifying the shared-registry contract at the message serializer level.
     */
    @Test
    public void shouldDehydrateAndHydrateRawObjectThroughGraphBinaryRequestResponseCycle() throws SerializationException {
        final PDTRegistry pdtRegistry = PDTRegistry.empty();
        pdtRegistry.register(new Uint32Adapter());

        final GraphBinaryMessageSerializerV4 serializer =
                new GraphBinaryMessageSerializerV4(TypeSerializerRegistry.INSTANCE, pdtRegistry);

        // Simulate: server sends response containing the dehydrated form of Uint32(7)
        // In real flow: server writes PrimitivePDT("Uint32","7"); client reads + hydrates
        final PrimitivePDT wireForm = new PrimitivePDT("Uint32", "7");
        final ResponseMessage response = ResponseMessage.build()
                .code(HttpResponseStatus.OK)
                .result(Arrays.asList(wireForm))
                .create();

        final ByteBuf buffer = serializer.serializeResponseAsBinary(response, allocator);
        final ResponseMessage deserialized = serializer.deserializeBinaryResponse(buffer);

        final List<?> data = deserialized.getResult().getData();
        assertEquals(1, data.size());
        // With the adapter registered, the reader hydrates PrimitivePDT back to Uint32
        assertTrue("Expected Uint32 but got " + data.get(0).getClass().getName(),
                data.get(0) instanceof Uint32);
        assertEquals(7L, ((Uint32) data.get(0)).value);
    }
}

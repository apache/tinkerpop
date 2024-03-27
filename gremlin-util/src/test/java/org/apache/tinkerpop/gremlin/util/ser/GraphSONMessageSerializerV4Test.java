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
package org.apache.tinkerpop.gremlin.util.ser;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Serializer tests that cover non-lossy serialization/deserialization methods.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@SuppressWarnings("unchecked")
public class GraphSONMessageSerializerV4Test extends GraphSONMessageSerializerV3Test {

    public final GraphSONMessageSerializerV4 serializer = new GraphSONMessageSerializerV4();

    @Test
    public void shouldDeserializeChunkedResponseMessage() throws SerializationException {
        final UUID id = UUID.randomUUID();
        final ResponseMessage response = ResponseMessage.build(id)
                .code(ResponseStatusCode.SUCCESS)
                .result(Arrays.asList("start/end", 0))
                .statusMessage("OK")
                .create();

        final ByteBuf bb0 = serializer.writeHeader(response, allocator);
        final ByteBuf bb1 = serializer.writeChunk(Arrays.asList("chunk", 1), allocator);
        final ByteBuf bb2 = serializer.writeChunk(Arrays.asList("chunk", 2), allocator);
        final ByteBuf bb3 = serializer.writeFooter(response, allocator);

        final ByteBuf bbCombined = allocator.buffer().writeBytes(bb0).writeBytes(bb1).writeBytes(bb2).writeBytes(bb3);

        final ResponseMessage deserialized = serializer.deserializeResponse(bbCombined);

        assertEquals(8, ((List)deserialized.getResult().getData()).size());
        assertEquals(200, deserialized.getStatus().getCode().getValue());
        assertEquals("OK", deserialized.getStatus().getMessage());
    }

    @Test
    public void shouldDeserializeChunkedResponseMessageWithError() throws SerializationException {
        final UUID id = UUID.randomUUID();
        final ResponseMessage response = ResponseMessage.build(id)
                .code(ResponseStatusCode.SUCCESS)
                .result(Arrays.asList("start/end", 0))
                .statusMessage("OK")
                .create();

        final ByteBuf bb0 = serializer.writeHeader(response, allocator);
        final ByteBuf bb1 = serializer.writeChunk(Arrays.asList("chunk", 1), allocator);
        final ByteBuf bb2 = serializer.writeChunk(Arrays.asList("chunk", 2), allocator);
        final ByteBuf bb3 = serializer.writeErrorFooter(response, allocator);

        final ByteBuf bbCombined = allocator.buffer().writeBytes(bb0).writeBytes(bb1).writeBytes(bb2).writeBytes(bb3);

        final ResponseMessage deserialized = serializer.deserializeResponse(bbCombined);

        // 3 chunks without errors, 2 items in each
        assertEquals(6, ((List)deserialized.getResult().getData()).size());
        // error description is in trailing headers
        assertEquals(200, deserialized.getStatus().getCode().getValue());
        assertEquals("OK", deserialized.getStatus().getMessage());
    }

    @Override
    protected ResponseMessage convert(final Object toSerialize) throws SerializationException {
        return convert(toSerialize, this.serializer);
    }
}

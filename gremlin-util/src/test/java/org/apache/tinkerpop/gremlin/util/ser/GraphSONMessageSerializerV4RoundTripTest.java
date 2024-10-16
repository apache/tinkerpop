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
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

public class GraphSONMessageSerializerV4RoundTripTest extends AbstractRoundTripTest {
    private final ResponseMessage.Builder responseMessageBuilder = ResponseMessage.build();
    private final static ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;

    public final GraphSONMessageSerializerV4 serializer = new GraphSONMessageSerializerV4();

    private static final List<String> skippedTests
            = Arrays.asList("ReferenceVertex", "ReferenceVertexProperty", "ReferenceProperty", "Graph", "BulkList"); // enable BulkList after implemented

    @Test
    public void shouldWriteAndRead() throws Exception {
        // some tests are not valid for json
        if (skippedTests.contains(name)) return;

        for (int i = 0; i < 5; i++) {
            // GraphSONv4 assumes that results are always in a list.
            final ByteBuf bb = serializer.serializeResponseAsBinary(
                    responseMessageBuilder.result(Collections.singletonList(value)).code(HttpResponseStatus.OK).create(), allocator);
            final Object result = serializer.deserializeBinaryResponse(bb).getResult().getData().get(0);

            Optional.ofNullable(assertion).orElse((Consumer) r -> assertEquals(value, r)).accept(result);
        }
    }
}

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
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReadLimits;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class GraphBinaryMessageSerializerV4ReadLimitsTest {

    private static final GraphBinaryReadLimits LOW_LIMITS = GraphBinaryReadLimits.of(50, 50, 50, 50);
    private static final String LONG_VALUE = String.join("", Collections.nCopies(100, "x"));

    private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

    private ByteBuf responseWithLongValue(final GraphBinaryMessageSerializerV4 serializer) throws SerializationException {
        return serializer.serializeResponseAsBinary(ResponseMessage.build().code(HttpResponseStatus.OK)
                .result(Collections.singletonList(LONG_VALUE)).create(), allocator);
    }

    @Test
    public void shouldUseDefaultLimitsUnlessSet() {
        assertSame(GraphBinaryReadLimits.DEFAULT, new GraphBinaryMessageSerializerV4().getMapper().getReader().getLimits());
    }

    @Test
    public void shouldExposeLimitsThroughTheMapper() {
        final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();
        final Object originalMapper = serializer.getMapper();

        serializer.setReadLimits(LOW_LIMITS);

        assertNotSame(originalMapper, serializer.getMapper());
        assertSame(LOW_LIMITS, serializer.getMapper().getReader().getLimits());
    }

    @Test
    public void shouldDeserializeResponseWithLongValueByDefault() throws SerializationException {
        final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();
        final ByteBuf buffer = responseWithLongValue(serializer);
        try {
            assertEquals(LONG_VALUE, serializer.deserializeBinaryResponse(buffer).getResult().getData().get(0));
        } finally {
            buffer.release();
        }
    }

    @Test
    public void shouldRejectResponseWithValueAboveConfiguredLimit() throws SerializationException {
        final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();
        serializer.setReadLimits(LOW_LIMITS);
        final ByteBuf buffer = responseWithLongValue(serializer);
        try {
            serializer.deserializeBinaryResponse(buffer);
            fail("a value longer than the configured limit must be refused");
        } catch (SerializationException expected) {
        } finally {
            buffer.release();
        }
    }

    @Test
    public void shouldRejectRequestWithValueAboveConfiguredLimit() throws SerializationException {
        final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();
        serializer.setReadLimits(LOW_LIMITS);
        final ByteBuf buffer = serializer.serializeRequestAsBinary(
                RequestMessage.build("g.inject('" + LONG_VALUE + "')").create(), allocator);
        try {
            serializer.deserializeBinaryRequest(buffer);
            fail("a request value longer than the configured limit must be refused");
        } catch (SerializationException expected) {
        } finally {
            buffer.release();
        }
    }

    private ByteBuf requestWithBulkedListField(final long bulk) {
        final ByteBuf buf = allocator.buffer();
        buf.writeByte(GraphBinaryWriter.VERSION_BYTE);
        buf.writeInt(1);
        buf.writeByte(DataType.STRING.getCodeByte());
        buf.writeByte(0);
        buf.writeInt(1);
        buf.writeByte('k');
        buf.writeByte(DataType.LIST.getCodeByte());
        buf.writeByte(2);
        buf.writeInt(1);
        buf.writeByte(DataType.INT.getCodeByte());
        buf.writeByte(0);
        buf.writeInt(42);
        buf.writeLong(bulk);
        buf.writeInt(1);
        buf.writeByte('g');
        return buf;
    }

    @Test
    public void shouldRejectRequestCarryingHugeBulkedListWithDefaultLimits() {
        final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();
        final ByteBuf buffer = requestWithBulkedListField(Long.MAX_VALUE);
        try {
            serializer.deserializeBinaryRequest(buffer);
            fail("a request carrying a bulked list that expands beyond the limit must be refused");
        } catch (SerializationException expected) {
            assertThat(expected.getCause().getMessage(), containsString("maxGraphBinaryBulkedListLength"));
        } finally {
            buffer.release();
        }
    }

    @Test
    public void shouldKeepLimitsWhenConfigured() throws SerializationException {
        final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();
        serializer.setReadLimits(LOW_LIMITS);
        serializer.configure(Collections.emptyMap(), null);

        final ByteBuf buffer = responseWithLongValue(serializer);
        try {
            serializer.deserializeBinaryResponse(buffer);
            fail("limits set before configure() must still apply afterwards");
        } catch (SerializationException expected) {
        } finally {
            buffer.release();
        }
    }

    @Test
    public void shouldApplyLimitsSetAfterConfigure() throws SerializationException {
        final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();
        serializer.configure(Collections.emptyMap(), null);
        serializer.setReadLimits(LOW_LIMITS);

        assertSame(LOW_LIMITS, serializer.getMapper().getReader().getLimits());
        final ByteBuf buffer = responseWithLongValue(serializer);
        try {
            serializer.deserializeBinaryResponse(buffer);
            fail("limits set after configure() must apply");
        } catch (SerializationException expected) {
        } finally {
            buffer.release();
        }
    }
}

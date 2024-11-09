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
package org.apache.tinkerpop.gremlin.server.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.CharsetUtil;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.MessageTextSerializer;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * A highly use-case specific serializer that only has context for HTTP where results simply need to be converted
 * to string in a line by line fashion for text based returns.
 */
public class TextPlainMessageSerializer implements MessageTextSerializer<Function<Object, String>> {

    @Override
    public Function<Object, String> getMapper() {
        return Objects::toString;
    }

    @Override
    public ByteBuf serializeResponseAsBinary(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        final String payload = serializeResponseAsString(responseMessage, allocator);
        final ByteBuf encodedMessage = allocator.buffer(payload.length());
        encodedMessage.writeCharSequence(payload, CharsetUtil.UTF_8);

        return encodedMessage;
    }

    @Override
    public ByteBuf serializeRequestAsBinary(final RequestMessage requestMessage, final ByteBufAllocator allocator) throws SerializationException {
        throw new UnsupportedOperationException("text/plain does not produce binary");
    }

    @Override
    public RequestMessage deserializeRequest(final ByteBuf msg) throws SerializationException {
        throw new UnsupportedOperationException("text/plain does not have deserialization functions");
    }

    @Override
    public ResponseMessage deserializeResponse(final ByteBuf msg) throws SerializationException {
        throw new UnsupportedOperationException("text/plain does not have deserialization functions");
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[] { "text/plain" };
    }

    @Override
    public String serializeResponseAsString(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        final StringBuilder sb = new StringBuilder();

        // this should only serialize success conditions so all should have data in List form
        final List<Object> data = (List<Object>) responseMessage.getResult().getData();
        for (int ix = 0; ix < data.size(); ix ++) {
            sb.append("==>");
            sb.append(data.get(ix));
            if (ix < data.size() - 1)
                sb.append(System.lineSeparator());
        }
        return sb.toString();
    }

    @Override
    public String serializeRequestAsString(final RequestMessage requestMessage, final ByteBufAllocator allocator) throws SerializationException {
        throw new UnsupportedOperationException("text/plain does not have any need to serialize requests");
    }

    @Override
    public RequestMessage deserializeRequest(final String msg) throws SerializationException {
        throw new UnsupportedOperationException("text/plain does not have deserialization functions");
    }

    @Override
    public ResponseMessage deserializeResponse(final String msg) throws SerializationException {
        throw new UnsupportedOperationException("text/plain does not have deserialization functions");
    }
}

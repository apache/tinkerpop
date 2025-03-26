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
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.AbstractMessageSerializer;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * A highly use-case specific serializer that only has context for HTTP where results simply need to be converted
 * to string in a line by line fashion for text based returns.
 */
public class TextPlainMessageSerializer extends AbstractMessageSerializer<Function<Object, String>> {

    @Override
    public Function<Object, String> getMapper() {
        return Objects::toString;
    }

    @Override
    public ByteBuf serializeResponseAsBinary(final ResponseMessage responseMessage, final ByteBufAllocator allocator) {
        return (responseMessage.getStatus().getCode() == HttpResponseStatus.OK)
                ? convertStringData(responseMessage.getResult().getData(), false, allocator)
                : convertErrorString(responseMessage.getStatus().getMessage(), allocator);
    }

    @Override
    public ByteBuf writeHeader(ResponseMessage responseMessage, ByteBufAllocator allocator) {
        return convertStringData(responseMessage.getResult().getData(), false, allocator);
    }

    @Override
    public ByteBuf writeChunk(Object aggregate, ByteBufAllocator allocator) {
        return convertStringData((List<Object>) aggregate, true, allocator);
    }

    @Override
    public ByteBuf writeFooter(ResponseMessage responseMessage, ByteBufAllocator allocator) {
        return convertStringData(responseMessage.getResult().getData(), true, allocator);
    }

    @Override
    public ByteBuf writeErrorFooter(ResponseMessage responseMessage, ByteBufAllocator allocator) {
        return convertErrorString(System.lineSeparator() + responseMessage.getStatus().getMessage(), allocator);
    }

    @Override
    public ResponseMessage readChunk(ByteBuf byteBuf, boolean isFirstChunk) {
        throw new UnsupportedOperationException("text/plain does not have deserialization functions");
    }

    @Override
    public ByteBuf serializeRequestAsBinary(final RequestMessage requestMessage, final ByteBufAllocator allocator) {
        throw new UnsupportedOperationException("text/plain does not produce binary");
    }

    @Override
    public RequestMessage deserializeBinaryRequest(final ByteBuf msg) {
        throw new UnsupportedOperationException("text/plain does not have deserialization functions");
    }

    @Override
    public ResponseMessage deserializeBinaryResponse(final ByteBuf msg) {
        throw new UnsupportedOperationException("text/plain does not have deserialization functions");
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[] { "text/plain" };
    }

    private ByteBuf convertStringData(final List<Object> data, final boolean addStartingSeparator, final ByteBufAllocator allocator) {
        final StringBuilder sb = new StringBuilder();

        if (addStartingSeparator) sb.append(System.lineSeparator());
        // this should only serialize success conditions so all should have data in List form
        for (int ix = 0; ix < data.size(); ix ++) {
            sb.append("==>");
            sb.append(data.get(ix));
            if (ix < data.size() - 1)
                sb.append(System.lineSeparator());
        }

        final ByteBuf encodedMessage = allocator.buffer(sb.length());
        encodedMessage.writeCharSequence(sb.toString(), CharsetUtil.UTF_8);

        return encodedMessage;
    }

    private ByteBuf convertErrorString(final String error, final ByteBufAllocator allocator) {
        final ByteBuf encodedMessage = allocator.buffer(error.length());
        encodedMessage.writeCharSequence(error, CharsetUtil.UTF_8);
        return encodedMessage;
    }
}

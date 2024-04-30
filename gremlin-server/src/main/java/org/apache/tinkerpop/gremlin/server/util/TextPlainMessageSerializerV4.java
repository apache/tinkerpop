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
import org.apache.tinkerpop.gremlin.util.message.RequestMessageV4;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessageV4;
import org.apache.tinkerpop.gremlin.util.ser.AbstractMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * A highly use-case specific serializer that only has context for HTTP where results simply need to be converted
 * to string in a line by line fashion for text based returns.
 */
public class TextPlainMessageSerializerV4 extends AbstractMessageSerializerV4<Function<Object, String>> {

    @Override
    public Function<Object, String> getMapper() {
        return Objects::toString;
    }

    @Override
    public ByteBuf serializeResponseAsBinary(final ResponseMessageV4 responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        return (responseMessage.getStatus().getCode() == HttpResponseStatus.OK)
                ? convertStringData((List<Object>) responseMessage.getResult().getData(), false, allocator)
                : convertErrorString(responseMessage.getStatus().getMessage(), allocator);
    }

    @Override
    public ByteBuf writeHeader(ResponseMessageV4 responseMessage, ByteBufAllocator allocator) throws SerializationException {
        return convertStringData((List<Object>) responseMessage.getResult().getData(), false, allocator);
    }

    @Override
    public ByteBuf writeChunk(Object aggregate, ByteBufAllocator allocator) throws SerializationException {
        return convertStringData((List<Object>) aggregate, true, allocator);
    }

    @Override
    public ByteBuf writeFooter(ResponseMessageV4 responseMessage, ByteBufAllocator allocator) throws SerializationException {
        return convertStringData((List<Object>) responseMessage.getResult().getData(), true, allocator);
    }

    @Override
    public ByteBuf writeErrorFooter(ResponseMessageV4 responseMessage, ByteBufAllocator allocator) throws SerializationException {
        return convertErrorString(System.lineSeparator() + responseMessage.getStatus().getMessage(), allocator);
    }

    @Override
    public ResponseMessageV4 readChunk(ByteBuf byteBuf, boolean isFirstChunk) throws SerializationException {
        throw new UnsupportedOperationException("text/plain does not have deserialization functions");
    }

    @Override
    public ByteBuf serializeRequestAsBinary(final RequestMessageV4 requestMessage, final ByteBufAllocator allocator) throws SerializationException {
        throw new UnsupportedOperationException("text/plain does not produce binary");
    }

    @Override
    public RequestMessageV4 deserializeBinaryRequest(final ByteBuf msg) throws SerializationException {
        throw new UnsupportedOperationException("text/plain does not have deserialization functions");
    }

    @Override
    public ResponseMessageV4 deserializeBinaryResponse(final ByteBuf msg) throws SerializationException {
        throw new UnsupportedOperationException("text/plain does not have deserialization functions");
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[] { "text/plain" };
    }

    private ByteBuf convertStringData(final List<Object> data, final boolean addStartingSeparator, final ByteBufAllocator allocator) throws SerializationException {
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

    private ByteBuf convertErrorString(final String error, final ByteBufAllocator allocator) throws SerializationException {
        final ByteBuf encodedMessage = allocator.buffer(error.length());
        encodedMessage.writeCharSequence(error, CharsetUtil.UTF_8);
        return encodedMessage;
    }
}

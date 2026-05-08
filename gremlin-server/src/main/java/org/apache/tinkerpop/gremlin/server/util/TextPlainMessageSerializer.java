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
package org.apache.tinkerpop.gremlin.server.util;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.AbstractMessageSerializer;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * A highly use-case specific serializer that only has context for HTTP where results simply need to be converted
 * to string in a line by line fashion for text based returns.
 */
public class TextPlainMessageSerializer extends AbstractMessageSerializer<Function<Object, String>> {

    private static final NettyBufferFactory bufferFactory = new NettyBufferFactory();

    @Override
    public Function<Object, String> getMapper() {
        return Objects::toString;
    }

    @Override
    public Buffer serializeResponseAsBinary(final ResponseMessage responseMessage) {
        return (responseMessage.getStatus().getCode() == HttpResponseStatus.OK)
                ? convertStringData(responseMessage.getResult().getData(), false)
                : convertErrorString(responseMessage.getStatus().getMessage());
    }

    @Override
    public Buffer writeHeader(final ResponseMessage responseMessage) {
        return convertStringData(responseMessage.getResult().getData(), false);
    }

    @Override
    public Buffer writeChunk(final Object aggregate) {
        return convertStringData((List<Object>) aggregate, true);
    }

    @Override
    public Buffer writeFooter(final ResponseMessage responseMessage) {
        return convertStringData(responseMessage.getResult().getData(), true);
    }

    @Override
    public Buffer writeErrorFooter(final ResponseMessage responseMessage) {
        return convertErrorString(System.lineSeparator() + responseMessage.getStatus().getMessage());
    }

    @Override
    public ResponseMessage readChunk(final Buffer buffer, final boolean isFirstChunk) {
        throw new UnsupportedOperationException("text/plain does not have deserialization functions");
    }

    @Override
    public Buffer serializeRequestAsBinary(final RequestMessage requestMessage) {
        throw new UnsupportedOperationException("text/plain does not produce binary");
    }

    @Override
    public RequestMessage deserializeBinaryRequest(final Buffer msg) {
        throw new UnsupportedOperationException("text/plain does not have deserialization functions");
    }

    @Override
    public ResponseMessage deserializeBinaryResponse(final Buffer msg) {
        throw new UnsupportedOperationException("text/plain does not have deserialization functions");
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[] { "text/plain" };
    }

    private Buffer convertStringData(final List<Object> data, final boolean addStartingSeparator) {
        final StringBuilder sb = new StringBuilder();

        if (addStartingSeparator) sb.append(System.lineSeparator());
        // this should only serialize success conditions so all should have data in List form
        for (int ix = 0; ix < data.size(); ix ++) {
            sb.append("==>");
            sb.append(data.get(ix));
            if (ix < data.size() - 1)
                sb.append(System.lineSeparator());
        }

        final byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        final Buffer buffer = bufferFactory.create(bytes.length);
        buffer.writeBytes(bytes);
        return buffer;
    }

    private Buffer convertErrorString(final String error) {
        final byte[] bytes = error.getBytes(StandardCharsets.UTF_8);
        final Buffer buffer = bufferFactory.create(bytes.length);
        buffer.writeBytes(bytes);
        return buffer;
    }
}

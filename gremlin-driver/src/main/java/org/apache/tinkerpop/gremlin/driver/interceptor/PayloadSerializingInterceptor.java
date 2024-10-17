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
package org.apache.tinkerpop.gremlin.driver.interceptor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tinkerpop.gremlin.driver.HttpRequest;
import org.apache.tinkerpop.gremlin.driver.RequestInterceptor;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;

import java.util.Map;

/**
 * A {@link RequestInterceptor} that serializes the request body usng the provided {@link MessageSerializer}. This
 * interceptor should be run before other interceptors that need to calculate values based on the request body.
 */
public class PayloadSerializingInterceptor implements RequestInterceptor {
    // Should be thread-safe as the GraphBinaryWriter/GraphSONMessageSerializer doesn't maintain state.
    private final MessageSerializer serializer;

    public PayloadSerializingInterceptor(final MessageSerializer serializer) {
        this.serializer = serializer;
    }

    @Override
    public HttpRequest apply(HttpRequest httpRequest) {
        if (!(httpRequest.getBody() instanceof RequestMessage)) {
            throw new IllegalArgumentException("Only RequestMessage serialization is supported");
        }

        final RequestMessage request = (RequestMessage) httpRequest.getBody();
        final ByteBuf requestBuf;
        try {
            requestBuf = serializer.serializeRequestAsBinary(request, ByteBufAllocator.DEFAULT);
        } catch (SerializationException se) {
            throw new RuntimeException(new ResponseException(HttpResponseStatus.BAD_REQUEST, String.format(
                    "An error occurred during serialization of this request [%s] - it could not be sent to the server - Reason: %s",
                    request, se)));
        }

        // Convert from ByteBuf to bytes[] because that's what the final request body should contain.
        final byte[] requestBytes = ByteBufUtil.getBytes(requestBuf);
        requestBuf.release();

        httpRequest.setBody(requestBytes);
        httpRequest.headers().put(HttpRequest.Headers.CONTENT_TYPE, serializer.mimeTypesSupported()[0]);

        return httpRequest;
    }
}

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

import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

/**
 * Serialize results to JSON with version 1.0.x schema and serialization which does not embed types.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GraphSONUntypedMessageSerializerV1 extends AbstractGraphSONMessageSerializerV1 implements MessageTextSerializer<ObjectMapper> {
    private static final Logger logger = LoggerFactory.getLogger(GraphSONUntypedMessageSerializerV1.class);
    private static final String MIME_TYPE = SerTokens.MIME_GRAPHSON_V1_UNTYPED;

    private static byte[] header;

    static {
        final ByteBuffer buffer = ByteBuffer.allocate(MIME_TYPE.length() + 1);
        buffer.put((byte) MIME_TYPE.length());
        buffer.put(MIME_TYPE.getBytes());
        header = buffer.array();
    }

    public GraphSONUntypedMessageSerializerV1() {
        super();
    }

    public GraphSONUntypedMessageSerializerV1(final GraphSONMapper mapper) {
        super(mapper);
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[]{MIME_TYPE, SerTokens.MIME_JSON};
    }

    @Override
    GraphSONMapper.Builder configureBuilder(final GraphSONMapper.Builder builder) {
        // already set to 1.0 in AbstractGraphSONMessageSerializerV1
        return builder.addCustomModule(new GremlinServerModule())
                .typeInfo(TypeInfo.NO_TYPES);
    }

    @Override
    byte[] obtainHeader() {
        return header;
    }

    @Override
    public ResponseMessage deserializeResponse(final String msg) throws SerializationException {
        try {
            final Map<String, Object> responseData = mapper.readValue(msg, mapTypeReference);
            final Map<String, Object> status = (Map<String, Object>) responseData.get(SerTokens.TOKEN_STATUS);
            final Map<String, Object> result = (Map<String, Object>) responseData.get(SerTokens.TOKEN_RESULT);
            return ResponseMessage.build(UUID.fromString(responseData.get(SerTokens.TOKEN_REQUEST).toString()))
                    .code(ResponseStatusCode.getFromValue((Integer) status.get(SerTokens.TOKEN_CODE)))
                    .statusMessage(String.valueOf(status.get(SerTokens.TOKEN_MESSAGE)))
                    .statusAttributes((Map<String, Object>) status.get(SerTokens.TOKEN_ATTRIBUTES))
                    .result(result.get(SerTokens.TOKEN_DATA))
                    .responseMetaData((Map<String, Object>) result.get(SerTokens.TOKEN_META))
                    .create();
        } catch (Exception ex) {
            logger.warn("Response [{}] could not be deserialized by {}.", msg, AbstractGraphSONMessageSerializerV1.class.getName());
            throw new SerializationException(ex);
        }
    }

    @Override
    public String serializeResponseAsString(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        try {
            return mapper.writeValueAsString(responseMessage);
        } catch (Exception ex) {
            logger.warn("Response [{}] could not be serialized by {}.", responseMessage.toString(), AbstractGraphSONMessageSerializerV1.class.getName());
            throw new SerializationException(ex);
        }
    }

    @Override
    public RequestMessage deserializeRequest(final String msg) throws SerializationException {
        try {
            return mapper.readValue(msg, RequestMessage.class);
        } catch (Exception ex) {
            logger.warn("Request [{}] could not be deserialized by {}.", msg, AbstractGraphSONMessageSerializerV1.class.getName());
            throw new SerializationException(ex);
        }
    }

    @Override
    public String serializeRequestAsString(final RequestMessage requestMessage, final ByteBufAllocator allocator) throws SerializationException {
        try {
            return mapper.writeValueAsString(requestMessage);
        } catch (Exception ex) {
            logger.warn("Request [{}] could not be serialized by {}.", requestMessage.toString(), AbstractGraphSONMessageSerializerV1.class.getName());
            throw new SerializationException(ex);
        }
    }
}

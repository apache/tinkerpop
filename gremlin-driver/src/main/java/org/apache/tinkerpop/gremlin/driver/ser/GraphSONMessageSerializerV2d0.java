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
package org.apache.tinkerpop.gremlin.driver.ser;

import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV2d0;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

/**
 * Serialize results to JSON with version 2.0.x schema and the extended module.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GraphSONMessageSerializerV2d0 extends AbstractGraphSONMessageSerializerV2d0 implements MessageTextSerializer {
    private static final Logger logger = LoggerFactory.getLogger(GraphSONMessageSerializerV2d0.class);
    private static final String MIME_TYPE = SerTokens.MIME_GRAPHSON_V2D0;

    private static byte[] header;

    static {
        final ByteBuffer buffer = ByteBuffer.allocate(MIME_TYPE.length() + 1);
        buffer.put((byte) MIME_TYPE.length());
        buffer.put(MIME_TYPE.getBytes());
        header = buffer.array();
    }

        /**
         * Creates a default GraphSONMessageSerializer.
         *
         * By default this will internally instantiate a {@link GraphSONMapper} and register
         * a {@link GremlinServerModule} and {@link GraphSONXModuleV2d0} to the mapper.
         *
         * @see #GraphSONMessageSerializerV2d0(GraphSONMapper.Builder)
         */
    public GraphSONMessageSerializerV2d0() {
        super();
    }

    /**
     * Create a GraphSONMessageSerializer from a {@link GraphSONMapper}. Deprecated, use
     * {@link #GraphSONMessageSerializerV2d0(GraphSONMapper.Builder)} instead.
     */
    @Deprecated
    public GraphSONMessageSerializerV2d0(final GraphSONMapper mapper) {
        super(mapper);
    }

    /**
     * Create a GraphSONMessageSerializer with a provided {@link GraphSONMapper.Builder}.
     *
     * Note that to make this mapper usable in the context of request messages and responses,
     * this method will automatically register a {@link GremlinServerModule} to the provided
     * mapper.
     */
    public GraphSONMessageSerializerV2d0(final GraphSONMapper.Builder mapperBuilder) {
        super(mapperBuilder);
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[]{MIME_TYPE, SerTokens.MIME_JSON};
    }

    @Override
    GraphSONMapper.Builder configureBuilder(final GraphSONMapper.Builder builder) {
        // already set to 2.0 in AbstractGraphSONMessageSerializerV2d0
        return builder.typeInfo(TypeInfo.PARTIAL_TYPES).addCustomModule(new GremlinServerModule());
    }

    @Override
    byte[] obtainHeader() {
        return header;
    }

    @Override
    public ResponseMessage deserializeResponse(final String msg) throws SerializationException {
        try {
            return mapper.readValue(msg, ResponseMessage.class);
        } catch (Exception ex) {
            logger.warn("Response [{}] could not be deserialized by {}.", msg, AbstractGraphSONMessageSerializerV2d0.class.getName());
            throw new SerializationException(ex);
        }
    }

    @Override
    public String serializeResponseAsString(final ResponseMessage responseMessage) throws SerializationException {
        try {
            return mapper.writeValueAsString(responseMessage);
        } catch (Exception ex) {
            logger.warn("Response [{}] could not be serialized by {}.", responseMessage.toString(), AbstractGraphSONMessageSerializerV2d0.class.getName());
            throw new SerializationException(ex);
        }
    }

    @Override
    public RequestMessage deserializeRequest(final String msg) throws SerializationException {
        try {
            return mapper.readValue(msg, RequestMessage.class);
        } catch (Exception ex) {
            logger.warn("Request [{}] could not be deserialized by {}.", msg, AbstractGraphSONMessageSerializerV2d0.class.getName());
            throw new SerializationException(ex);
        }
    }

    @Override
    public String serializeRequestAsString(final RequestMessage requestMessage) throws SerializationException {
        try {
            return mapper.writeValueAsString(requestMessage);
        } catch (Exception ex) {
            logger.warn("Request [{}] could not be serialized by {}.", requestMessage.toString(), AbstractGraphSONMessageSerializerV2d0.class.getName());
            throw new SerializationException(ex);
        }
    }
}

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
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Serialize results to JSON with version 3.0.x schema and the extended module.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GraphSONMessageSerializerV3d0 extends AbstractGraphSONMessageSerializerV2d0 implements MessageTextSerializer {
    private static final Logger logger = LoggerFactory.getLogger(GraphSONMessageSerializerV3d0.class);
    private static final String MIME_TYPE = SerTokens.MIME_GRAPHSON_V3D0;

    private static byte[] header;

    static {
        final ByteBuffer buffer = ByteBuffer.allocate(MIME_TYPE.length() + 1);
        buffer.put((byte) MIME_TYPE.length());
        buffer.put(MIME_TYPE.getBytes());
        header = buffer.array();
    }

    /**
     * Creates a default GraphSONMessageSerializer.
     * <p>
     * By default this will internally instantiate a {@link GraphSONMapper} and register
     * a {@link GremlinServerModule} and {@link org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV3d0} to the mapper.
     *
     * @see #GraphSONMessageSerializerV3d0(GraphSONMapper.Builder)
     */
    public GraphSONMessageSerializerV3d0() {
        super();
    }

    /**
     * Create a GraphSONMessageSerializer from a {@link GraphSONMapper}. Deprecated, use
     * {@link #GraphSONMessageSerializerV3d0(GraphSONMapper.Builder)} instead.
     */
    @Deprecated
    public GraphSONMessageSerializerV3d0(final GraphSONMapper mapper) {
        super(mapper);
    }

    /**
     * Create a GraphSONMessageSerializer with a provided {@link GraphSONMapper.Builder}.
     *
     * Note that to make this mapper usable in the context of request messages and responses,
     * this method will automatically register a {@link GremlinServerModule} to the provided
     * mapper.
     */
    public GraphSONMessageSerializerV3d0(final GraphSONMapper.Builder mapperBuilder) {
        super(mapperBuilder);
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[]{MIME_TYPE, "application/json"};
    }

    @Override
    GraphSONMapper.Builder configureBuilder(final GraphSONMapper.Builder builder) {
        // override the 2.0 in AbstractGraphSONMessageSerializerV2d0
        return builder.version(GraphSONVersion.V3_0).addCustomModule(new GremlinServerModule());
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
            logger.warn(String.format("Response [%s] could not be deserialized by %s.", msg, GraphSONMessageSerializerV3d0.class.getName()), ex);
            throw new SerializationException(ex);
        }
    }

    @Override
    public String serializeResponseAsString(final ResponseMessage responseMessage) throws SerializationException {
        try {
            return mapper.writeValueAsString(responseMessage);
        } catch (Exception ex) {
            logger.warn(String.format("Response [%s] could not be serialized by %s.", responseMessage.toString(), GraphSONMessageSerializerV3d0.class.getName()), ex);
            throw new SerializationException(ex);
        }
    }

    @Override
    public RequestMessage deserializeRequest(final String msg) throws SerializationException {
        try {
            return mapper.readValue(msg, RequestMessage.class);
        } catch (Exception ex) {
            logger.warn(String.format("Request [%s] could not be deserialized by %s.", msg, GraphSONMessageSerializerV3d0.class.getName()), ex);
            throw new SerializationException(ex);
        }
    }

    @Override
    public String serializeRequestAsString(final RequestMessage requestMessage) throws SerializationException {
        try {
            return mapper.writeValueAsString(requestMessage);
        } catch (Exception ex) {
            logger.warn(String.format("Request [%s] could not be serialized by %s.", requestMessage.toString(), GraphSONMessageSerializerV3d0.class.getName()), ex);
            throw new SerializationException(ex);
        }
    }
}

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

import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;

import java.nio.ByteBuffer;

/**
 * Serialize results to JSON with version 2.0.x schema and the extended module.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As for release 3.4.0, replaced by {@link GraphSONMessageSerializerV2d0}.
 */
@Deprecated
public final class GraphSONMessageSerializerGremlinV2d0 extends AbstractGraphSONMessageSerializerV2d0 {

    private static final String MIME_TYPE = SerTokens.MIME_GRAPHSON_V2D0;

    private static byte[] header;

    static {
        final ByteBuffer buffer = ByteBuffer.allocate(MIME_TYPE.length() + 1);
        buffer.put((byte) MIME_TYPE.length());
        buffer.put(MIME_TYPE.getBytes());
        header = buffer.array();
    }

    /**
     * Creates a default GraphSONMessageSerializerGremlin.
     * <p>
     * By default this will internally instantiate a {@link GraphSONMapper} and register
     * a {@link GremlinServerModule} and {@link org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV2d0} to the mapper.
     *
     * @see #GraphSONMessageSerializerGremlinV2d0(GraphSONMapper.Builder)
     */
    public GraphSONMessageSerializerGremlinV2d0() {
        super();
    }

    /**
     * Create a GraphSONMessageSerializer from a {@link GraphSONMapper}. Deprecated, use
     * {@link #GraphSONMessageSerializerGremlinV2d0(GraphSONMapper.Builder)} instead.
     */
    @Deprecated
    public GraphSONMessageSerializerGremlinV2d0(final GraphSONMapper mapper) {
        super(mapper);
    }

    /**
     * Create a GraphSONMessageSerializerGremlin with a provided {@link GraphSONMapper.Builder}.
     * <p>
     * Note that to make this mapper usable in the context of request messages and responses,
     * this method will automatically register a {@link GremlinServerModule} to the provided
     * mapper.
     */
    public GraphSONMessageSerializerGremlinV2d0(final GraphSONMapper.Builder mapperBuilder) {
        super(mapperBuilder);
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[]{SerTokens.MIME_GRAPHSON_V2D0, SerTokens.MIME_JSON};
    }

    @Override
    byte[] obtainHeader() {
        return header;
    }

    @Override
    GraphSONMapper.Builder configureBuilder(final GraphSONMapper.Builder builder) {
        // already set to 2.0 in AbstractGraphSONMessageSerializerV2d0
        return builder.typeInfo(TypeInfo.PARTIAL_TYPES).addCustomModule(new GremlinServerModule());
    }
}

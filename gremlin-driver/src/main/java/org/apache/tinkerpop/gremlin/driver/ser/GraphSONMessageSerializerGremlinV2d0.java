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
 */
public final class GraphSONMessageSerializerGremlinV2d0 extends AbstractGraphSONMessageSerializerV2d0 {

    private static final String MIME_TYPE = SerTokens.MIME_GRAPHSON_V2D0;

    private static byte[] header;

    static {
        final ByteBuffer buffer = ByteBuffer.allocate(MIME_TYPE.length() + 1);
        buffer.put((byte) MIME_TYPE.length());
        buffer.put(MIME_TYPE.getBytes());
        header = buffer.array();
    }

    public GraphSONMessageSerializerGremlinV2d0() {
        super();
    }

    public GraphSONMessageSerializerGremlinV2d0(final GraphSONMapper mapper) {
        super(mapper);
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[]{MIME_TYPE};
    }

    @Override
    byte[] obtainHeader() {
        return header;
    }

    @Override
    GraphSONMapper.Builder configureBuilder(final GraphSONMapper.Builder builder) {
        // already set to 2.0 in AbstractGraphSONMessageSerializerV2d0
        return builder.typeInfo(TypeInfo.PARTIAL_TYPES);
    }
}

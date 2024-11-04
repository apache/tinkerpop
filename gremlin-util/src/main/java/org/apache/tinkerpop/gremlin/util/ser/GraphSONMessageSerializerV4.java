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

import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV4;

/**
 * Serialize results to JSON with version 4.0.x schema and the extended module.
 */
public final class GraphSONMessageSerializerV4 extends AbstractGraphSONMessageSerializerV4 {
    private static final String MIME_TYPE = SerTokens.MIME_GRAPHSON_V4;

    /**
     * Creates a default GraphSONMessageSerializer.
     * <p>
     * By default this will internally instantiate a {@link GraphSONMapper} and register
     * a {@link GremlinServerModuleV4} and {@link GraphSONXModuleV4} to the mapper.
     *
     * @see #GraphSONMessageSerializerV4(GraphSONMapper.Builder)
     */
    public GraphSONMessageSerializerV4() {
        super();
    }

    /**
     * Create a GraphSONMessageSerializer with a provided {@link GraphSONMapper.Builder}.
     *
     * Note that to make this mapper usable in the context of request messages and responses,
     * this method will automatically register a {@link GremlinServerModuleV4} to the provided
     * mapper.
     */
    public GraphSONMessageSerializerV4(final GraphSONMapper.Builder mapperBuilder) {
        super(mapperBuilder);
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[]{MIME_TYPE, SerTokens.MIME_JSON};
    }

    @Override
    GraphSONMapper.Builder configureBuilder(final GraphSONMapper.Builder builder) {
        return builder.version(GraphSONVersion.V4_0).addCustomModule(new GremlinServerModuleV4());
    }
}

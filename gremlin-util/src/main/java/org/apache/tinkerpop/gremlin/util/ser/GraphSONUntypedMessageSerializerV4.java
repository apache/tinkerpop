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
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV4;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;

/**
 * Serialize results to JSON with version 4 schema and the extended module without embedded types.
  */
public final class GraphSONUntypedMessageSerializerV4 extends AbstractGraphSONMessageSerializerV4 {
    private static final String MIME_TYPE = SerTokens.MIME_GRAPHSON_V4_UNTYPED;

    /**
     * Creates a default GraphSONMessageSerializer.
     *
     * By default this will internally instantiate a {@link GraphSONMapper} and register
     * a {@link GremlinServerModuleV4} and {@link GraphSONXModuleV4} to the mapper.
     *
     * @see #GraphSONUntypedMessageSerializerV4(GraphSONMapper.Builder)
     */
    public GraphSONUntypedMessageSerializerV4() {
        super();
    }

    /**
     * Create a GraphSONMessageSerializer with a provided {@link GraphSONMapper.Builder}.
     *
     * Note that to make this mapper usable in the context of request messages and responses,
     * this method will automatically register a {@link GremlinServerModuleV4} to the provided
     * mapper.
     */
    public GraphSONUntypedMessageSerializerV4(final GraphSONMapper.Builder mapperBuilder) {
        super(mapperBuilder);
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[]{MIME_TYPE, SerTokens.MIME_JSON};
    }

    @Override
    GraphSONMapper.Builder configureBuilder(final GraphSONMapper.Builder builder) {
        return builder.typeInfo(TypeInfo.NO_TYPES).addCustomModule(new GremlinServerModuleV4());
    }

    @Override
    protected boolean isTyped() { return false; }
}

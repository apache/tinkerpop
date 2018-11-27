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

import org.apache.tinkerpop.gremlin.driver.MessageSerializer;

/**
 * An enum of the default serializers.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public enum Serializers {
    /**
     * GraphSON 3.0.
     */
    GRAPHSON(SerTokens.MIME_JSON),
    GRAPHSON_V1D0(SerTokens.MIME_GRAPHSON_V1D0),
    GRAPHSON_V2D0(SerTokens.MIME_GRAPHSON_V2D0),
    GRAPHSON_V3D0(SerTokens.MIME_GRAPHSON_V3D0),
    GRYO_V1D0(SerTokens.MIME_GRYO_V1D0),
    GRYO_V3D0(SerTokens.MIME_GRYO_V3D0),
    GRYO_LITE_V1D0(SerTokens.MIME_GRYO_LITE_V1D0);

    private String value;

    /**
     * Default serializer for results returned from Gremlin Server. This implementation must be of type
     * {@link org.apache.tinkerpop.gremlin.driver.ser.MessageTextSerializer} so that it can be compatible with text-based
     * websocket messages.
     *
     * @deprecated As of release 3.3.5, not replaced, simply specify the exact version of the serializer to use.
     */
    @Deprecated
    public static final MessageSerializer DEFAULT_RESULT_SERIALIZER = new GraphSONMessageSerializerV1d0();

    /**
     * Default serializer for requests received by Gremlin Server. This implementation must be of type
     * {@link org.apache.tinkerpop.gremlin.driver.ser.MessageTextSerializer} so that it can be compatible with text-based
     * websocket messages.
     *
     * @deprecated As of release 3.3.5, not replaced, simply specify the exact version of the serializer to use.
     */
    @Deprecated
    public static final MessageSerializer DEFAULT_REQUEST_SERIALIZER = new GraphSONMessageSerializerV1d0();

    Serializers(final String mimeType) {
        this.value = mimeType;
    }

    public String getValue() {
        return value;
    }

    public MessageSerializer simpleInstance() {
        switch (value) {
            case SerTokens.MIME_JSON:
                return new GraphSONMessageSerializerV3d0();
            case SerTokens.MIME_GRAPHSON_V1D0:
                return new GraphSONMessageSerializerGremlinV1d0();
            case SerTokens.MIME_GRAPHSON_V2D0:
                return new GraphSONMessageSerializerV2d0();
            case SerTokens.MIME_GRAPHSON_V3D0:
                return new GraphSONMessageSerializerV3d0();
            case SerTokens.MIME_GRYO_V1D0:
                return new GryoMessageSerializerV1d0();
            case SerTokens.MIME_GRYO_V3D0:
                return new GryoMessageSerializerV3d0();
            case SerTokens.MIME_GRYO_LITE_V1D0:
                return new GryoLiteMessageSerializerV1d0();
            default:
                throw new RuntimeException("Could not create a simple MessageSerializer instance of " + value);
        }
    }
}

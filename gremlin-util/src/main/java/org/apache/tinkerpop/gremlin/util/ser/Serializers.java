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

import org.apache.tinkerpop.gremlin.util.MessageSerializer;

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

    /**
     * GraphSON 1.0 with types.
     */
    GRAPHSON_V1(SerTokens.MIME_GRAPHSON_V1),

    /**
     * GraphSON 1.0 without types.
     */
    GRAPHSON_V1_UNTYPED(SerTokens.MIME_GRAPHSON_V1_UNTYPED),

    /**
     * GraphSON 2.0 with types.
     */
    GRAPHSON_V2(SerTokens.MIME_GRAPHSON_V2),

    /**
     * GraphSON 2.0 without types.
     */
    GRAPHSON_V2_UNTYPED(SerTokens.MIME_GRAPHSON_V2_UNTYPED),

    /**
     * GraphSON 3.0 with types.
     */
    GRAPHSON_V3(SerTokens.MIME_GRAPHSON_V3),

    /**
     * GraphSON 3.0 without types.
     */
    GRAPHSON_V3_UNTYPED(SerTokens.MIME_GRAPHSON_V3_UNTYPED),

    /**
     * GraphBinary 1.0.
     */
    GRAPHBINARY_V1(SerTokens.MIME_GRAPHBINARY_V1);

    private String value;

    Serializers(final String mimeType) {
        this.value = mimeType;
    }

    public String getValue() {
        return value;
    }

    public MessageSerializer<?> simpleInstance() {
        switch (value) {
            case SerTokens.MIME_JSON:
            case SerTokens.MIME_GRAPHSON_V3:
                return new GraphSONMessageSerializerV3();
            case SerTokens.MIME_GRAPHSON_V1:
                return new GraphSONMessageSerializerV1();
            case SerTokens.MIME_GRAPHSON_V1_UNTYPED:
                return new GraphSONUntypedMessageSerializerV1();
            case SerTokens.MIME_GRAPHSON_V2:
                return new GraphSONMessageSerializerV2();
            case SerTokens.MIME_GRAPHSON_V2_UNTYPED:
                return new GraphSONUntypedMessageSerializerV2();
            case SerTokens.MIME_GRAPHSON_V3_UNTYPED:
                return new GraphSONUntypedMessageSerializerV3();
            case SerTokens.MIME_GRAPHBINARY_V1:
                return new GraphBinaryMessageSerializerV1();
            default:
                throw new RuntimeException("Could not create a simple MessageSerializer instance of " + value);
        }
    }
}

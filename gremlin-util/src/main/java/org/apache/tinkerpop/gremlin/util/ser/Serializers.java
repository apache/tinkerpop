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
 * An enum of the default serializers available starting in v4.0.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public enum Serializers {

    /**
     * GraphSON 4.0.
     */
    GRAPHSON(SerTokens.MIME_JSON),

    /**
     * GraphSON 4.0 with types.
     */
    GRAPHSON_V4(SerTokens.MIME_GRAPHSON_V4),

    /**
     * GraphSON 4.0 without types.
     */
    GRAPHSON_V4_UNTYPED(SerTokens.MIME_GRAPHSON_V4_UNTYPED),

    /**
     * GraphBinary 4.0.
     */
    GRAPHBINARY_V4(SerTokens.MIME_GRAPHBINARY_V4);

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
            case SerTokens.MIME_GRAPHSON_V4:
                return new GraphSONMessageSerializerV4();
            case SerTokens.MIME_GRAPHSON_V4_UNTYPED:
                return new GraphSONUntypedMessageSerializerV4();
            case SerTokens.MIME_GRAPHBINARY_V4:
                return new GraphBinaryMessageSerializerV4();
            default:
                throw new RuntimeException("Could not create a simple MessageSerializer instance of " + value);
        }
    }
}

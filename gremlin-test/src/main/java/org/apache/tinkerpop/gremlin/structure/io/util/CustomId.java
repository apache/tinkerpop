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
package org.apache.tinkerpop.gremlin.structure.io.util;

import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerationException;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.UUID;

/**
 * A mock identifier for use in ensuring that custom serializers work around element identifiers.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class CustomId {
    private String cluster;
    private UUID elementId;

    private CustomId() {
        // required no-arg for gryo serialization
    }

    public CustomId(final String cluster, final UUID elementId) {
        this.cluster = cluster;
        this.elementId = elementId;
    }

    public String getCluster() {
        return cluster;
    }

    public UUID getElementId() {
        return elementId;
    }

    @Override
    public String toString() {
        return cluster + ":" + elementId;
    }

    public static class CustomIdJacksonSerializer extends StdSerializer<CustomId> {
        public CustomIdJacksonSerializer() {
            super(CustomId.class);
        }

        @Override
        public void serialize(final CustomId customId, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException {
            // when types are not embedded, stringify or resort to JSON primitive representations of the
            // type so that non-jvm languages can better interoperate with the TinkerPop stack.
            jsonGenerator.writeString(customId.toString());
        }

        @Override
        public void serializeWithType(final CustomId customId, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException, JsonProcessingException {
            // when the type is included add "class" as a key and then try to utilize as much of the
            // default serialization provided by jackson data-bind as possible.  for example, write
            // the uuid as an object so that when jackson serializes it, it uses the uuid serializer
            // to write it out with the type.  in this way, data-bind should be able to deserialize
            // it back when types are embedded.
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField(GraphSONTokens.CLASS, CustomId.class.getName());
            jsonGenerator.writeStringField("cluster", customId.getCluster());
            jsonGenerator.writeObjectField("elementId", customId.getElementId());
            jsonGenerator.writeEndObject();
        }
    }
}

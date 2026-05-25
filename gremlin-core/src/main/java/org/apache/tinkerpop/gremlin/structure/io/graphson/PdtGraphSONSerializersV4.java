/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedType;
import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedTypeRegistry;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdScalarSerializer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * GraphSON V4 serializers for {@link ProviderDefinedType}.
 */
final class PdtGraphSONSerializersV4 {

    private PdtGraphSONSerializersV4() {
    }

    final static class ProviderDefinedTypeJacksonSerializer extends StdScalarSerializer<ProviderDefinedType> {

        public ProviderDefinedTypeJacksonSerializer() {
            super(ProviderDefinedType.class);
        }

        @Override
        public void serialize(final ProviderDefinedType pdt, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("type", pdt.getName());
            jsonGenerator.writeFieldName("fields");
            jsonGenerator.writeStartObject();
            for (final Map.Entry<String, Object> entry : pdt.getProperties().entrySet()) {
                jsonGenerator.writeFieldName(entry.getKey());
                jsonGenerator.writeObject(entry.getValue());
            }
            jsonGenerator.writeEndObject();
            jsonGenerator.writeEndObject();
        }
    }

    static class ProviderDefinedTypeJacksonDeserializer extends StdDeserializer<ProviderDefinedType> {

        private ProviderDefinedTypeRegistry registry;

        public ProviderDefinedTypeJacksonDeserializer() {
            super(ProviderDefinedType.class);
        }

        void setRegistry(final ProviderDefinedTypeRegistry registry) {
            this.registry = registry;
        }

        @Override
        public ProviderDefinedType deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext)
                throws IOException {
            String typeName = null;
            Map<String, Object> fields = new LinkedHashMap<>();

            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                final String fieldName = jsonParser.getCurrentName();
                if ("type".equals(fieldName)) {
                    jsonParser.nextToken();
                    typeName = jsonParser.getText();
                } else if ("fields".equals(fieldName)) {
                    jsonParser.nextToken();
                    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                        final String key = jsonParser.getCurrentName();
                        jsonParser.nextToken();
                        final Object value = deserializationContext.readValue(jsonParser, Object.class);
                        fields.put(key, value);
                    }
                }
            }

            final ProviderDefinedType pdt = new ProviderDefinedType(typeName, fields);
            if (registry != null) {
                final Object hydrated = registry.hydrate(pdt);
                if (hydrated instanceof ProviderDefinedType)
                    return (ProviderDefinedType) hydrated;
                // Store hydrated object back as a single-entry PDT so the typed result is accessible.
                // This preserves the return type contract while enabling hydration.
                return pdt.withHydrated(hydrated);
            }
            return pdt;
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }
}

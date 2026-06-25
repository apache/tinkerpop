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

import org.apache.tinkerpop.gremlin.structure.io.pdt.CompositePDTAdapter;
import org.apache.tinkerpop.gremlin.structure.io.pdt.PrimitivePDTAdapter;
import org.apache.tinkerpop.gremlin.structure.io.pdt.PrimitiveProviderDefinedType;
import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedType;
import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedTypeRegistry;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdScalarSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

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
            for (final Map.Entry<String, Object> entry : pdt.getFields().entrySet()) {
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

    final static class PrimitiveProviderDefinedTypeJacksonSerializer extends StdScalarSerializer<PrimitiveProviderDefinedType> {

        public PrimitiveProviderDefinedTypeJacksonSerializer() {
            super(PrimitiveProviderDefinedType.class);
        }

        @Override
        public void serialize(final PrimitiveProviderDefinedType pdt, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("type", pdt.getName());
            jsonGenerator.writeStringField("value", pdt.getValue());
            jsonGenerator.writeEndObject();
        }
    }

    static class PrimitiveProviderDefinedTypeJacksonDeserializer extends StdDeserializer<PrimitiveProviderDefinedType> {

        private ProviderDefinedTypeRegistry registry;

        public PrimitiveProviderDefinedTypeJacksonDeserializer() {
            super(PrimitiveProviderDefinedType.class);
        }

        void setRegistry(final ProviderDefinedTypeRegistry registry) {
            this.registry = registry;
        }

        @Override
        public PrimitiveProviderDefinedType deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext)
                throws IOException {
            String typeName = null;
            String value = null;

            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                final String fieldName = jsonParser.getCurrentName();
                jsonParser.nextToken();
                if ("type".equals(fieldName)) {
                    typeName = jsonParser.getText();
                } else if ("value".equals(fieldName)) {
                    value = jsonParser.getText();
                }
            }

            final PrimitiveProviderDefinedType pdt = new PrimitiveProviderDefinedType(typeName, value);
            if (registry != null) {
                final Object hydrated = registry.hydratePrimitive(pdt);
                if (hydrated instanceof PrimitiveProviderDefinedType)
                    return (PrimitiveProviderDefinedType) hydrated;
                return pdt.withHydrated(hydrated);
            }
            return pdt;
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

    /**
     * A serializer that converts raw objects to {@link ProviderDefinedType} using a registered adapter,
     * then serializes the resulting PDT in the standard CompositePdt format.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static class PdtAdapterJacksonSerializer extends StdSerializer<Object> {

        private final ProviderDefinedTypeRegistry registry;

        PdtAdapterJacksonSerializer(final ProviderDefinedTypeRegistry registry) {
            super(Object.class);
            this.registry = registry;
        }

        @Override
        public void serialize(final Object value, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException {
            final ProviderDefinedType pdt = toPdt(value);
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("type", pdt.getName());
            jsonGenerator.writeFieldName("fields");
            jsonGenerator.writeStartObject();
            for (final Map.Entry<String, Object> entry : pdt.getFields().entrySet()) {
                jsonGenerator.writeFieldName(entry.getKey());
                jsonGenerator.writeObject(entry.getValue());
            }
            jsonGenerator.writeEndObject();
            jsonGenerator.writeEndObject();
        }

        @Override
        public void serializeWithType(final Object value, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider,
                                      final org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer typeSerializer) throws IOException {
            // Convert to ProviderDefinedType and delegate to its registered typed serializer
            final ProviderDefinedType pdt = toPdt(value);
            serializerProvider.findTypedValueSerializer(ProviderDefinedType.class, true, null)
                    .serialize(pdt, jsonGenerator, serializerProvider);
        }

        private ProviderDefinedType toPdt(final Object value) throws IOException {
            final Optional<CompositePDTAdapter<?>> opt = registry.getCompositeAdapterByClass(value.getClass());
            if (!opt.isPresent()) {
                throw new IOException("No adapter found for " + value.getClass().getName());
            }
            final CompositePDTAdapter adapter = opt.get();
            final Map<String, Object> fields = adapter.toFields(value);
            return new ProviderDefinedType(adapter.typeName(), fields);
        }
    }

    /**
     * A serializer that converts raw objects to {@link PrimitiveProviderDefinedType} using a registered
     * {@link PrimitivePDTAdapter}, then serializes the result in the standard PrimitivePdt format.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static class PrimitivePdtAdapterJacksonSerializer extends StdSerializer<Object> {

        private final ProviderDefinedTypeRegistry registry;

        PrimitivePdtAdapterJacksonSerializer(final ProviderDefinedTypeRegistry registry) {
            super(Object.class);
            this.registry = registry;
        }

        @Override
        public void serialize(final Object value, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException {
            final PrimitiveProviderDefinedType pdt = toPrimitivePdt(value);
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("type", pdt.getName());
            jsonGenerator.writeStringField("value", pdt.getValue());
            jsonGenerator.writeEndObject();
        }

        @Override
        public void serializeWithType(final Object value, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider,
                                      final org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer typeSerializer) throws IOException {
            final PrimitiveProviderDefinedType pdt = toPrimitivePdt(value);
            serializerProvider.findTypedValueSerializer(PrimitiveProviderDefinedType.class, true, null)
                    .serialize(pdt, jsonGenerator, serializerProvider);
        }

        private PrimitiveProviderDefinedType toPrimitivePdt(final Object value) throws IOException {
            final Optional<PrimitivePDTAdapter<?>> opt = registry.getPrimitiveAdapterByClass(value.getClass());
            if (!opt.isPresent()) {
                throw new IOException("No primitive adapter found for " + value.getClass().getName());
            }
            final PrimitivePDTAdapter adapter = opt.get();
            final String strValue = adapter.toValue(value);
            return new PrimitiveProviderDefinedType(adapter.typeName(), strValue);
        }
    }
}

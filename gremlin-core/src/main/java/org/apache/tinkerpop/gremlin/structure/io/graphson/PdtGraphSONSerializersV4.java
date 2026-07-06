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
import org.apache.tinkerpop.gremlin.structure.io.pdt.PrimitivePDT;
import org.apache.tinkerpop.gremlin.structure.io.pdt.CompositePDT;
import org.apache.tinkerpop.gremlin.structure.io.pdt.PDTRegistry;
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
 * GraphSON V4 serializers for {@link CompositePDT}.
 */
final class PdtGraphSONSerializersV4 {

    private PdtGraphSONSerializersV4() {
    }

    final static class CompositePDTJacksonSerializer extends StdScalarSerializer<CompositePDT> {

        public CompositePDTJacksonSerializer() {
            super(CompositePDT.class);
        }

        @Override
        public void serialize(final CompositePDT pdt, final JsonGenerator jsonGenerator,
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

    static class CompositePDTJacksonDeserializer extends StdDeserializer<CompositePDT> {

        private PDTRegistry registry;

        public CompositePDTJacksonDeserializer() {
            super(CompositePDT.class);
        }

        void setRegistry(final PDTRegistry registry) {
            this.registry = registry;
        }

        @Override
        public CompositePDT deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext)
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

            final CompositePDT pdt = new CompositePDT(typeName, fields);
            if (registry != null) {
                final Object hydrated = registry.hydrate(pdt);
                if (hydrated instanceof CompositePDT)
                    return (CompositePDT) hydrated;
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

    final static class PrimitivePDTJacksonSerializer extends StdScalarSerializer<PrimitivePDT> {

        public PrimitivePDTJacksonSerializer() {
            super(PrimitivePDT.class);
        }

        @Override
        public void serialize(final PrimitivePDT pdt, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("type", pdt.getName());
            jsonGenerator.writeStringField("value", pdt.getValue());
            jsonGenerator.writeEndObject();
        }
    }

    static class PrimitivePDTJacksonDeserializer extends StdDeserializer<PrimitivePDT> {

        private PDTRegistry registry;

        public PrimitivePDTJacksonDeserializer() {
            super(PrimitivePDT.class);
        }

        void setRegistry(final PDTRegistry registry) {
            this.registry = registry;
        }

        @Override
        public PrimitivePDT deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext)
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

            final PrimitivePDT pdt = new PrimitivePDT(typeName, value);
            if (registry != null) {
                final Object hydrated = registry.hydratePrimitive(pdt);
                if (hydrated instanceof PrimitivePDT)
                    return (PrimitivePDT) hydrated;
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
     * A serializer that converts raw objects to {@link CompositePDT} using a registered adapter,
     * then serializes the resulting PDT in the standard CompositePdt format.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static class PdtAdapterJacksonSerializer extends StdSerializer<Object> {

        private final PDTRegistry registry;

        PdtAdapterJacksonSerializer(final PDTRegistry registry) {
            super(Object.class);
            this.registry = registry;
        }

        @Override
        public void serialize(final Object value, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException {
            final CompositePDT pdt = toPdt(value);
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
            // Convert to CompositePDT and delegate to its registered typed serializer
            final CompositePDT pdt = toPdt(value);
            serializerProvider.findTypedValueSerializer(CompositePDT.class, true, null)
                    .serialize(pdt, jsonGenerator, serializerProvider);
        }

        private CompositePDT toPdt(final Object value) throws IOException {
            final Optional<CompositePDTAdapter<?>> opt = registry.getCompositeAdapterByClass(value.getClass());
            if (!opt.isPresent()) {
                throw new IOException("No adapter found for " + value.getClass().getName());
            }
            final CompositePDTAdapter adapter = opt.get();
            final Map<String, Object> fields = adapter.toFields(value);
            return new CompositePDT(adapter.typeName(), fields);
        }
    }

    /**
     * A serializer that converts raw objects to {@link PrimitivePDT} using a registered
     * {@link PrimitivePDTAdapter}, then serializes the result in the standard PrimitivePdt format.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static class PrimitivePdtAdapterJacksonSerializer extends StdSerializer<Object> {

        private final PDTRegistry registry;

        PrimitivePdtAdapterJacksonSerializer(final PDTRegistry registry) {
            super(Object.class);
            this.registry = registry;
        }

        @Override
        public void serialize(final Object value, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException {
            final PrimitivePDT pdt = toPrimitivePdt(value);
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("type", pdt.getName());
            jsonGenerator.writeStringField("value", pdt.getValue());
            jsonGenerator.writeEndObject();
        }

        @Override
        public void serializeWithType(final Object value, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider,
                                      final org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer typeSerializer) throws IOException {
            final PrimitivePDT pdt = toPrimitivePdt(value);
            serializerProvider.findTypedValueSerializer(PrimitivePDT.class, true, null)
                    .serialize(pdt, jsonGenerator, serializerProvider);
        }

        private PrimitivePDT toPrimitivePdt(final Object value) throws IOException {
            final Optional<PrimitivePDTAdapter<?>> opt = registry.getPrimitiveAdapterByClass(value.getClass());
            if (!opt.isPresent()) {
                throw new IOException("No primitive adapter found for " + value.getClass().getName());
            }
            final PrimitivePDTAdapter adapter = opt.get();
            final String strValue = adapter.toValue(value);
            return new PrimitivePDT(adapter.typeName(), strValue);
        }
    }
}

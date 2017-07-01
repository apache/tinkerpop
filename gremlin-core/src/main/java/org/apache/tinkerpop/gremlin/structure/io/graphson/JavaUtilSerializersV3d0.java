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
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdScalarSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * GraphSON serializers for classes in {@code java.util.*} for the version 3.0 of GraphSON.
 */
final class JavaUtilSerializersV3d0 {

    private JavaUtilSerializersV3d0() {}

    ////////////////////////////// SERIALIZERS /////////////////////////////////

    final static class MapJacksonSerializer extends StdSerializer<Map> {
        public MapJacksonSerializer() {
            super(Map.class);
        }

        @Override
        public void serialize(final Map map, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            for(Map.Entry entry : (Set<Map.Entry>) map.entrySet()) {
                jsonGenerator.writeObject(entry.getKey());
                jsonGenerator.writeObject(entry.getValue());
            }
        }

        @Override
        public void serializeWithType(final Map map, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            typeSerializer.writeTypePrefixForObject(map, jsonGenerator);
            serialize(map, jsonGenerator, serializerProvider);
            typeSerializer.writeTypeSuffixForObject(map, jsonGenerator);
        }
    }

    final static class MapEntryJacksonSerializer extends StdSerializer<Map.Entry> {

        public MapEntryJacksonSerializer() {
            super(Map.Entry.class);
        }

        @Override
        public void serialize(final Map.Entry entry, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeObject(entry.getKey());
            jsonGenerator.writeObject(entry.getValue());
        }

        @Override
        public void serializeWithType(final Map.Entry entry, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            typeSerializer.writeTypePrefixForObject(entry, jsonGenerator);
            serialize(entry, jsonGenerator, serializerProvider);
            typeSerializer.writeTypeSuffixForObject(entry, jsonGenerator);
        }
    }

    ////////////////////////////// DESERIALIZERS /////////////////////////////////


    static class MapJacksonDeserializer extends StdDeserializer<Map> {

        protected MapJacksonDeserializer() {
            super(Map.class);
        }

        @Override
        public Map deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final Map<Object,Object> m = new LinkedHashMap<>();

            while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                final Object key = deserializationContext.readValue(jsonParser, Object.class);
                jsonParser.nextToken();
                final Object val = deserializationContext.readValue(jsonParser, Object.class);
                m.put(key, val);
            }

            return m;
        }
    }

    static class MapEntryJacksonDeserializer extends StdDeserializer<Map.Entry> {

        protected MapEntryJacksonDeserializer() {
            super(Map.Entry.class);
        }

        @Override
        public Map.Entry deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final Map<Object,Object> m = new HashMap<>();

            while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                final Object key = deserializationContext.readValue(jsonParser, Object.class);
                jsonParser.nextToken();
                final Object val = deserializationContext.readValue(jsonParser, Object.class);
                m.put(key, val);
            }

            return m.entrySet().iterator().next();
        }
    }
}

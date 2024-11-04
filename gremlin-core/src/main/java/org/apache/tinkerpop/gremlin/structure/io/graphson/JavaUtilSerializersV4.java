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
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * GraphSON serializers for classes in {@code java.util.*} for the version 4.0 of GraphSON.
 */
final class JavaUtilSerializersV4 {

    private JavaUtilSerializersV4() {}

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

    /**
     * Coerces {@code Map.Entry} to a {@code Map} with a single entry in it.
     */
    final static class MapEntryJacksonSerializer extends StdSerializer<Map.Entry> {

        public MapEntryJacksonSerializer() {
            super(Map.Entry.class);
        }

        @Override
        public void serialize(final Map.Entry entry, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            final Map<Object,Object> m = new HashMap<>();
            if (entry != null) m.put(entry.getKey(), entry.getValue());
            jsonGenerator.writeObject(m);
        }

        @Override
        public void serializeWithType(final Map.Entry entry, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            serialize(entry, jsonGenerator, serializerProvider);
        }
    }

    final static class SetJacksonSerializer extends StdSerializer<Set> {
        public SetJacksonSerializer() {
            super(Set.class);
        }

        @Override
        public void serialize(final Set set, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            for(Object o : set) {
                jsonGenerator.writeObject(o);
            }
        }

        @Override
        public void serializeWithType(final Set set, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            typeSerializer.writeTypePrefixForArray(set, jsonGenerator);
            serialize(set, jsonGenerator, serializerProvider);
            typeSerializer.writeTypeSuffixForArray(set, jsonGenerator);
        }
    }

    final static class ListJacksonSerializer extends StdSerializer<List> {
        public ListJacksonSerializer() {
            super(List.class);
        }

        @Override
        public void serialize(final List list, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            for(Object o : list) {
                jsonGenerator.writeObject(o);
            }
        }

        @Override
        public void serializeWithType(final List list, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            typeSerializer.writeTypePrefixForArray(list, jsonGenerator);
            serialize(list, jsonGenerator, serializerProvider);
            typeSerializer.writeTypeSuffixForArray(list, jsonGenerator);
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

    static class SetJacksonDeserializer extends StdDeserializer<Set> {

        protected SetJacksonDeserializer() {
            super(Set.class);
        }

        @Override
        public Set deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final Set<Object> s = new LinkedHashSet<>();

            while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                s.add(deserializationContext.readValue(jsonParser, Object.class));
            }

            return s;
        }
    }

    static class ListJacksonDeserializer extends StdDeserializer<List> {

        protected ListJacksonDeserializer() {
            super(List.class);
        }

        @Override
        public List deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final List<Object> s = new LinkedList<>();

            while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                s.add(deserializationContext.readValue(jsonParser, Object.class));
            }

            return s;
        }
    }
}

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

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.databind.SerializationFeature;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.ByteBufferSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;

/**
 * GraphSON serializers for classes in {@code java.util.*} for the version 2.0 of GraphSON.
 */
final class JavaUtilSerializersV2d0 {

    private JavaUtilSerializersV2d0() {}

    final static class MapEntryJacksonSerializer extends StdSerializer<Map.Entry> {

        public MapEntryJacksonSerializer() {
            super(Map.Entry.class);
        }

        @Override
        public void serialize(final Map.Entry entry, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();
            ser(entry, jsonGenerator, serializerProvider);
            jsonGenerator.writeEndObject();
        }

        @Override
        public void serializeWithType(final Map.Entry entry, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            typeSerializer.writeTypePrefixForObject(entry, jsonGenerator);
            ser(entry, jsonGenerator, serializerProvider);
            typeSerializer.writeTypeSuffixForObject(entry, jsonGenerator);
        }

        private static void ser(final Map.Entry entry, final JsonGenerator jsonGenerator,
                                final SerializerProvider serializerProvider) throws IOException {
            // this treatment of keys is consistent with the current GraphSONKeySerializer which extends the
            // StdKeySerializer
            final Object key = entry.getKey();
            final Class cls = key.getClass();
            String k;
            if (cls == String.class)
                k = (String) key;
            else if (Element.class.isAssignableFrom(cls))
                k = ((Element) key).id().toString();
            else if(Date.class.isAssignableFrom(cls)) {
                if (serializerProvider.isEnabled(SerializationFeature.WRITE_DATE_KEYS_AS_TIMESTAMPS))
                    k = String.valueOf(((Date) key).getTime());
                else
                    k = serializerProvider.getConfig().getDateFormat().format((Date) key);
            } else if(cls == Class.class)
                k = ((Class) key).getName();
            else
                k = key.toString();

            serializerProvider.defaultSerializeField(k, entry.getValue(), jsonGenerator);
        }
    }
}

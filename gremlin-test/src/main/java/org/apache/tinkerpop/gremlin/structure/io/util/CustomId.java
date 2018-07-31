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

import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.AbstractObjectDeserializer;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TinkerPopJacksonModule;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerationException;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdScalarSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final CustomId customId = (CustomId) o;

        if (!cluster.equals(customId.cluster)) return false;
        return elementId.equals(customId.elementId);
    }

    @Override
    public int hashCode() {
        int result = cluster.hashCode();
        result = 31 * result + elementId.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return cluster + ":" + elementId;
    }

    public static class CustomIdJacksonSerializerV1d0 extends StdSerializer<CustomId> {
        public CustomIdJacksonSerializerV1d0() {
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

    public static class CustomIdJacksonSerializerV2d0 extends StdSerializer<CustomId> {
        public CustomIdJacksonSerializerV2d0() {
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
            typeSerializer.writeTypePrefixForScalar(customId, jsonGenerator);
            final Map<String, Object> m = new HashMap<>();
            m.put("cluster", customId.getCluster());
            m.put("elementId", customId.getElementId());
            jsonGenerator.writeObject(m);
            typeSerializer.writeTypeSuffixForScalar(customId, jsonGenerator);
        }
    }

    public static class CustomIdJacksonDeserializerV2d0 extends AbstractObjectDeserializer<CustomId> {
        public CustomIdJacksonDeserializerV2d0() {
            super(CustomId.class);
        }

        @Override
        public CustomId createObject(final Map data) {
            return new CustomId((String) data.get("cluster"), (UUID) data.get("elementId"));
        }
    }

    public static class CustomIdTinkerPopJacksonModuleV2d0 extends TinkerPopJacksonModule {

        private static final Map<Class, String> TYPE_DEFINITIONS = Collections.unmodifiableMap(
                new LinkedHashMap<Class, String>() {{
                    put(CustomId.class, "id");
                }});

        public CustomIdTinkerPopJacksonModuleV2d0() {
            super("custom");
            addSerializer(CustomId.class, new CustomIdJacksonSerializerV2d0());
            addDeserializer(CustomId.class, new CustomIdJacksonDeserializerV2d0());
        }

        @Override
        public Map<Class, String> getTypeDefinitions() {
            return TYPE_DEFINITIONS;
        }

        @Override
        public String getTypeNamespace() {
            return "simple";
        }
    }

    public static class CustomIdJacksonSerializerV3d0 extends StdScalarSerializer<CustomId> {
        public CustomIdJacksonSerializerV3d0() {
            super(CustomId.class);
        }

        @Override
        public void serialize(final CustomId customId, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException {
            final Map<String, Object> m = new HashMap<>();
            m.put("cluster", customId.getCluster());
            m.put("elementId", customId.getElementId());
            jsonGenerator.writeObject(m);
        }
    }

    public static class CustomIdJacksonDeserializerV3d0 extends StdDeserializer<CustomId> {
        public CustomIdJacksonDeserializerV3d0() {
            super(CustomId.class);
        }

        @Override
        public CustomId deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final Map<String, Object> data = deserializationContext.readValue(jsonParser, Map.class);
            return new CustomId((String) data.get("cluster"), (UUID) data.get("elementId"));
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

    public static class CustomIdTinkerPopJacksonModuleV3d0 extends TinkerPopJacksonModule {

        private static final Map<Class, String> TYPE_DEFINITIONS = Collections.unmodifiableMap(
                new LinkedHashMap<Class, String>() {{
                    put(CustomId.class, "id");
                }});

        public CustomIdTinkerPopJacksonModuleV3d0() {
            super("custom");
            addSerializer(CustomId.class, new CustomIdJacksonSerializerV3d0());
            addDeserializer(CustomId.class, new CustomIdJacksonDeserializerV3d0());
        }

        @Override
        public Map<Class, String> getTypeDefinitions() {
            return TYPE_DEFINITIONS;
        }

        @Override
        public String getTypeNamespace() {
            return "simple";
        }
    }

    public static class CustomIdIoRegistry extends AbstractIoRegistry {

        private static final CustomIdIoRegistry INSTANCE = new CustomIdIoRegistry();

        private CustomIdIoRegistry() {
            register(GryoIo.class, CustomId.class, null);
            register(GraphSONIo.class, null, new CustomIdTinkerPopJacksonModuleV3d0());
        }

        public static CustomIdIoRegistry instance() {
            return INSTANCE;
        }
    }
}

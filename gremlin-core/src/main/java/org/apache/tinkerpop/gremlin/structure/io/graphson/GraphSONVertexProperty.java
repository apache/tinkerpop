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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Serializes the {@link VertexProperty} but does so without a label.  This serializer should be used when the
 * property is serialized as part of a {@link Map} where the label isn't required.  In those cases, the key is
 * the same as the label and therefore redundant.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONVertexProperty {
    private final VertexProperty toSerialize;

    public GraphSONVertexProperty(final VertexProperty toSerialize) {
        this.toSerialize = toSerialize;
    }

    public VertexProperty getToSerialize() {
        return toSerialize;
    }

    static class GraphSONVertexPropertySerializer extends StdSerializer<GraphSONVertexProperty> {
        public GraphSONVertexPropertySerializer() {
            super(GraphSONVertexProperty.class);
        }

        @Override
        public void serialize(final GraphSONVertexProperty property, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(property, jsonGenerator);
        }

        @Override
        public void serializeWithType(final GraphSONVertexProperty property, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(property, jsonGenerator);
        }

        private static void ser(final GraphSONVertexProperty graphSONVertexProperty, final JsonGenerator jsonGenerator) throws IOException {
            final VertexProperty property = graphSONVertexProperty.getToSerialize();
            final Map<String, Object> m = new HashMap<>();
            m.put(GraphSONTokens.ID, property.id());
            m.put(GraphSONTokens.VALUE, property.value());
            m.put(GraphSONTokens.PROPERTIES, props(property));

            jsonGenerator.writeObject(m);
        }

        private static Map<String, Object> props(final VertexProperty property) {
            if (property instanceof DetachedVertexProperty) {
                try {
                    return IteratorUtils.collectMap(property.iterators().propertyIterator(), Property::key, Property::value);
                } catch (UnsupportedOperationException uoe) {
                    return new HashMap<>();
                }
            } else {
                return (property.graph().features().vertex().supportsMetaProperties()) ?
                        IteratorUtils.collectMap(property.iterators().propertyIterator(), Property::key, Property::value) :
                        new HashMap<>();
            }
        }
    }
}

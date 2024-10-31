/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdScalarSerializer;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class TraversalSerializersV4 {

    private TraversalSerializersV4() {
    }

    /////////////////
    // SERIALIZERS //
    ////////////////

    static class EnumJacksonSerializer extends StdScalarSerializer<Enum> {

        public EnumJacksonSerializer() {
            super(Enum.class);
        }

        @Override
        public void serialize(final Enum enumInstance, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeString(enumInstance.name());
        }

    }

    final static class BulkSetJacksonSerializer extends StdScalarSerializer<BulkSet> {

        public BulkSetJacksonSerializer() {
            super(BulkSet.class);
        }

        @Override
        public void serialize(final BulkSet bulkSet, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartArray();
            for (Object element : bulkSet) {
                jsonGenerator.writeObject(element);
            }
            jsonGenerator.writeEndArray();
        }
    }

    ///////////////////
    // DESERIALIZERS //
    //////////////////

    final static class EnumJacksonDeserializer<A extends Enum> extends StdDeserializer<A> {

        public EnumJacksonDeserializer(final Class<A> enumClass) {
            super(enumClass);
        }

        @Override
        public A deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final Class<A> enumClass = (Class<A>) this._valueClass;
            final String enumName = jsonParser.getText();
            for (final Enum a : enumClass.getEnumConstants()) {
                if (a.name().equals(enumName))
                    return (A) a;
            }
            throw new IOException("Unknown enum type: " + enumClass);
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }
}

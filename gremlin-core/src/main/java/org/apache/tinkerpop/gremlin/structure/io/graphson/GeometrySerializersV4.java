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

import org.apache.tinkerpop.gremlin.structure.geo.Geometry;
import org.apache.tinkerpop.gremlin.structure.geo.Model;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdScalarSerializer;

import java.io.IOException;
import java.util.Map;

/**
 * GraphSON V4 serializers for {@link Geometry}, written as an object holding the ISO WKT of the
 * value and the name of its {@link Model}.
 * <p>
 * The model carries the coordinate interpretation rather than the SRID, so a geometry whose
 * {@link Geometry#model()} is {@link Model#OTHER} cannot be written: its SRID names a coordinate
 * system that the two model names do not describe. A value that is read must name
 * {@link Model#CARTESIAN} or {@link Model#GEOGRAPHIC}, which fixes its SRID at 0 or 4326.
 */
final class GeometrySerializersV4 {

    /**
     * The field holding the ISO WKT of the geometry.
     */
    private static final String WKT = "wkt";

    /**
     * The field holding the name of the geometry's {@link Model}.
     */
    private static final String MODEL = "model";

    /**
     * The EWKT prefix, which carries an SRID of its own and so is not accepted in {@link #WKT}.
     */
    private static final String EWKT_PREFIX = "SRID=";

    private GeometrySerializersV4() {
    }

    final static class GeometryJacksonSerializer extends StdScalarSerializer<Geometry> {

        public GeometryJacksonSerializer() {
            super(Geometry.class);
        }

        @Override
        public void serialize(final Geometry value, final JsonGenerator gen,
                              final SerializerProvider serializerProvider) throws IOException {
            final Model model = value.model();
            if (model == Model.OTHER) {
                throw new IOException("GraphSON carries the model rather than the SRID, so a geometry with SRID "
                        + value.srid() + " cannot be written");
            }

            gen.writeStartObject();
            gen.writeStringField(WKT, value.toWKT());
            gen.writeStringField(MODEL, model.name());
            gen.writeEndObject();
        }
    }

    final static class GeometryJacksonDeserializer extends AbstractObjectDeserializer<Geometry> {

        public GeometryJacksonDeserializer() {
            super(Geometry.class);
        }

        @Override
        public Geometry deserialize(final JsonParser jsonParser,
                                    final DeserializationContext deserializationContext) throws IOException {
            try {
                return super.deserialize(jsonParser, deserializationContext);
            } catch (IllegalArgumentException iae) {
                throw new IOException(iae.getMessage(), iae);
            }
        }

        /**
         * Builds the geometry from the {@code wkt} and {@code model} fields. Any other field is
         * ignored, as {@link AbstractObjectDeserializer} leaves unknown fields in the map.
         */
        @Override
        public Geometry createObject(final Map<String, Object> data) {
            final String wkt = requireString(data, WKT);
            final String modelName = requireString(data, MODEL);

            final Model model;
            try {
                model = Model.valueOf(modelName);
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException(modelMustBeKnown(modelName), iae);
            }
            // fromWkt(String, Model) also rejects OTHER, but points at fromWkt(wkt, srid) instead of
            // naming the two models a GraphSON body can carry
            if (model == Model.OTHER) {
                throw new IllegalArgumentException(modelMustBeKnown(modelName));
            }

            // fromWkt(String, Model) accepts an EWKT prefix whose SRID agrees with the model, but the
            // GraphSON body carries the coordinate system in its model field alone
            if (wkt.trim().regionMatches(true, 0, EWKT_PREFIX, 0, EWKT_PREFIX.length())) {
                throw new IllegalArgumentException("Geometry wkt must be ISO WKT without an EWKT " + EWKT_PREFIX
                        + " prefix");
            }

            return Geometry.fromWkt(wkt, model);
        }

        private static String modelMustBeKnown(final String modelName) {
            return "Geometry model must be " + Model.CARTESIAN + " or " + Model.GEOGRAPHIC + " but was " + modelName;
        }

        private static String requireString(final Map<String, Object> data, final String field) {
            final Object value = data.get(field);
            if (!(value instanceof String)) {
                throw new IllegalArgumentException("Geometry requires a string " + field + " field");
            }
            return (String) value;
        }
    }
}

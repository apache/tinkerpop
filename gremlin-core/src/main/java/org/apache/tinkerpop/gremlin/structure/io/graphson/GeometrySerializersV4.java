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
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdScalarSerializer;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;

/**
 * GraphSON V4 serializers for {@link Geometry}, written as an object holding the ISO WKT of the
 * value and its SRID as a JSON integer.
 * <p>
 * The SRID is carried as given, so every geometry round trips, including one whose
 * {@link Geometry#model()} is {@link org.apache.tinkerpop.gremlin.structure.geo.Model#OTHER}.
 * The model is not written because it follows from the SRID.
 */
final class GeometrySerializersV4 {

    /**
     * The field holding the ISO WKT of the geometry.
     */
    private static final String WKT = "wkt";

    /**
     * The field holding the SRID of the geometry.
     */
    private static final String SRID = "srid";

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
            gen.writeStartObject();
            gen.writeStringField(WKT, value.toWKT());
            gen.writeNumberField(SRID, value.srid());
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
         * Builds the geometry from the {@code wkt} and {@code srid} fields. Any other field, such
         * as the {@code model} field of earlier drafts, is ignored, as
         * {@link AbstractObjectDeserializer} leaves unknown fields in the map.
         */
        @Override
        public Geometry createObject(final Map<String, Object> data) {
            final String wkt = requireString(data, WKT);
            final int srid = requireSrid(data);

            // fromWkt(String, int) accepts an EWKT prefix whose SRID agrees with the srid argument, but the
            // GraphSON body carries the SRID in its srid field alone
            if (wkt.trim().regionMatches(true, 0, EWKT_PREFIX, 0, EWKT_PREFIX.length())) {
                throw new IllegalArgumentException("Geometry wkt must be ISO WKT without an EWKT " + EWKT_PREFIX
                        + " prefix");
            }

            return Geometry.fromWkt(wkt, srid);
        }

        private static String requireString(final Map<String, Object> data, final String field) {
            final Object value = data.get(field);
            if (!(value instanceof String)) {
                throw new IllegalArgumentException("Geometry requires a string " + field + " field");
            }
            return (String) value;
        }

        /**
         * Reads the {@code srid} field, which has to be a JSON integer that fits in a 32-bit signed
         * integer. A floating point number, a string or a boolean is refused rather than converted.
         */
        private static int requireSrid(final Map<String, Object> data) {
            final Object value = data.get(SRID);
            if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
                return ((Number) value).intValue();
            }
            if (value instanceof Long || value instanceof BigInteger) {
                final BigInteger big = value instanceof Long ? BigInteger.valueOf((Long) value) : (BigInteger) value;
                if (big.bitLength() < Integer.SIZE) {
                    return big.intValue();
                }
                throw new IllegalArgumentException("Geometry srid must fit in a 32-bit signed integer but was "
                        + value);
            }
            throw new IllegalArgumentException("Geometry requires an integer " + SRID + " field");
        }
    }
}

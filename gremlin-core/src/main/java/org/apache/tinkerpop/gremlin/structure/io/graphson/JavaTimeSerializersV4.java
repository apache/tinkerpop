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
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * GraphSON serializers for classes in {@code java.time.*} for the version 4.0 of GraphSON.
 */
final class JavaTimeSerializersV4 {

    private JavaTimeSerializersV4() {}

    /**
     * Base class for serializing the {@code java.time.*} to ISO-8061 formats.
     */
    static abstract class AbstractJavaTimeSerializer<T> extends StdSerializer<T> {

        public AbstractJavaTimeSerializer(final Class<T> clazz) {
            super(clazz);
        }

        @Override
        public void serialize(final T value, final JsonGenerator gen,
                              final SerializerProvider serializerProvider) throws IOException {
            gen.writeString(value.toString());
        }

        @Override
        public void serializeWithType(final T value, final JsonGenerator gen,
                                      final SerializerProvider serializers, final TypeSerializer typeSer) throws IOException {
            typeSer.writeTypePrefixForScalar(value, gen);
            gen.writeString(value.toString());
            typeSer.writeTypeSuffixForScalar(value, gen);
        }
    }
    /**
     * Base class for serializing the {@code java.time.*} from ISO-8061 formats.
     */
    abstract static class AbstractJavaTimeJacksonDeserializer<T> extends StdDeserializer<T> {
        public AbstractJavaTimeJacksonDeserializer(final Class<T> clazz) {
            super(clazz);
        }

        public abstract T parse(final String val);

        @Override
        public T deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {
            return parse(jsonParser.getText());
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

    final static class DurationJacksonSerializer extends AbstractJavaTimeSerializer<Duration> {

        public DurationJacksonSerializer() {
            super(Duration.class);
        }
    }

    final static class DurationJacksonDeserializer extends AbstractJavaTimeJacksonDeserializer<Duration> {
        public DurationJacksonDeserializer() {
            super(Duration.class);
        }

        @Override
        public Duration parse(final String val) {
            return Duration.parse(val);
        }
    }

    final static class OffsetDateTimeJacksonSerializer extends AbstractJavaTimeSerializer<OffsetDateTime> {

        public OffsetDateTimeJacksonSerializer() {
            super(OffsetDateTime.class);
        }
    }

    final static class OffsetDateTimeJacksonDeserializer extends AbstractJavaTimeJacksonDeserializer<OffsetDateTime> {
        public OffsetDateTimeJacksonDeserializer() {
            super(OffsetDateTime.class);
        }

        @Override
        public OffsetDateTime parse(final String val) {
            return OffsetDateTime.parse(val);
        }
    }
}

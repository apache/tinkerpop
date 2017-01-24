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
 * GraphSON serializers for classes in {@code java.time.*} for the version 3.0 of GraphSON.
 */
final class JavaTimeSerializersV3d0 {

    private JavaTimeSerializersV3d0() {}

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

    final static class InstantJacksonSerializer extends AbstractJavaTimeSerializer<Instant> {

        public InstantJacksonSerializer() {
            super(Instant.class);
        }
    }

    final static class InstantJacksonDeserializer extends AbstractJavaTimeJacksonDeserializer<Instant> {
        public InstantJacksonDeserializer() {
            super(Instant.class);
        }

        @Override
        public Instant parse(final String val) {
            return Instant.parse(val);
        }
    }

    final static class LocalDateJacksonSerializer extends AbstractJavaTimeSerializer<LocalDate> {

        public LocalDateJacksonSerializer() {
            super(LocalDate.class);
        }
    }

    final static class LocalDateJacksonDeserializer extends AbstractJavaTimeJacksonDeserializer<LocalDate> {
        public LocalDateJacksonDeserializer() {
            super(LocalDate.class);
        }

        @Override
        public LocalDate parse(final String val) {
            return LocalDate.parse(val);
        }
    }

    final static class LocalDateTimeJacksonSerializer extends AbstractJavaTimeSerializer<LocalDateTime> {

        public LocalDateTimeJacksonSerializer() {
            super(LocalDateTime.class);
        }
    }

    final static class LocalDateTimeJacksonDeserializer extends AbstractJavaTimeJacksonDeserializer<LocalDateTime> {
        public LocalDateTimeJacksonDeserializer() {
            super(LocalDateTime.class);
        }

        @Override
        public LocalDateTime parse(final String val) {
            return LocalDateTime.parse(val);
        }
    }

    final static class LocalTimeJacksonSerializer extends AbstractJavaTimeSerializer<LocalTime> {

        public LocalTimeJacksonSerializer() {
            super(LocalTime.class);
        }
    }

    final static class LocalTimeJacksonDeserializer extends AbstractJavaTimeJacksonDeserializer<LocalTime> {
        public LocalTimeJacksonDeserializer() {
            super(LocalTime.class);
        }

        @Override
        public LocalTime parse(final String val) {
            return LocalTime.parse(val);
        }
    }

    final static class MonthDayJacksonSerializer extends AbstractJavaTimeSerializer<MonthDay> {

        public MonthDayJacksonSerializer() {
            super(MonthDay.class);
        }
    }

    final static class MonthDayJacksonDeserializer extends AbstractJavaTimeJacksonDeserializer<MonthDay> {
        public MonthDayJacksonDeserializer() {
            super(MonthDay.class);
        }

        @Override
        public MonthDay parse(final String val) {
            return MonthDay.parse(val);
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

    final static class OffsetTimeJacksonSerializer extends AbstractJavaTimeSerializer<OffsetTime> {

        public OffsetTimeJacksonSerializer() {
            super(OffsetTime.class);
        }
    }

    final static class OffsetTimeJacksonDeserializer extends AbstractJavaTimeJacksonDeserializer<OffsetTime> {
        public OffsetTimeJacksonDeserializer() {
            super(OffsetTime.class);
        }

        @Override
        public OffsetTime parse(final String val) {
            return OffsetTime.parse(val);
        }
    }

    final static class PeriodJacksonSerializer extends AbstractJavaTimeSerializer<Period> {

        public PeriodJacksonSerializer() {
            super(Period.class);
        }
    }

    final static class PeriodJacksonDeserializer extends AbstractJavaTimeJacksonDeserializer<Period> {
        public PeriodJacksonDeserializer() {
            super(Period.class);
        }

        @Override
        public Period parse(final String val) {
            return Period.parse(val);
        }
    }

    final static class YearJacksonSerializer extends AbstractJavaTimeSerializer<Year> {

        public YearJacksonSerializer() {
            super(Year.class);
        }
    }

    final static class YearJacksonDeserializer extends AbstractJavaTimeJacksonDeserializer<Year> {
        public YearJacksonDeserializer() {
            super(Year.class);
        }

        @Override
        public Year parse(final String val) {
            return Year.parse(val);
        }
    }

    final static class YearMonthJacksonSerializer extends AbstractJavaTimeSerializer<YearMonth> {

        public YearMonthJacksonSerializer() {
            super(YearMonth.class);
        }
    }

    final static class YearMonthJacksonDeserializer extends AbstractJavaTimeJacksonDeserializer<YearMonth> {
        public YearMonthJacksonDeserializer() {
            super(YearMonth.class);
        }

        @Override
        public YearMonth parse(final String val) {
            return YearMonth.parse(val);
        }
    }

    final static class ZonedDateTimeJacksonSerializer extends AbstractJavaTimeSerializer<ZonedDateTime> {

        public ZonedDateTimeJacksonSerializer() {
            super(ZonedDateTime.class);
        }
    }

    final static class ZonedDateTimeJacksonDeserializer extends AbstractJavaTimeJacksonDeserializer<ZonedDateTime> {
        public ZonedDateTimeJacksonDeserializer() {
            super(ZonedDateTime.class);
        }

        @Override
        public ZonedDateTime parse(final String val) {
            return ZonedDateTime.parse(val);
        }
    }

    final static class ZoneOffsetJacksonSerializer extends AbstractJavaTimeSerializer<ZoneOffset> {

        public ZoneOffsetJacksonSerializer() {
            super(ZoneOffset.class);
        }
    }

    final static class ZoneOffsetJacksonDeserializer extends AbstractJavaTimeJacksonDeserializer<ZoneOffset> {
        public ZoneOffsetJacksonDeserializer() {
            super(ZoneOffset.class);
        }

        @Override
        public ZoneOffset parse(final String val) {
            return ZoneOffset.of(val);
        }
    }
}

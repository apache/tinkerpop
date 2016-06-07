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
package org.apache.tinkerpop.gremlin.structure.io.gryo;

import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.InputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.OutputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.SerializerShim;

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
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * Serializers for classes in the {@code java.time} package.
 */
final class JavaTimeSerializers {

    private JavaTimeSerializers() {}

    /**
     * Serializer for the {@link Duration} class.
     */
    final static class DurationSerializer implements SerializerShim<Duration> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final Duration duration) {
            output.writeLong(duration.toNanos());
        }

        @Override
        public <I extends InputShim> Duration read(final KryoShim<I, ?> kryo, final I input, final Class<Duration> durationClass) {
            return Duration.ofNanos(input.readLong());
        }
    }

    /**
     * Serializer for the {@link Instant} class.
     */
    final static class InstantSerializer implements SerializerShim<Instant> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final Instant instant) {
            output.writeLong(instant.getEpochSecond());
            output.writeInt(instant.getNano());
        }

        @Override
        public <I extends InputShim> Instant read(final KryoShim<I, ?> kryo, final I input, final Class<Instant> aClass) {
            return Instant.ofEpochSecond(input.readLong(), input.readInt());
        }
    }

    /**
     * Serializer for the {@link LocalDate} class.
     */
    final static class LocalDateSerializer implements SerializerShim<LocalDate> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final LocalDate localDate) {
            output.writeLong(localDate.toEpochDay());
        }

        @Override
        public <I extends InputShim> LocalDate read(final KryoShim<I, ?> kryo, final I input, final Class<LocalDate> clazz) {
            return LocalDate.ofEpochDay(input.readLong());
        }
    }

    /**
     * Serializer for the {@link LocalDateTime} class.
     */
    final static class LocalDateTimeSerializer implements SerializerShim<LocalDateTime> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final LocalDateTime localDateTime) {
            output.writeInt(localDateTime.getYear());
            output.writeInt(localDateTime.getMonthValue());
            output.writeInt(localDateTime.getDayOfMonth());
            output.writeInt(localDateTime.getHour());
            output.writeInt(localDateTime.getMinute());
            output.writeInt(localDateTime.getSecond());
            output.writeInt(localDateTime.getNano());
        }

        @Override
        public <I extends InputShim> LocalDateTime read(final KryoShim<I, ?> kryo, final I input, final Class<LocalDateTime> clazz) {
            return LocalDateTime.of(input.readInt(), input.readInt(), input.readInt(), input.readInt(), input.readInt(), input.readInt(), input.readInt());
        }
    }

    /**
     * Serializer for the {@link LocalTime} class.
     */
    final static class LocalTimeSerializer implements SerializerShim<LocalTime> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final LocalTime localTime) {
            output.writeLong(localTime.toNanoOfDay());
        }

        @Override
        public <I extends InputShim> LocalTime read(final KryoShim<I, ?> kryo, final I input, final Class<LocalTime> clazz) {
            return LocalTime.ofNanoOfDay(input.readLong());
        }
    }

    /**
     * Serializer for the {@link MonthDay} class.
     */
    final static class MonthDaySerializer implements SerializerShim<MonthDay> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final MonthDay monthDay) {
            output.writeInt(monthDay.getMonthValue());
            output.writeInt(monthDay.getDayOfMonth());
        }

        @Override
        public <I extends InputShim> MonthDay read(final KryoShim<I, ?> kryo, final I input, final Class<MonthDay> clazz) {
            return MonthDay.of(input.readInt(), input.readInt());
        }
    }

    /**
     * Serializer for the {@link OffsetDateTime} class.
     */
    final static class OffsetDateTimeSerializer implements SerializerShim<OffsetDateTime> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final OffsetDateTime offsetDateTime) {
            kryo.writeObject(output, offsetDateTime.toLocalDateTime());
            kryo.writeObject(output, offsetDateTime.getOffset());
        }

        @Override
        public <I extends InputShim> OffsetDateTime read(final KryoShim<I, ?> kryo, final I input, final Class<OffsetDateTime> clazz) {
            return OffsetDateTime.of(kryo.readObject(input, LocalDateTime.class), kryo.readObject(input, ZoneOffset.class));
        }
    }

    /**
     * Serializer for the {@link OffsetTime} class.
     */
    final static class OffsetTimeSerializer implements SerializerShim<OffsetTime> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final OffsetTime offsetTime) {
            kryo.writeObject(output, offsetTime.toLocalTime());
            kryo.writeObject(output, offsetTime.getOffset());
        }

        @Override
        public <I extends InputShim> OffsetTime read(final KryoShim<I, ?> kryo, final I input, final Class<OffsetTime> clazz) {
            return OffsetTime.of(kryo.readObject(input, LocalTime.class), kryo.readObject(input, ZoneOffset.class));
        }
    }

    /**
     * Serializer for the {@link Period} class.
     */
    final static class PeriodSerializer implements SerializerShim<Period> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final Period period) {
            output.writeInt(period.getYears());
            output.writeInt(period.getMonths());
            output.writeInt(period.getDays());
        }

        @Override
        public <I extends InputShim> Period read(final KryoShim<I, ?> kryo, final I input, final Class<Period> clazz) {
            return Period.of(input.readInt(), input.readInt(), input.readInt());
        }
    }

    /**
     * Serializer for the {@link Year} class.
     */
    final static class YearSerializer implements SerializerShim<Year> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final Year year) {
            output.writeInt(year.getValue());
        }

        @Override
        public <I extends InputShim> Year read(final KryoShim<I, ?> kryo, final I input, final Class<Year> clazz) {
            return Year.of(input.readInt());
        }
    }

    /**
     * Serializer for the {@link YearMonth} class.
     */
    final static class YearMonthSerializer implements SerializerShim<YearMonth> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final YearMonth monthDay) {
            output.writeInt(monthDay.getYear());
            output.writeInt(monthDay.getMonthValue());
        }

        @Override
        public <I extends InputShim> YearMonth read(final KryoShim<I, ?> kryo, final I input, final Class<YearMonth> clazz) {
            return YearMonth.of(input.readInt(), input.readInt());
        }
    }

    /**
     * Serializer for the {@link ZonedDateTime} class.
     */
    final static class ZonedDateTimeSerializer implements SerializerShim<ZonedDateTime> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final ZonedDateTime zonedDateTime) {
            output.writeInt(zonedDateTime.getYear());
            output.writeInt(zonedDateTime.getMonthValue());
            output.writeInt(zonedDateTime.getDayOfMonth());
            output.writeInt(zonedDateTime.getHour());
            output.writeInt(zonedDateTime.getMinute());
            output.writeInt(zonedDateTime.getSecond());
            output.writeInt(zonedDateTime.getNano());
            output.writeString(zonedDateTime.getZone().getId());
        }

        @Override
        public <I extends InputShim> ZonedDateTime read(final KryoShim<I, ?> kryo, final I input, final Class<ZonedDateTime> clazz) {
            return ZonedDateTime.of(input.readInt(), input.readInt(), input.readInt(),
                    input.readInt(), input.readInt(), input.readInt(), input.readInt(),
                    ZoneId.of(input.readString()));
        }
    }

    /**
     * Serializer for the {@link ZoneOffset} class.
     */
    final static class ZoneOffsetSerializer implements SerializerShim<ZoneOffset> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final ZoneOffset zoneOffset) {
            output.writeString(zoneOffset.getId());
        }

        @Override
        public <I extends InputShim> ZoneOffset read(final KryoShim<I, ?> kryo, final I input, final Class<ZoneOffset> clazz) {
            return ZoneOffset.of(input.readString());
        }
    }
}

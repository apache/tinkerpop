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

import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

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
    final static class DurationSerializer extends Serializer<Duration>
    {
        @Override
        public void write(final Kryo kryo, final Output output, final Duration duration)
        {
            output.writeLong(duration.toNanos());
        }

        @Override
        public Duration read(final Kryo kryo, final Input input, final Class<Duration> durationClass)
        {
            return Duration.ofNanos(input.readLong());
        }
    }

    /**
     * Serializer for the {@link Instant} class.
     */
    final static class InstantSerializer extends Serializer<Instant>
    {
        @Override
        public void write(Kryo kryo, Output output, Instant instant)
        {
            output.writeLong(instant.getEpochSecond());
            output.writeInt(instant.getNano());
        }

        @Override
        public Instant read(Kryo kryo, Input input, Class<Instant> aClass)
        {
            return Instant.ofEpochSecond(input.readLong(), input.readInt());
        }
    }

    /**
     * Serializer for the {@link LocalDate} class.
     */
    final static class LocalDateSerializer extends Serializer<LocalDate>
    {
        @Override
        public void write(final Kryo kryo, final Output output, final LocalDate localDate)
        {
            output.writeLong(localDate.toEpochDay());
        }

        @Override
        public LocalDate read(final Kryo kryo, final Input input, final Class<LocalDate> clazz)
        {
            return LocalDate.ofEpochDay(input.readLong());
        }
    }

    /**
     * Serializer for the {@link LocalDateTime} class.
     */
    final static class LocalDateTimeSerializer extends Serializer<LocalDateTime>
    {
        @Override
        public void write(final Kryo kryo, final Output output, final LocalDateTime localDateTime)
        {
            output.writeInt(localDateTime.getYear());
            output.writeInt(localDateTime.getMonthValue());
            output.writeInt(localDateTime.getDayOfMonth());
            output.writeInt(localDateTime.getHour());
            output.writeInt(localDateTime.getMinute());
            output.writeInt(localDateTime.getSecond());
            output.writeInt(localDateTime.getNano());
        }

        @Override
        public LocalDateTime read(final Kryo kryo, final Input input, final Class<LocalDateTime> clazz)
        {
            return LocalDateTime.of(input.readInt(), input.readInt(), input.readInt(), input.readInt(), input.readInt(), input.readInt(), input.readInt());
        }
    }

    /**
     * Serializer for the {@link LocalTime} class.
     */
    final static class LocalTimeSerializer extends Serializer<LocalTime>
    {
        @Override
        public void write(final Kryo kryo, final Output output, final LocalTime localTime)
        {
            output.writeLong(localTime.toNanoOfDay());
        }

        @Override
        public LocalTime read(final Kryo kryo, final Input input, final Class<LocalTime> clazz)
        {
            return LocalTime.ofNanoOfDay(input.readLong());
        }
    }

    /**
     * Serializer for the {@link MonthDay} class.
     */
    final static class MonthDaySerializer extends Serializer<MonthDay>
    {
        @Override
        public void write(final Kryo kryo, final Output output, final MonthDay monthDay)
        {
            output.writeInt(monthDay.getMonthValue());
            output.writeInt(monthDay.getDayOfMonth());
        }

        @Override
        public MonthDay read(final Kryo kryo, final Input input, final Class<MonthDay> clazz)
        {
            return MonthDay.of(input.readInt(), input.readInt());
        }
    }

    /**
     * Serializer for the {@link OffsetDateTime} class.
     */
    final static class OffsetDateTimeSerializer extends Serializer<OffsetDateTime>
    {
        @Override
        public void write(final Kryo kryo, final Output output, final OffsetDateTime offsetDateTime)
        {
            kryo.writeObject(output, offsetDateTime.toLocalDateTime());
            kryo.writeObject(output, offsetDateTime.getOffset());
        }

        @Override
        public OffsetDateTime read(final Kryo kryo, final Input input, final Class<OffsetDateTime> clazz)
        {
            return OffsetDateTime.of(kryo.readObject(input, LocalDateTime.class), kryo.readObject(input, ZoneOffset.class));
        }
    }

    /**
     * Serializer for the {@link OffsetTime} class.
     */
    final static class OffsetTimeSerializer extends Serializer<OffsetTime>
    {
        @Override
        public void write(final Kryo kryo, final Output output, final OffsetTime offsetTime)
        {
            kryo.writeObject(output, offsetTime.toLocalTime());
            kryo.writeObject(output, offsetTime.getOffset());
        }

        @Override
        public OffsetTime read(final Kryo kryo, final Input input, final Class<OffsetTime> clazz)
        {
            return OffsetTime.of(kryo.readObject(input, LocalTime.class), kryo.readObject(input, ZoneOffset.class));
        }
    }

    /**
     * Serializer for the {@link Period} class.
     */
    final static class PeriodSerializer extends Serializer<Period>
    {
        @Override
        public void write(final Kryo kryo, final Output output, final Period period)
        {
            output.writeInt(period.getYears());
            output.writeInt(period.getMonths());
            output.writeInt(period.getDays());
        }

        @Override
        public Period read(final Kryo kryo, final Input input, final Class<Period> clazz)
        {
            return Period.of(input.readInt(), input.readInt(), input.readInt());
        }
    }

    /**
     * Serializer for the {@link Year} class.
     */
    final static class YearSerializer extends Serializer<Year>
    {
        @Override
        public void write(final Kryo kryo, final Output output, final Year year)
        {
            output.writeInt(year.getValue());
        }

        @Override
        public Year read(final Kryo kryo, final Input input, final Class<Year> clazz)
        {
            return Year.of(input.readInt());
        }
    }

    /**
     * Serializer for the {@link YearMonth} class.
     */
    final static class YearMonthSerializer extends Serializer<YearMonth>
    {
        @Override
        public void write(final Kryo kryo, final Output output, final YearMonth monthDay)
        {
            output.writeInt(monthDay.getYear());
            output.writeInt(monthDay.getMonthValue());
        }

        @Override
        public YearMonth read(final Kryo kryo, final Input input, final Class<YearMonth> clazz)
        {
            return YearMonth.of(input.readInt(), input.readInt());
        }
    }

    /**
     * Serializer for the {@link ZonedDateTime} class.
     */
    final static class ZonedDateTimeSerializer extends Serializer<ZonedDateTime>
    {
        @Override
        public void write(final Kryo kryo, final Output output, final ZonedDateTime zonedDateTime)
        {
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
        public ZonedDateTime read(final Kryo kryo, final Input input, final Class<ZonedDateTime> clazz)
        {
            return ZonedDateTime.of(input.readInt(), input.readInt(), input.readInt(),
                    input.readInt(), input.readInt(), input.readInt(), input.readInt(),
                    ZoneId.of(input.readString()));
        }
    }

    /**
     * Serializer for the {@link ZoneOffset} class.
     */
    final static class ZoneOffsetSerializer extends Serializer<ZoneOffset>
    {
        @Override
        public void write(final Kryo kryo, final Output output, final ZoneOffset zoneOffset)
        {
            output.writeString(zoneOffset.getId());
        }

        @Override
        public ZoneOffset read(final Kryo kryo, final Input input, final Class<ZoneOffset> clazz)
        {
            return ZoneOffset.of(input.readString());
        }
    }
}

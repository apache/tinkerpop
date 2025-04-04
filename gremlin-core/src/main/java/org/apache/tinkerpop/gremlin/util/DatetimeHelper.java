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
package org.apache.tinkerpop.gremlin.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Date;

import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

/**
 * Utility class for parsing and formatting dates/times.
 */
public final class DatetimeHelper {

    /**
     * Seems like the "noOffsetText" needs to only be set to "Z" once - doing it twice duplicates the "Z" on
     * {@code format()} calls.
     */
    private static final DateTimeFormatter datetimeFormatter = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_TIME)
            .optionalStart()
                .appendOffset("+HHMMss", "Z")
            .optionalEnd()
            .optionalStart()
                .appendOffset("+HH:MM:ss", "")
            .optionalEnd()
            .toFormatter();

    private static final DateTimeFormatter yearMonthFormatter = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendValue(ChronoField.YEAR)
            .appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR).toFormatter().withResolverStyle(ResolverStyle.LENIENT);

    private static final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
            .appendOptional(datetimeFormatter)
            .appendOptional(ISO_LOCAL_DATE)
            .appendOptional(yearMonthFormatter)
            .toFormatter();

    private DatetimeHelper() {}

    /**
     * Formats an {@code Instant} to a form of {@code 2018-03-22T00:35:44Z} at UTC.
     */
    public static String format(final Instant d) {
        return datetimeFormatter.format(d.atZone(UTC));
    }

    /**
     * Parses a {@code String} representing a date and/or time to a {@code Date} object with a default time zone offset
     * of UTC (+00:00). It can parse dates in any of the following formats.
     *
     * <ul>
     *     <li>2018-03-22</li>
     *     <li>2018-03-22T00:35:44</li>
     *     <li>2018-03-22T00:35:44Z</li>
     *     <li>2018-03-22T00:35:44.741</li>
     *     <li>2018-03-22T00:35:44.741Z</li>
     *     <li>2018-03-22T00:35:44.741+1600</li>
     *     <li>2018-03-22T00:35:44.741+16:00</li>
     *     <li>2018-03-22T00:35:44.741+160000</li>
     *     <li>2018-03-22T00:35:44.741+16:00:00</li>
     * </ul>>
     *
     */
    public static OffsetDateTime parse(final String d) {
        final TemporalAccessor t = formatter.parse(d);

        if (!t.isSupported(ChronoField.HOUR_OF_DAY)) {
            // no hours field so it must be a Date or a YearMonth
            if (!t.isSupported(ChronoField.DAY_OF_MONTH)) {
                // must be a YearMonth coz no day
                return OffsetDateTime.of(LocalDate.of(Year.from(t).getValue(), Month.from(t), 1), LocalTime.MIDNIGHT, UTC);
            } else {
                // must be a Date as the day is present
                return OffsetDateTime.of(LocalDate.from(t), LocalTime.MIDNIGHT, UTC);
            }
        } else if (!t.isSupported(ChronoField.MONTH_OF_YEAR)) {
            // no month field so must be a Time
            return OffsetDateTime.of(LocalDate.ofEpochDay(0), LocalTime.from(t), UTC);
        } else if (t.isSupported(ChronoField.OFFSET_SECONDS)) {
            // has all datetime components including an offset
            return OffsetDateTime.of(LocalDateTime.from(t), ZoneOffset.from(t));
        } else {
            // has all datetime components but no offset so throw in some UTC
            return OffsetDateTime.of(LocalDateTime.from(t), UTC);
        }
    }

    /**
     * A proxy call to {@link #parse(String)} but allows for syntax similar to Gremlin grammar of {@code datetime()}.
     */
    public static OffsetDateTime datetime(final String d) {
        return parse(d);
    }

    /**
     * A proxy allows for syntax similar to Gremlin grammar of {@code datetime()}.
     */
    public static OffsetDateTime datetime() {
        return OffsetDateTime.now();
    }
}

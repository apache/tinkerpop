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

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;

import static java.time.ZoneOffset.UTC;
import static org.junit.Assert.assertEquals;

@RunWith(Enclosed.class)
public class DatetimeHelperTest {

    @RunWith(Parameterized.class)
    public static class DatetimeHelperParseTest {

        @Parameterized.Parameter(value = 0)
        public String d;

        @Parameterized.Parameter(value = 1)
        public OffsetDateTime expected;

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {"2018-03-22T00:35:44.741Z", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), UTC)},
                    {"2018-03-22T00:35:44.741-0000", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), UTC)},
                    {"2018-03-22T00:35:44.741+0000", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), UTC)},
                    {"2018-03-22T00:35:44.741+00:00", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), UTC)},
                    {"2018-03-22T00:35:44.741+000000", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), UTC)},
                    {"2018-03-22T00:35:44.741+00:00:00", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), UTC)},
                    {"2018-03-22T00:35:44.741-0300", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), ZoneOffset.ofHours(-3))},
                    {"2018-03-22T00:35:44.741+1600", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), ZoneOffset.ofHours(16))},
                    {"2018-03-22T00:35:44.741+16:00", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), ZoneOffset.ofHours(16))},
                    {"2018-03-22T00:35:44.741+160000", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), ZoneOffset.ofHours(16))},
                    {"2018-03-22T00:35:44.741+16:00:00", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), ZoneOffset.ofHours(16))},
                    {"2018-03-22T00:35:44.741+1659", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), ZoneOffset.ofHoursMinutes(16, 59))},
                    {"2018-03-22T00:35:44.741+16:59", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), ZoneOffset.ofHoursMinutes(16, 59))},
                    {"2018-03-22T00:35:44.741+165900", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), ZoneOffset.ofHoursMinutes(16, 59))},
                    {"2018-03-22T00:35:44.741+16:59:00", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), ZoneOffset.ofHoursMinutes(16, 59))},
                    {"2018-03-22T00:35:44.741+165930", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), ZoneOffset.ofHoursMinutesSeconds(16, 59, 30))},
                    {"2018-03-22T00:35:44.741+16:59:30", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), ZoneOffset.ofHoursMinutesSeconds(16, 59, 30))},
                    {"2018-03-22T00:35:44.741", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), UTC)},
                    {"2018-03-22T00:35:44Z", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 0), UTC)},
                    {"2018-03-22T00:35:44", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 0), UTC)},
                    {"2018-03-22", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 0, 0, 0, 0), UTC)},
                    {"1018-03-22", OffsetDateTime.of(LocalDateTime.of(1018, 03, 22, 0, 0, 0, 0), UTC)},
                    {"9018-03-22", OffsetDateTime.of(LocalDateTime.of(9018, 03, 22, 0, 0, 0, 0), UTC)},
                    {"1000-001", OffsetDateTime.of(LocalDateTime.of(1000, 1, 1, 0, 0, 0, 0), UTC)},
            });
        }

        @Test
        public void shouldParse() {
            assertEquals(expected, DatetimeHelper.parse(d));
        }
    }

    @RunWith(Parameterized.class)
    public static class DatetimeHelperFormatTest {

        @Parameterized.Parameter(value = 0)
        public String expected;

        @Parameterized.Parameter(value = 1)
        public Instant d;

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {"2018-03-22T00:35:44.741Z", ZonedDateTime.of(2018, 03, 22, 00, 35, 44, 741000000, UTC).toInstant()},
                    {"2018-03-22T00:35:44.741Z", ZonedDateTime.of(2018, 03, 22, 00, 35, 44, 741000000, UTC).toInstant()},
                    {"2018-03-22T00:35:44.741Z", ZonedDateTime.of(2018, 03, 22, 00, 35, 44, 741000000, UTC).toInstant()},
                    {"2018-03-22T03:35:44.741Z", ZonedDateTime.of(2018, 03, 22, 00, 35, 44, 741000000, ZoneOffset.ofHours(-3)).toInstant()},
                    {"2018-03-21T08:35:44.741Z", ZonedDateTime.of(2018, 03, 22, 00, 35, 44, 741000000, ZoneOffset.ofHours(16)).toInstant()},
                    {"2018-03-22T00:35:44Z", ZonedDateTime.of(2018, 03, 22, 00, 35, 44, 0, UTC).toInstant()},
                    {"2018-03-22T00:00:00Z", ZonedDateTime.of(2018, 03, 22, 0, 0, 0, 0, UTC).toInstant()},
                    {"1018-03-22T00:00:00Z", ZonedDateTime.of(1018, 03, 22, 0, 0, 0, 0, UTC).toInstant()},
                    {"9018-03-22T00:00:00Z", ZonedDateTime.of(9018, 03, 22, 0, 0, 0, 0, UTC).toInstant()},
                    {"1970-01-01T00:00:00Z", ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, UTC).toInstant()},
                    {"1000-01-01T00:00:00Z", ZonedDateTime.of(1000, 1, 1, 0, 0, 0, 0, UTC).toInstant()},
            });
        }

        @Test
        public void shouldFormat() {
            assertEquals(expected, DatetimeHelper.format(d));
        }
    }
}

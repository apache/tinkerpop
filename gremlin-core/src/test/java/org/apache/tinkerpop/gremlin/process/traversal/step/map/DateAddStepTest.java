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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.DT;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import static java.time.ZoneOffset.UTC;
import static org.junit.Assert.assertEquals;

public class DateAddStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Collections.singletonList(__.dateAdd(DT.hour, 5));
    }

    @Test
    public void shouldAddHours() {
        final OffsetDateTime now = OffsetDateTime.now(UTC);
        final OffsetDateTime expected = now.plus(Duration.ofHours(2));

        assertEquals(expected, __.__(now).dateAdd(DT.hour, 2).next());
    }

    @Test
    public void shouldAddNegativeHours() {
        final OffsetDateTime now = OffsetDateTime.now(UTC);
        final OffsetDateTime expected = now.plus(Duration.ofHours(-3));

        assertEquals(expected, __.__(now).dateAdd(DT.hour, -3).next());
    }

    @Test
    public void shouldAddMinutes() {
        final OffsetDateTime now = OffsetDateTime.now(UTC);
        final OffsetDateTime expected = now.plus(Duration.ofMinutes(5));

        assertEquals(expected, __.__(now).dateAdd(DT.minute, 5).next());
    }

    @Test
    public void shouldAddSeconds() {
        final OffsetDateTime now = OffsetDateTime.now(UTC);
        final OffsetDateTime expected = now.plus(Duration.ofSeconds(15));

        assertEquals(expected, __.__(now).dateAdd(DT.second, 15).next());
    }

    @Test
    public void shouldAddDays() {
        final OffsetDateTime now = OffsetDateTime.now(UTC);
        final OffsetDateTime expected = now.plus(Duration.ofDays(50));

        assertEquals(expected, __.__(now).dateAdd(DT.day, 50).next());
    }

    @Test
    public void shouldAddHoursForDateCompatibility() {
        final Date now = new Date();

        final Calendar cal = Calendar.getInstance();
        cal.setTime(now);
        cal.add(Calendar.HOUR_OF_DAY, 2);
        final OffsetDateTime expected = OffsetDateTime.ofInstant(cal.getTime().toInstant(), ZoneOffset.UTC);

        assertEquals(expected, __.__(now).dateAdd(DT.hour, 2).next());
    }

    @Test
    public void shouldAddNegativeHoursForDateCompatibility() {
        final Date now = new Date();

        final Calendar cal = Calendar.getInstance();
        cal.setTime(now);
        cal.add(Calendar.HOUR_OF_DAY, -3);
        final OffsetDateTime expected = OffsetDateTime.ofInstant(cal.getTime().toInstant(), ZoneOffset.UTC);

        assertEquals(expected, __.__(now).dateAdd(DT.hour, -3).next());
    }

    @Test
    public void shouldAddMinutesForDateCompatibility() {
        final Date now = new Date();

        final Calendar cal = Calendar.getInstance();
        cal.setTime(now);
        cal.add(Calendar.MINUTE, 5);
        final OffsetDateTime expected = OffsetDateTime.ofInstant(cal.getTime().toInstant(), ZoneOffset.UTC);

        assertEquals(expected, __.__(now).dateAdd(DT.minute, 5).next());
    }

    @Test
    public void shouldAddSecondsForDateCompatibility() {
        final Date now = new Date();

        final Calendar cal = Calendar.getInstance();
        cal.setTime(now);
        cal.add(Calendar.SECOND, 15);
        final OffsetDateTime expected = OffsetDateTime.ofInstant(cal.getTime().toInstant(), ZoneOffset.UTC);

        assertEquals(expected, __.__(now).dateAdd(DT.second, 15).next());
    }

    @Test @Ignore("need to restruture to account for day light saving changes")
    public void shouldAddDaysForDateCompatibility() {
        final Date now = new Date();

        final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.setTime(now);
        cal.add(Calendar.DAY_OF_MONTH, 50);
        final OffsetDateTime expected = OffsetDateTime.ofInstant(cal.getTime().toInstant(), ZoneOffset.UTC);

        assertEquals(expected, __.__(now).dateAdd(DT.day, 50).next());
    }
}

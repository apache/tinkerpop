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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.junit.Test;

import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static java.time.ZoneOffset.UTC;
import static org.junit.Assert.assertEquals;

public class AsDateStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Collections.singletonList(__.asDate());
    }

    @Test
    public void shouldParseDate() {

        final OffsetDateTime testDate = OffsetDateTime.of(LocalDateTime.of(2023, 8, 2, 0, 0, 0, 0), UTC);

        assertEquals(OffsetDateTime.ofInstant(Instant.ofEpochMilli(1), ZoneOffset.UTC), __.__(1).asDate().next());
        assertEquals(OffsetDateTime.ofInstant(Instant.ofEpochMilli(3), ZoneOffset.UTC), __.__(3L).asDate().next());
        assertEquals(OffsetDateTime.ofInstant(Instant.ofEpochMilli(6), ZoneOffset.UTC), __.__(new BigInteger("6")).asDate().next());
        assertEquals(testDate, __.__(testDate.toInstant().toEpochMilli()).asDate().next());

        assertEquals(testDate, __.__("2023-08-02T00:00:00Z").asDate().next());
        assertEquals(testDate, __.__(testDate).asDate().next());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenInvalidStringInput() {
        __.__("This String is not an ISO 8601 Date").asDate().next();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenArrayInput() {
        __.__(Arrays.asList(1, 2)).asDate().next();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenUUIDInput() {
        __.__(UUID.randomUUID()).asDate().next();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenDecimalInput() {
        __.__(2.2d).asDate().next();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenBigIntegerOutOfLongInput() {
        __.__(new BigInteger("1000000000000000000000")).asDate().next();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenNullInput() {
        __.__(null).asDate().next();
    }

}

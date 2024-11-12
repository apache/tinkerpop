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

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;

import static java.time.ZoneOffset.UTC;
import static org.junit.Assert.assertEquals;

public class DateDiffStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Collections.singletonList(__.dateDiff(OffsetDateTime.parse("2023-08-02T00:00:20Z")));
    }

    @Test
    public void shouldHandlePositiveValues() {
        final OffsetDateTime now = OffsetDateTime.now(UTC);
        final OffsetDateTime other = now.plus(Duration.ofDays(7));

        assertEquals(604800L, (long) __.__(other).dateDiff(now).next());
    }

    @Test
    public void shouldHandleNegativeValues() {
        final OffsetDateTime now = OffsetDateTime.now(UTC);
        final OffsetDateTime other = now.plus(Duration.ofDays(7));

        assertEquals(-604800L, (long) __.__(now).dateDiff(other).next());
    }

    @Test
    public void shouldHandleTraversalParam() {
        final OffsetDateTime now = OffsetDateTime.now(UTC);
        final OffsetDateTime other = now.plus(Duration.ofDays(7));

        assertEquals(-604800L, (long) __.__(now).dateDiff(__.constant(other)).next());
    }

    @Test
    public void shouldHandleNullTraversalParam() {
        final OffsetDateTime now = OffsetDateTime.now(UTC);

        assertEquals(now.toEpochSecond(), (long) __.__(now).dateDiff(__.constant(null)).next());
    }

    @Test
    public void shouldHandleNullDate() {
        final OffsetDateTime now = OffsetDateTime.now(UTC);

        assertEquals(now.toEpochSecond(), (long) __.__(now).dateDiff((OffsetDateTime) null).next());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowWhenInputIsNotDate() {
        __.__("2023-08-23T00:00:00Z").dateDiff(OffsetDateTime.now(UTC)).next();
    }
}

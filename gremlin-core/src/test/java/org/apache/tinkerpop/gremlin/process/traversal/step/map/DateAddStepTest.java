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
import org.junit.Test;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;

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
}

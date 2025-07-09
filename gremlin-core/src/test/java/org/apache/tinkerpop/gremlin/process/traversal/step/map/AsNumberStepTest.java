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

import org.apache.tinkerpop.gremlin.process.traversal.N;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class AsNumberStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Collections.singletonList(__.asNumber());
    }

    @Test
    public void testReturnTypes() {
        assertEquals(1, __.__(1).asNumber().next());
        assertEquals((byte) 1, __.__(1).asNumber(N.nbyte).next());
        assertEquals(1, __.__(1.8).asNumber(N.nint).next());
        assertEquals(1, __.__(1L).asNumber(N.nint).next());
        assertEquals(1L, __.__(1L).asNumber().next());
        assertEquals(1, __.__("1").asNumber(N.nint).next());
        assertEquals(1, __.__("1").asNumber().next());
        assertEquals((byte) 1, __.__("1").asNumber(N.nbyte).next());
    }

    @Test(expected = NumberFormatException.class)
    public void shouldThrowExceptionWhenInvalidStringInput() {
        __.__("This String is not a number").asNumber().next();
    }

    @Test(expected = NumberFormatException.class)
    public void shouldThrowParseExceptionWithInvalidNumberStringInput() {
        __.__("128abc").asNumber().next();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenArrayInput() {
        __.__(Arrays.asList(1, 2)).asNumber().next();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenUUIDInput() {
        __.__(UUID.randomUUID()).asNumber().next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenParsedNumberOverflows() {
        __.__("128").asNumber(N.nbyte).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenCastNumberOverflows() {
        __.__(128).asNumber(N.nbyte).next();
    }

}

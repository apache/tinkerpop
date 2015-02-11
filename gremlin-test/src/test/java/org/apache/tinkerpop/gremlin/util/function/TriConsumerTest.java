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
package org.apache.tinkerpop.gremlin.util.function;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TriConsumerTest {
    @Test
    public void shouldApplyCurrentFunctionAndThenAnotherSuppliedOne() {
        final List<String> l = new ArrayList<>();
        final TriConsumer<String, String, String> f = (a, b, c) -> l.add("first");
        final TriConsumer<String, String, String> after = (a, b, c) -> l.add("second");

        f.andThen(after).accept("a", "b", "c");

        assertEquals(2, l.size());
        assertEquals("first", l.get(0));
        assertEquals("second", l.get(1));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfAfterFunctionIsNull() {
        final List<String> l = new ArrayList<>();
        final TriConsumer<String, String, String> f = (a, b, c) -> l.add("second");
        f.andThen(null);
    }

    @Test
    public void shouldNotApplySecondIfFirstFails() {
        final List<String> l = new ArrayList<>();
        final TriConsumer<String, String, String> f = (a, b, c) -> {
            throw new RuntimeException();
        };
        final TriConsumer<String, String, String> after = (a, b, c) -> l.add("second");

        try {
            f.andThen(after).accept("a", "b", "c");
            fail("Should have throw an exception");
        } catch (RuntimeException re) {
            assertEquals(0, l.size());
        }
    }
}

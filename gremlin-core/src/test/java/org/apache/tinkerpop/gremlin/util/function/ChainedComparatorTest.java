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

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ChainedComparatorTest {

    final ChainedComparator<Integer> chained = new ChainedComparator<>(Arrays.asList(Comparator.<Integer>naturalOrder(), Comparator.<Integer>reverseOrder()));

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfNoComparators() {
        new ChainedComparator<>(Collections.emptyList());
    }

    @Test
    public void shouldCompareBigger() {
        assertEquals(1, chained.compare(2, 1));
    }

    @Test
    public void shouldCompareSmaller() {
        assertEquals(-1, chained.compare(1, 2));
    }

    @Test
    public void shouldCompareSame() {
        assertEquals(0, chained.compare(2, 2));
    }
}

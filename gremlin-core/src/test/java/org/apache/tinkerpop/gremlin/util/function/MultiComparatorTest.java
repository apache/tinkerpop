/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.util.function;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MultiComparatorTest {

    private static final Random RANDOM = new Random();

    @Test
    public void shouldHandleShuffleCorrectly() {
        MultiComparator<Object> comparator = new MultiComparator<>(Arrays.asList(Order.asc, Order.desc, Order.shuffle));
        assertTrue(comparator.isShuffle()); // because its a shuffle, the comparator simply returns 0
        for (int i = 0; i < 100; i++) {
            assertEquals(0, comparator.compare(RANDOM.nextInt(), RANDOM.nextInt()));
        }
        //
        comparator = new MultiComparator<>(Arrays.asList(Order.asc, Order.shuffle, Order.desc));
        assertEquals(1, comparator.compare(1, 2));
        assertEquals(-1, comparator.compare(2, 1));
        assertEquals(0, comparator.compare(2, 2));
        assertEquals(2, comparator.startIndex);
        assertFalse(comparator.isShuffle());
        //
        comparator = new MultiComparator<>(Arrays.asList(Order.asc, Order.shuffle, Order.desc, Order.shuffle, Order.asc));
        assertEquals(-1, comparator.compare(1, 2));
        assertEquals(1, comparator.compare(2, 1));
        assertEquals(0, comparator.compare(2, 2));
        assertEquals(4, comparator.startIndex);
        assertFalse(comparator.isShuffle());
        //
        comparator = new MultiComparator<>(Collections.emptyList());
        assertEquals(-1, comparator.compare(1, 2));
        assertEquals(1, comparator.compare(2, 1));
        assertEquals(0, comparator.compare(2, 2));
        assertEquals(0, comparator.startIndex);
        assertFalse(comparator.isShuffle());
    }
}

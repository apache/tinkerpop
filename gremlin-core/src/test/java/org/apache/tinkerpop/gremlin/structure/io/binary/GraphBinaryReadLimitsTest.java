/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.io.binary;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class GraphBinaryReadLimitsTest {

    @Test
    public void shouldDefaultToDocumentedValues() {
        assertEquals(64 * 1024 * 1024, GraphBinaryReadLimits.DEFAULT.getMaxValueLength());
        assertEquals(1_048_576, GraphBinaryReadLimits.DEFAULT.getMaxArrayLength());
        assertEquals(16_384, GraphBinaryReadLimits.DEFAULT.getMaxInitialCapacity());
    }

    @Test
    public void shouldCreateFromValues() {
        final GraphBinaryReadLimits limits = GraphBinaryReadLimits.of(1, 2, 3);
        assertEquals(1, limits.getMaxValueLength());
        assertEquals(2, limits.getMaxArrayLength());
        assertEquals(3, limits.getMaxInitialCapacity());
    }

    @Test
    public void shouldAcceptIntegerMaxValue() {
        final GraphBinaryReadLimits limits = GraphBinaryReadLimits.of(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
        assertEquals(Integer.MAX_VALUE, limits.getMaxValueLength());
    }

    @Test
    public void shouldRejectNonPositiveValueLength() {
        assertRejected(0, 1, 1, "maxGraphBinaryValueLength must be greater than zero");
        assertRejected(-1, 1, 1, "maxGraphBinaryValueLength must be greater than zero");
    }

    @Test
    public void shouldRejectNonPositiveArrayLength() {
        assertRejected(1, 0, 1, "maxGraphBinaryArrayLength must be greater than zero");
        assertRejected(1, -1, 1, "maxGraphBinaryArrayLength must be greater than zero");
    }

    @Test
    public void shouldRejectNonPositiveInitialCapacity() {
        assertRejected(1, 1, 0, "maxGraphBinaryInitialCapacity must be greater than zero");
        assertRejected(1, 1, -1, "maxGraphBinaryInitialCapacity must be greater than zero");
    }

    @Test
    public void shouldImplementEqualsAndHashCode() {
        assertEquals(GraphBinaryReadLimits.of(1, 2, 3), GraphBinaryReadLimits.of(1, 2, 3));
        assertEquals(GraphBinaryReadLimits.of(1, 2, 3).hashCode(), GraphBinaryReadLimits.of(1, 2, 3).hashCode());
        assertNotEquals(GraphBinaryReadLimits.of(1, 2, 3), GraphBinaryReadLimits.of(1, 2, 4));
    }

    @Test
    public void readerShouldUseDefaultLimitsUnlessConfigured() {
        assertSame(GraphBinaryReadLimits.DEFAULT, new GraphBinaryReader().getLimits());
        assertSame(GraphBinaryReadLimits.DEFAULT, new GraphBinaryReader(TypeSerializerRegistry.INSTANCE).getLimits());
        assertSame(GraphBinaryReadLimits.DEFAULT, new GraphBinaryReader(TypeSerializerRegistry.INSTANCE, null).getLimits());
    }

    @Test
    public void readerShouldExposeConfiguredLimits() {
        final GraphBinaryReadLimits limits = GraphBinaryReadLimits.of(1, 2, 3);
        assertSame(limits, new GraphBinaryReader(TypeSerializerRegistry.INSTANCE, null, limits).getLimits());
    }

    @Test
    public void withLimitsShouldReturnReaderWithNewLimitsAndLeaveOriginalUnchanged() {
        final GraphBinaryReader original = new GraphBinaryReader();
        final GraphBinaryReadLimits limits = GraphBinaryReadLimits.of(1, 2, 3);

        final GraphBinaryReader changed = original.withLimits(limits);

        assertSame(limits, changed.getLimits());
        assertSame(GraphBinaryReadLimits.DEFAULT, original.getLimits());
    }

    @Test(expected = NullPointerException.class)
    public void readerShouldRejectNullLimits() {
        new GraphBinaryReader(TypeSerializerRegistry.INSTANCE, null, null);
    }

    private static void assertRejected(final int value, final int array, final int capacity, final String message) {
        try {
            GraphBinaryReadLimits.of(value, array, capacity);
            fail("Should have rejected the limits");
        } catch (IllegalArgumentException ex) {
            assertEquals(message, ex.getMessage());
        }
    }
}

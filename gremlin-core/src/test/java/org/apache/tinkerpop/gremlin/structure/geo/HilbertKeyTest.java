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
package org.apache.tinkerpop.gremlin.structure.geo;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the level-16 Hilbert key used as the third key of the geometry order. The expected
 * indexes are pinned to the values JTS 1.20 {@code HilbertCode.encode(16, x, y)} produces, so that
 * the hand written encoding stays interchangeable with the standard one.
 */
public class HilbertKeyTest {

    private static final int MAX_CELL = 65535;

    @Test
    public void shouldMatchPinnedIndexes() {
        assertIndex(0L, 0, 0);
        assertIndex(1L, 1, 0);
        assertIndex(2L, 1, 1);
        assertIndex(3L, 0, 1);
        assertIndex(4294967295L, 65535, 0);
        assertIndex(1431655765L, 0, 65535);
        assertIndex(2863311530L, 65535, 65535);
        assertIndex(2147483648L, 32768, 32768);
        assertIndex(1555040834L, 12345, 54321);
    }

    @Test
    public void shouldVisitEveryCellOfASmallPatchExactlyOnce() {
        // the curve is a bijection, so no two distinct cells share an index
        final Set<Long> seen = new HashSet<>();
        for (int x = 0; x < 64; x++) {
            for (int y = 0; y < 64; y++) {
                assertTrue("duplicate index at " + x + "," + y,
                        seen.add(Integer.toUnsignedLong(HilbertKey.encode(x, y))));
            }
        }
        assertEquals(64 * 64, seen.size());
    }

    @Test
    public void shouldQuantiseLongitudeOverTheWholeRange() {
        assertEquals(0, HilbertKey.cellForLongitude(-180.0));
        assertEquals(32768, HilbertKey.cellForLongitude(0.0));
        assertEquals(MAX_CELL, HilbertKey.cellForLongitude(180.0));
    }

    @Test
    public void shouldQuantiseLatitudeOverTheWholeRange() {
        assertEquals(0, HilbertKey.cellForLatitude(-90.0));
        assertEquals(32768, HilbertKey.cellForLatitude(0.0));
        assertEquals(MAX_CELL, HilbertKey.cellForLatitude(90.0));
    }

    @Test
    public void shouldQuantiseGeographicCornersAndCentre() {
        assertEquals(0L, keyOf(Point.of(-180, -90, Model.GEOGRAPHIC)));
        assertEquals(2863311530L, keyOf(Point.of(180, 90, Model.GEOGRAPHIC)));
        assertEquals(2147483648L, keyOf(Point.of(0, 0, Model.GEOGRAPHIC)));
    }

    @Test
    public void shouldQuantiseCartesianMonotonically() {
        final double[] ascending = {
                -Double.MAX_VALUE, -1e300, -1e6, -1.5, -1.0, -0.5, -Double.MIN_VALUE,
                0.0, Double.MIN_VALUE, 0.5, 1.0, 1.5, 1e6, 1e300, Double.MAX_VALUE};
        int previous = HilbertKey.cellForValue(ascending[0]);
        for (int i = 1; i < ascending.length; i++) {
            final int cell = HilbertKey.cellForValue(ascending[i]);
            assertTrue("cell for " + ascending[i] + " must not be below the cell for "
                    + ascending[i - 1], cell >= previous);
            previous = cell;
        }
        // the extremes land in different cells, so the mapping spreads the range
        assertNotEquals(HilbertKey.cellForValue(-Double.MAX_VALUE),
                HilbertKey.cellForValue(Double.MAX_VALUE));
    }

    @Test
    public void shouldGiveTheSameKeyForNegativeAndPositiveZero() {
        assertEquals(keyOf(Point.of(0.0, 0.0)), keyOf(Point.of(-0.0, -0.0)));
        assertEquals(keyOf(Point.of(0.0, 0.0)), keyOf(Point.of(-0.0, 0.0)));
        assertEquals(keyOf(Point.of(0.0, 0.0, Model.GEOGRAPHIC)),
                keyOf(Point.of(-0.0, -0.0, Model.GEOGRAPHIC)));
        assertEquals(0.0, HilbertKey.normalizeZero(-0.0), 0.0);
        assertTrue(Double.doubleToRawLongBits(HilbertKey.normalizeZero(-0.0)) == 0L);
        // the cartesian quantiser normalises the sign of zero itself
        assertEquals(HilbertKey.cellForValue(0.0), HilbertKey.cellForValue(-0.0));
    }

    @Test
    public void shouldKeyByTheBoundingBoxCentre() {
        // a line string from (0 0) to (2 2) has the same centre as the point (1 1)
        final long line = keyOf(LineString.of(Point.of(0, 0), Point.of(2, 2)));
        assertEquals(keyOf(Point.of(1, 1)), line);
        // z plays no part in the key
        assertEquals(keyOf(Point.of(1, 1)), keyOf(Point.of(1, 1, 7)));
    }

    @Test
    public void shouldHaveNoKeyForAGeometryWithoutCoordinates() {
        assertEquals(0L, keyOf(Point.empty()));
        assertEquals(0L, keyOf(GeometryCollection.empty()));
        assertEquals(0L, keyOf(Geometry.fromWkt("MULTIPOINT(EMPTY)")));
    }

    private static long keyOf(final Geometry geometry) {
        return Integer.toUnsignedLong(HilbertKey.of(geometry));
    }

    private static void assertIndex(final long expected, final int x, final int y) {
        assertEquals("index of cell " + x + "," + y,
                expected, Integer.toUnsignedLong(HilbertKey.encode(x, y)));
    }
}

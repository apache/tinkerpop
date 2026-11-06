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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests the geometry order over a fixed corpus: the order laws, and the emptiness and locality
 * keys.
 */
public class GeometryOrderTest {

    /**
     * A fixed corpus covering every kind, both models, an SRID that is neither, empty values of
     * each kind, z values, nesting, every tie-break pair, and a -0.0 value beside its 0.0 twin.
     * The twin is the only pair in the corpus that is equal; every other pair of entries differs.
     */
    private static List<Geometry> corpus() {
        final List<Geometry> values = new ArrayList<>();

        // empty values of every kind, in the CARTESIAN model
        values.add(Point.empty());
        values.add(Geometry.fromWkt("POINT Z EMPTY"));
        values.add(LineString.empty());
        values.add(Geometry.fromWkt("LINESTRING Z EMPTY"));
        values.add(Polygon.empty());
        values.add(MultiPoint.empty());
        values.add(MultiLineString.empty());
        values.add(MultiPolygon.empty());
        values.add(GeometryCollection.empty());
        values.add(Geometry.fromWkt("MULTIPOINT(EMPTY)"));
        values.add(Geometry.fromWkt("GEOMETRYCOLLECTION(POINT EMPTY)"));

        // empty values in the GEOGRAPHIC model
        values.add(Point.empty(Model.GEOGRAPHIC));
        values.add(LineString.empty(Model.GEOGRAPHIC));

        // points spread over the plane, with a -0.0 value beside its 0.0 twin
        values.add(Point.of(0, 0));
        values.add(Point.of(-0.0, -0.0));
        values.add(Point.of(1, 1));
        values.add(Point.of(-1, -1));
        values.add(Point.of(1, -1));
        values.add(Point.of(-1, 1));
        values.add(Point.of(0.5, 0.5));
        values.add(Point.of(3, 4));
        values.add(Point.of(100, 100));
        values.add(Point.of(-100, 50));
        values.add(Point.of(1e6, -1e6));
        values.add(Point.of(1, 2));

        // z values
        values.add(Point.of(0, 0, 0));
        values.add(Point.of(1, 2, 3));
        values.add(Point.of(1, 2, 4));
        values.add(Geometry.fromWkt("LINESTRING Z (0 0 0, 1 1 1)"));

        // line strings, including the same points in another order
        values.add(LineString.of(Point.of(0, 0), Point.of(1, 1)));
        values.add(LineString.of(Point.of(1, 1), Point.of(0, 0)));
        values.add(LineString.of(Point.of(0, 0), Point.of(1, 1), Point.of(2, 2), Point.of(3, 3)));

        // multi-points, including the kind pairs of the single point and the line string
        values.add(MultiPoint.of(Point.of(0, 0)));
        values.add(MultiPoint.of(Point.of(0, 0), Point.of(1, 1)));
        values.add(MultiPoint.of(Point.of(1, 2)));
        values.add(Geometry.fromWkt("MULTIPOINT((1 2), EMPTY)"));
        values.add(Geometry.fromWkt("MULTIPOINT(EMPTY, (1 2))"));

        // multi-line strings with the same points but a different number of parts
        values.add(MultiLineString.of(
                LineString.of(Point.of(0, 0), Point.of(1, 1), Point.of(2, 2), Point.of(3, 3))));
        values.add(MultiLineString.of(
                LineString.of(Point.of(0, 0), Point.of(1, 1)),
                LineString.of(Point.of(2, 2), Point.of(3, 3))));

        // polygons, with and without a hole, and a multi-polygon over the same shell
        final LineString shell = LineString.of(Point.of(0, 0), Point.of(10, 0), Point.of(10, 10),
                Point.of(0, 10), Point.of(0, 0));
        final LineString hole = LineString.of(Point.of(2, 2), Point.of(4, 2), Point.of(4, 4),
                Point.of(2, 2));
        values.add(Polygon.of(shell));
        values.add(Polygon.of(shell, hole));
        values.add(MultiPolygon.of(Polygon.of(shell)));

        // collections, including nesting and parts of a different kind over the same coordinates
        values.add(GeometryCollection.of(Point.of(0, 0), Point.of(1, 1)));
        values.add(GeometryCollection.of(LineString.of(Point.of(0, 0), Point.of(1, 1))));
        values.add(GeometryCollection.of(MultiPoint.of(Point.of(0, 0), Point.of(1, 1))));
        values.add(GeometryCollection.of(GeometryCollection.of(Point.of(5, 5))));
        values.add(Geometry.fromWkt("GEOMETRYCOLLECTION(POINT EMPTY, POINT(1 2))"));
        values.add(Geometry.fromWkt("GEOMETRYCOLLECTION(POINT(1 2), POINT EMPTY)"));

        // the globe
        values.add(Point.of(-180, -90, Model.GEOGRAPHIC));
        values.add(Point.of(180, 90, Model.GEOGRAPHIC));
        values.add(Point.of(0, 0, Model.GEOGRAPHIC));
        values.add(Point.of(-74, 40.7, Model.GEOGRAPHIC));
        values.add(Point.of(-74.1, 40.75, Model.GEOGRAPHIC));
        values.add(Point.of(2.35, 48.86, Model.GEOGRAPHIC));
        values.add(Point.of(1, 2, 3, Model.GEOGRAPHIC));
        values.add(LineString.of(Point.of(-74, 40.7, Model.GEOGRAPHIC),
                Point.of(2.35, 48.86, Model.GEOGRAPHIC)));

        // an SRID that is neither CARTESIAN nor GEOGRAPHIC
        values.add(Geometry.fromWkt("POINT(500000 4500000)", 32618));
        values.add(Geometry.fromWkt("LINESTRING(500000 4500000, 500100 4500100)", 32618));
        values.add(Geometry.fromWkt("POINT EMPTY", 32618));

        return values;
    }

    // compare is antisymmetric over every pair
    @Test
    public void shouldBeAntisymmetric() {
        final List<Geometry> values = corpus();
        for (final Geometry a : values) {
            for (final Geometry b : values) {
                assertEquals("compare(" + a + ", " + b + ")",
                        -Integer.signum(b.compareTo(a)), Integer.signum(a.compareTo(b)));
            }
        }
    }

    // compare is transitive over every triple
    @Test
    public void shouldBeTransitive() {
        final List<Geometry> values = corpus();
        for (final Geometry a : values) {
            for (final Geometry b : values) {
                if (a.compareTo(b) > 0) {
                    continue;
                }
                for (final Geometry c : values) {
                    if (b.compareTo(c) > 0) {
                        continue;
                    }
                    assertTrue(a + " <= " + b + " <= " + c + " so " + a + " <= " + c,
                            a.compareTo(c) <= 0);
                }
            }
        }
    }

    // compare returns 0 exactly when equals is true
    @Test
    public void shouldTieExactlyForEqualValues() {
        final List<Geometry> values = corpus();
        for (final Geometry a : values) {
            for (final Geometry b : values) {
                assertEquals("compare(" + a + ", " + b + ") == 0 must match equals",
                        a.equals(b), a.compareTo(b) == 0);
            }
        }
    }

    // Equal values hash alike
    @Test
    public void shouldHashEqualValuesAlike() {
        final List<Geometry> values = corpus();
        for (final Geometry a : values) {
            for (final Geometry b : values) {
                if (a.equals(b)) {
                    assertEquals("hashCode of " + a + " and " + b, a.hashCode(), b.hashCode());
                }
            }
        }
    }

    // The corpus sorts to the same order however it is shuffled
    @Test
    public void shouldSortShuffledCorporaAlike() {
        final List<Geometry> expected = new ArrayList<>(corpus());
        Collections.sort(expected);
        for (final long seed : new long[]{1L, 42L, 1234L, 987654321L, -7L}) {
            final List<Geometry> shuffled = new ArrayList<>(corpus());
            Collections.shuffle(shuffled, new Random(seed));
            Collections.sort(shuffled);
            assertEquals("sorted order for seed " + seed, expected, shuffled);
        }
    }

    // Empty values sort before non-empty values of the same SRID, a collection of empty parts
    // among them
    @Test
    public void shouldSortEmptyValuesFirst() {
        final List<Geometry> values = new ArrayList<>(Arrays.asList(
                Point.of(1, 1),
                Geometry.fromWkt("GEOMETRYCOLLECTION(POINT EMPTY)"),
                LineString.of(Point.of(0, 0), Point.of(1, 1)),
                Point.empty(),
                MultiPolygon.empty()));
        Collections.sort(values);
        for (int i = 0; i < 3; i++) {
            assertTrue("expected an empty value at " + i + " but got " + values.get(i),
                    values.get(i).isEmpty());
        }
        assertFalse(values.get(3).isEmpty());
        assertFalse(values.get(4).isEmpty());
    }

    // Points spread over the plane sort by the Hilbert key of their cells
    @Test
    public void shouldSortPlanePointsByHilbertKey() {
        final List<Geometry> values = new ArrayList<>(Arrays.asList(
                Point.of(100, 100),
                Point.of(0, 0),
                Point.of(1e6, -1e6),
                Point.of(-1, 1),
                Point.of(3, 4),
                Point.of(1, 1),
                Point.of(-100, 50),
                Point.of(1, -1),
                Point.of(0.5, 0.5),
                Point.of(-1, -1)));
        Collections.sort(values);
        assertEquals(Arrays.asList(
                Point.of(-1, -1),
                Point.of(-100, 50),
                Point.of(-1, 1),
                Point.of(0, 0),
                Point.of(0.5, 0.5),
                Point.of(1, 1),
                Point.of(3, 4),
                Point.of(100, 100),
                Point.of(1, -1),
                Point.of(1e6, -1e6)), values);
    }

    // Points spread over the globe sort by the Hilbert key of their cells, and the two points
    // a few kilometres apart stay next to each other
    @Test
    public void shouldSortGlobePointsByHilbertKey() {
        final List<Geometry> values = new ArrayList<>(Arrays.asList(
                Point.of(151.2, -33.87, Model.GEOGRAPHIC),
                Point.of(-74, 40.7, Model.GEOGRAPHIC),
                Point.of(180, 90, Model.GEOGRAPHIC),
                Point.of(2.35, 48.86, Model.GEOGRAPHIC),
                Point.of(-180, -90, Model.GEOGRAPHIC),
                Point.of(139.7, 35.68, Model.GEOGRAPHIC),
                Point.of(-74.1, 40.75, Model.GEOGRAPHIC),
                Point.of(0, 0, Model.GEOGRAPHIC),
                Point.of(-122.4, 37.8, Model.GEOGRAPHIC)));
        Collections.sort(values);
        assertEquals(Arrays.asList(
                Point.of(-180, -90, Model.GEOGRAPHIC),
                Point.of(-122.4, 37.8, Model.GEOGRAPHIC),
                Point.of(-74, 40.7, Model.GEOGRAPHIC),
                Point.of(-74.1, 40.75, Model.GEOGRAPHIC),
                Point.of(0, 0, Model.GEOGRAPHIC),
                Point.of(2.35, 48.86, Model.GEOGRAPHIC),
                Point.of(180, 90, Model.GEOGRAPHIC),
                Point.of(139.7, 35.68, Model.GEOGRAPHIC),
                Point.of(151.2, -33.87, Model.GEOGRAPHIC)), values);
    }
}

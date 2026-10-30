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

import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for geometry construction, accessors, model, immutability, and equality.
 */
public class GeometryTest {

    // Point.of(3, 4) - CARTESIAN, SRID 0, x 3, y 4, z NaN, hasZ() false, not empty
    @Test
    public void testPointOf2D() {
        final Point p = Point.of(3, 4);
        assertEquals(Model.CARTESIAN, p.model());
        assertEquals(0, p.srid());
        assertEquals(3.0, p.x(), 0.0);
        assertEquals(4.0, p.y(), 0.0);
        assertTrue(Double.isNaN(p.z()));
        assertFalse(p.hasZ());
        assertFalse(p.isEmpty());
    }

    // Point.of(-74.0, 40.7, GEOGRAPHIC) - GEOGRAPHIC, SRID 4326
    @Test
    public void testPointOfGeographic() {
        final Point p = Point.of(-74.0, 40.7, Model.GEOGRAPHIC);
        assertEquals(Model.GEOGRAPHIC, p.model());
        assertEquals(4326, p.srid());
    }

    // Point.of(1, 2, 3) with int literals - a 3D CARTESIAN point, z 3, hasZ() true
    @Test
    public void testPointOf3D() {
        final Point p = Point.of(1, 2, 3);
        assertEquals(Model.CARTESIAN, p.model());
        assertEquals(3.0, p.z(), 0.0);
        assertTrue(p.hasZ());
    }

    // Point.of(1, 2, 3, GEOGRAPHIC) - 3D, SRID 4326
    @Test
    public void testPointOf3DGeographic() {
        final Point p = Point.of(1, 2, 3, Model.GEOGRAPHIC);
        assertEquals(Model.GEOGRAPHIC, p.model());
        assertEquals(4326, p.srid());
        assertTrue(p.hasZ());
    }

    // Every factory that takes a Model rejects Model.OTHER
    @Test
    public void testModelOtherRejected() {
        // Both Point.of overloads that take a Model; the other two are CARTESIAN by definition
        assertIae(() -> Point.of(1, 2, Model.OTHER));
        assertIae(() -> Point.of(1, 2, 3, Model.OTHER));

        // Every kind's empty(Model)
        assertIae(() -> Point.empty(Model.OTHER));
        assertIae(() -> LineString.empty(Model.OTHER));
        assertIae(() -> Polygon.empty(Model.OTHER));
        assertIae(() -> MultiPoint.empty(Model.OTHER));
        assertIae(() -> MultiLineString.empty(Model.OTHER));
        assertIae(() -> MultiPolygon.empty(Model.OTHER));
        assertIae(() -> GeometryCollection.empty(Model.OTHER));

        // The parser entry point that takes a Model
        assertIae(() -> Geometry.fromWkt("POINT(1 2)", Model.OTHER));
        assertIae(() -> Geometry.fromWkt("LINESTRING(0 0, 1 1)", Model.OTHER));
        assertIae(() -> Geometry.fromWkt("GEOMETRYCOLLECTION EMPTY", Model.OTHER));

        // An SRID that maps to OTHER is still accepted, since it names the system
        assertEquals(Model.OTHER, Geometry.fromWkt("POINT(1 2)", 32618).model());
    }

    // NaN or infinite x or y - IAE
    @Test
    public void testNaNInfiniteXY() {
        try {
            Point.of(Double.NaN, 1);
            fail("Expected IAE for NaN x");
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            Point.of(1, Double.NaN);
            fail("Expected IAE for NaN y");
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            Point.of(Double.POSITIVE_INFINITY, 1);
            fail("Expected IAE for infinite x");
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            Point.of(1, Double.NEGATIVE_INFINITY);
            fail("Expected IAE for infinite y");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    // Point.of(1, 2, Double.NaN) - IAE
    @Test(expected = IllegalArgumentException.class)
    public void testNaNZRejected() {
        Point.of(1, 2, Double.NaN);
    }

    // Point.empty(), Point.empty(GEOGRAPHIC) - empty, SRID 0 and 4326
    @Test
    public void testEmptyPoints() {
        final Point p1 = Point.empty();
        assertTrue(p1.isEmpty());
        assertEquals(0, p1.srid());
        assertEquals(Model.CARTESIAN, p1.model());

        final Point p2 = Point.empty(Model.GEOGRAPHIC);
        assertTrue(p2.isEmpty());
        assertEquals(4326, p2.srid());
        assertEquals(Model.GEOGRAPHIC, p2.model());
    }

    // x(), y() and z() on Point.empty() and Point.empty(GEOGRAPHIC) - IllegalStateException
    @Test
    public void testEmptyPointAccessorsThrow() {
        final Point empty1 = Point.empty();
        try {
            empty1.x();
            fail("Expected ISE for x() on empty point");
        } catch (IllegalStateException e) {
            // expected
        }
        try {
            empty1.y();
            fail("Expected ISE for y() on empty point");
        } catch (IllegalStateException e) {
            // expected
        }
        try {
            empty1.z();
            fail("Expected ISE for z() on empty point");
        } catch (IllegalStateException e) {
            // expected
        }

        final Point empty2 = Point.empty(Model.GEOGRAPHIC);
        try {
            empty2.x();
            fail("Expected ISE for x() on empty GEOGRAPHIC point");
        } catch (IllegalStateException e) {
            // expected
        }
    }

    // GEOGRAPHIC bounds tests
    @Test
    public void testGeographicBounds() {
        // Bounds accepted
        Point.of(-180.0, -90.0, Model.GEOGRAPHIC);
        Point.of(180.0, 90.0, Model.GEOGRAPHIC);
        Point.of(0.0, 0.0, Model.GEOGRAPHIC);

        // Out of range longitude
        try {
            Point.of(180.000001, 0, Model.GEOGRAPHIC);
            fail("Expected IAE for longitude > 180");
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            Point.of(-180.000001, 0, Model.GEOGRAPHIC);
            fail("Expected IAE for longitude < -180");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // Out of range latitude
        try {
            Point.of(0, 90.000001, Model.GEOGRAPHIC);
            fail("Expected IAE for latitude > 90");
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            Point.of(0, -90.000001, Model.GEOGRAPHIC);
            fail("Expected IAE for latitude < -90");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // CARTESIAN and OTHER accept values outside geographic range
        Point.of(190, 95); // CARTESIAN
        Geometry.fromWkt("POINT(190 95)", 32618); // OTHER
    }

    // GEOGRAPHIC bounds are enforced on the WKT path as well
    @Test
    public void testGeographicBoundsThroughWkt() {
        // Explicit model argument
        assertIae(() -> Geometry.fromWkt("POINT(180.000001 0)", Model.GEOGRAPHIC));
        assertIae(() -> Geometry.fromWkt("POINT(-180.000001 0)", Model.GEOGRAPHIC));
        assertIae(() -> Geometry.fromWkt("POINT(0 90.000001)", Model.GEOGRAPHIC));
        assertIae(() -> Geometry.fromWkt("POINT(0 -90.000001)", Model.GEOGRAPHIC));

        // EWKT prefix
        assertIae(() -> Geometry.fromWkt("SRID=4326;POINT(180.000001 0)"));
        assertIae(() -> Geometry.fromWkt("SRID=4326;POINT(0 90.000001)"));

        // Bounds themselves are accepted
        assertEquals(4326, Geometry.fromWkt("POINT(-180 -90)", Model.GEOGRAPHIC).srid());
        assertEquals(4326, Geometry.fromWkt("SRID=4326;POINT(180 90)").srid());

        // Nested parts are checked too
        assertIae(() -> Geometry.fromWkt("SRID=4326;LINESTRING(0 0, 181 0)"));

        // CARTESIAN and an OTHER SRID accept the same coordinates
        assertEquals(0, Geometry.fromWkt("POINT(190 95)", Model.CARTESIAN).srid());
        assertEquals(32618, Geometry.fromWkt("POINT(190 95)", 32618).srid());
    }

    // GEOGRAPHIC bounds are enforced on the WKB path as well
    @Test
    public void testGeographicBoundsThroughWkb() {
        final byte[] outOfRangeLon = Point.of(180.000001, 0).toWKB();
        assertIae(() -> Geometry.fromWkb(outOfRangeLon, 4326));

        final byte[] outOfRangeLat = Point.of(0, 90.000001).toWKB();
        assertIae(() -> Geometry.fromWkb(outOfRangeLat, 4326));

        // Bounds accepted
        final byte[] atBounds = Point.of(180, 90).toWKB();
        assertEquals(4326, Geometry.fromWkb(atBounds, 4326).srid());

        // CARTESIAN and an OTHER SRID accept the same bytes
        final byte[] outside = Point.of(190, 95).toWKB();
        assertEquals(0, Geometry.fromWkb(outside, 0).srid());
        assertEquals(32618, Geometry.fromWkb(outside, 32618).srid());
    }

    // model() for SRID 0, 4326 and 32618
    @Test
    public void testModelForSrid() {
        final Geometry g1 = Geometry.fromWkt("POINT(1 2)");
        assertEquals(Model.CARTESIAN, g1.model());
        assertEquals(0, g1.srid());

        final Geometry g2 = Geometry.fromWkt("SRID=4326;POINT(1 2)");
        assertEquals(Model.GEOGRAPHIC, g2.model());
        assertEquals(4326, g2.srid());

        final Geometry g3 = Geometry.fromWkt("POINT(1 2)", 32618);
        assertEquals(Model.OTHER, g3.model());
        assertEquals(32618, g3.srid());
    }

    // LineString.of with fewer than 2 points - IAE; 0 points only through empty() or WKT
    @Test
    public void testLineStringMinPoints() {
        // Zero points is not allowed through of()
        assertIae(() -> LineString.of());
        try {
            LineString.of();
            fail("Expected IAE for LineString with 0 points");
        } catch (IllegalArgumentException e) {
            assertTrue("Message should point at empty(), was: " + e.getMessage(),
                    e.getMessage().contains("LineString.empty()"));
        }

        // One point is not allowed either
        assertIae(() -> LineString.of(Point.of(0, 0)));

        // Zero points is reached through empty(), empty(Model) and LINESTRING EMPTY
        final LineString empty = LineString.empty();
        assertTrue(empty.isEmpty());
        assertTrue(empty.points().isEmpty());

        final LineString emptyGeographic = LineString.empty(Model.GEOGRAPHIC);
        assertTrue(emptyGeographic.isEmpty());
        assertEquals(4326, emptyGeographic.srid());

        final Geometry emptyFromWkt = Geometry.fromWkt("LINESTRING EMPTY");
        assertTrue(emptyFromWkt.isEmpty());
        assertEquals(empty, emptyFromWkt);

        // Two points is ok
        final LineString ls = LineString.of(Point.of(0, 0), Point.of(1, 1));
        assertFalse(ls.isEmpty());
        assertEquals(2, ls.points().size());
    }

    // The collection factories reject zero parts; empty() and empty(Model) build empty values
    @Test
    public void testCollectionMinParts() {
        // Zero parts is not allowed through of(), and the message points at empty()
        assertIaeMentioning("MultiPoint.empty()", () -> MultiPoint.of());
        assertIaeMentioning("MultiLineString.empty()", () -> MultiLineString.of());
        assertIaeMentioning("MultiPolygon.empty()", () -> MultiPolygon.of());
        assertIaeMentioning("GeometryCollection.empty()", () -> GeometryCollection.of());

        // empty() gives an empty CARTESIAN collection
        assertTrue(MultiPoint.empty().isEmpty());
        assertTrue(MultiLineString.empty().isEmpty());
        assertTrue(MultiPolygon.empty().isEmpty());
        assertTrue(GeometryCollection.empty().isEmpty());
        assertEquals(0, MultiPoint.empty().srid());

        // empty(Model) is how an empty GEOGRAPHIC collection is built
        final MultiPoint mp = MultiPoint.empty(Model.GEOGRAPHIC);
        assertTrue(mp.isEmpty());
        assertTrue(mp.geometries().isEmpty());
        assertEquals(Model.GEOGRAPHIC, mp.model());
        assertEquals(4326, mp.srid());

        final MultiLineString mls = MultiLineString.empty(Model.GEOGRAPHIC);
        assertTrue(mls.isEmpty());
        assertTrue(mls.geometries().isEmpty());
        assertEquals(Model.GEOGRAPHIC, mls.model());

        final MultiPolygon mpoly = MultiPolygon.empty(Model.GEOGRAPHIC);
        assertTrue(mpoly.isEmpty());
        assertTrue(mpoly.geometries().isEmpty());
        assertEquals(Model.GEOGRAPHIC, mpoly.model());

        final GeometryCollection gc = GeometryCollection.empty(Model.GEOGRAPHIC);
        assertTrue(gc.isEmpty());
        assertTrue(gc.geometries().isEmpty());
        assertEquals(Model.GEOGRAPHIC, gc.model());

        // The WKT/WKB parse path still accepts zero parts
        assertEquals(MultiPoint.empty(), Geometry.fromWkt("MULTIPOINT EMPTY"));
        assertEquals(MultiLineString.empty(), Geometry.fromWkt("MULTILINESTRING EMPTY"));
        assertEquals(MultiPolygon.empty(), Geometry.fromWkt("MULTIPOLYGON EMPTY"));
        assertEquals(GeometryCollection.empty(), Geometry.fromWkt("GEOMETRYCOLLECTION EMPTY"));
    }

    // Polygon ring not closed, or with fewer than 4 points - IAE
    @Test
    public void testPolygonRingValidation() {
        // Not closed
        try {
            final LineString notClosed = LineString.of(
                    Point.of(0, 0), Point.of(1, 0), Point.of(1, 1), Point.of(0, 1)
            );
            Polygon.of(notClosed);
            fail("Expected IAE for unclosed ring");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // Fewer than 4 points (closed)
        try {
            final LineString tooShort = LineString.of(
                    Point.of(0, 0), Point.of(1, 0), Point.of(0, 0)
            );
            Polygon.of(tooShort);
            fail("Expected IAE for ring with fewer than 4 points");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    // Polygon.of(shell) - interiorRings() empty list, exteriorRing() is shell
    @Test
    public void testPolygonNoHoles() {
        final LineString shell = LineString.of(
                Point.of(0, 0), Point.of(1, 0), Point.of(1, 1), Point.of(0, 0)
        );
        final Polygon p = Polygon.of(shell);
        assertEquals(shell, p.exteriorRing());
        assertTrue(p.interiorRings().isEmpty());
    }

    // Parts with different models in a Polygon, Multi* or GeometryCollection - IAE
    @Test
    public void testMixedModels() {
        final Point cartesian = Point.of(0, 0);
        final Point geographic = Point.of(0, 0, Model.GEOGRAPHIC);

        try {
            MultiPoint.of(cartesian, geographic);
            fail("Expected IAE for mixed models in MultiPoint");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            GeometryCollection.of(cartesian, geographic);
            fail("Expected IAE for mixed models in GeometryCollection");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // Polygon rings of different models
        final LineString cartesianRing = LineString.of(
                Point.of(0, 0), Point.of(1, 0), Point.of(1, 1), Point.of(0, 0)
        );
        final LineString geographicRing = LineString.of(
                Point.of(0, 0, Model.GEOGRAPHIC), Point.of(1, 0, Model.GEOGRAPHIC),
                Point.of(1, 1, Model.GEOGRAPHIC), Point.of(0, 0, Model.GEOGRAPHIC)
        );
        assertIae(() -> Polygon.of(cartesianRing, geographicRing));
        assertIae(() -> Polygon.of(geographicRing, cartesianRing));

        // Multi* of parts with different models
        assertIae(() -> MultiLineString.of(cartesianRing, geographicRing));
        assertIae(() -> MultiPolygon.of(Polygon.of(cartesianRing), Polygon.of(geographicRing)));
    }

    // Parts with and without z in one geometry - IAE
    @Test
    public void testMixedZ() {
        final Point p2d = Point.of(0, 0);
        final Point p3d = Point.of(0, 0, 0);

        // LineString of 2D and 3D point
        try {
            LineString.of(p2d, p3d);
            fail("Expected IAE for mixed Z in LineString");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // Collection of 2D point and 3D point
        try {
            GeometryCollection.of(p2d, p3d);
            fail("Expected IAE for mixed Z in GeometryCollection");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // 2D shell with a 3D hole, and the other way round
        final LineString ring2d = LineString.of(
                Point.of(0, 0), Point.of(10, 0), Point.of(10, 10), Point.of(0, 0)
        );
        final LineString ring3d = LineString.of(
                Point.of(1, 1, 0), Point.of(2, 1, 0), Point.of(2, 2, 0), Point.of(1, 1, 0)
        );
        assertIae(() -> Polygon.of(ring2d, ring3d));
        assertIae(() -> Polygon.of(ring3d, ring2d));

        // A collection of a 2D point and a 3D line
        assertIae(() -> GeometryCollection.of(p2d, LineString.of(Point.of(0, 0, 0), Point.of(1, 1, 1))));

        // Multi* of parts that differ in z
        assertIae(() -> MultiLineString.of(ring2d, ring3d));
        assertIae(() -> MultiPolygon.of(Polygon.of(ring2d), Polygon.of(ring3d)));
    }

    // Self-intersecting "bow-tie" polygon - constructs (invalid allowed, malformed rejected)
    @Test
    public void testSelfIntersectingPolygon() {
        // This is a "bow-tie" polygon that self-intersects
        final LineString bowTie = LineString.of(
                Point.of(0, 0), Point.of(2, 2), Point.of(2, 0), Point.of(0, 2), Point.of(0, 0)
        );
        final Polygon p = Polygon.of(bowTie);
        // Should construct without error - invalid but not malformed
        assertFalse(p.isEmpty());
    }

    // Nested GeometryCollection - constructs; geometries() returns parts in order
    @Test
    public void testNestedCollection() {
        final Point p1 = Point.of(0, 0);
        final Point p2 = Point.of(1, 1);
        final GeometryCollection inner = GeometryCollection.of(p2);
        final GeometryCollection outer = GeometryCollection.of(p1, inner);

        final List<? extends Geometry> parts = outer.geometries();
        assertEquals(2, parts.size());
        assertEquals(p1, parts.get(0));
        assertEquals(inner, parts.get(1));
    }

    // Hierarchy instanceof checks
    @Test
    public void testHierarchy() {
        assertThat(LineString.of(Point.of(0, 0), Point.of(1, 1)), instanceOf(Curve.class));

        final LineString shell = LineString.of(
                Point.of(0, 0), Point.of(1, 0), Point.of(1, 1), Point.of(0, 0)
        );
        assertThat(Polygon.of(shell), instanceOf(Surface.class));

        assertThat(MultiPoint.of(Point.of(0, 0)), instanceOf(GeometryCollection.class));
        assertThat(MultiLineString.of(shell), instanceOf(MultiCurve.class));
        assertThat(MultiPolygon.of(Polygon.of(shell)), instanceOf(MultiSurface.class));
    }

    // points(), interiorRings(), geometries() - unmodifiable and defensive copies
    @Test
    public void testImmutability() {
        // LineString.points() is unmodifiable
        final LineString ls = LineString.of(Point.of(0, 0), Point.of(1, 1));
        try {
            ls.points().add(Point.of(2, 2));
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }

        // Modifying source array doesn't affect created geometry
        final Point[] points = {Point.of(0, 0), Point.of(1, 1)};
        final LineString ls2 = LineString.of(points);
        points[0] = Point.of(5, 5);
        assertEquals(Point.of(0, 0), ls2.points().get(0));

        // GeometryCollection.geometries() is unmodifiable
        final GeometryCollection gc = GeometryCollection.of(Point.of(0, 0));
        try {
            gc.geometries().clear();
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    // Every list accessor rejects mutation
    @Test
    public void testAccessorsUnmodifiable() {
        final LineString shell = LineString.of(
                Point.of(0, 0), Point.of(10, 0), Point.of(10, 10), Point.of(0, 0)
        );
        final LineString hole = LineString.of(
                Point.of(1, 1), Point.of(2, 1), Point.of(2, 2), Point.of(1, 1)
        );
        final Polygon polygon = Polygon.of(shell, hole);

        assertUnsupported(() -> polygon.interiorRings().add(hole));
        assertUnsupported(() -> polygon.interiorRings().remove(0));
        assertUnsupported(() -> polygon.interiorRings().set(0, shell));
        assertUnsupported(() -> polygon.interiorRings().clear());
        assertUnsupported(() -> polygon.exteriorRing().points().add(Point.of(3, 3)));

        final MultiPoint mp = MultiPoint.of(Point.of(0, 0), Point.empty());
        assertUnsupported(() -> mp.geometries().add(Point.of(1, 1)));
        assertUnsupported(() -> mp.geometries().clear());

        final MultiLineString mls = MultiLineString.of(shell);
        assertUnsupported(() -> mls.geometries().add(hole));
        assertUnsupported(() -> mls.geometries().set(0, hole));
        assertUnsupported(() -> mls.geometries().clear());

        final MultiPolygon mpoly = MultiPolygon.of(polygon);
        assertUnsupported(() -> mpoly.geometries().add(polygon));
        assertUnsupported(() -> mpoly.geometries().set(0, polygon));
        assertUnsupported(() -> mpoly.geometries().clear());

        final GeometryCollection gc = GeometryCollection.of(Point.of(0, 0), polygon);
        assertUnsupported(() -> gc.geometries().remove(0));
        assertUnsupported(() -> gc.geometries().clear());
    }

    // geometries() on the multi-part kinds is typed to its element kind
    @Test
    public void testNarrowedGeometriesElementType() {
        final MultiPoint mp = MultiPoint.of(Point.of(1, 2), Point.of(3, 4));
        final List<Point> points = mp.geometries();
        assertEquals(2, points.size());
        final Point p = points.get(0);
        assertEquals(1.0, p.x(), 0.0);
        assertEquals(2.0, p.y(), 0.0);
        assertUnsupported(() -> mp.geometries().add(Point.of(5, 6)));

        final LineString ring = LineString.of(
                Point.of(0, 0), Point.of(10, 0), Point.of(10, 10), Point.of(0, 0)
        );
        final MultiLineString mls = MultiLineString.of(ring);
        final List<LineString> lineStrings = mls.geometries();
        assertEquals(1, lineStrings.size());
        final LineString ls = lineStrings.get(0);
        assertEquals(4, ls.points().size());
        assertEquals(Point.of(0, 0), ls.points().get(0));
        assertUnsupported(() -> mls.geometries().add(ring));

        final Polygon polygon = Polygon.of(ring);
        final MultiPolygon mpoly = MultiPolygon.of(polygon);
        final List<Polygon> polygons = mpoly.geometries();
        assertEquals(1, polygons.size());
        final Polygon poly = polygons.get(0);
        assertEquals(ring, poly.exteriorRing());
        assertTrue(poly.interiorRings().isEmpty());
        assertUnsupported(() -> mpoly.geometries().add(polygon));
    }

    // Mutating an array passed in afterwards does not change the value
    @Test
    public void testDefensiveCopies() {
        final Point[] points = {Point.of(0, 0), Point.of(1, 1)};
        final LineString ls = LineString.of(points);
        points[0] = Point.of(9, 9);
        assertEquals(2, ls.points().size());
        assertEquals(Point.of(0, 0), ls.points().get(0));

        final Point[] mpPoints = {Point.of(0, 0)};
        final MultiPoint mp = MultiPoint.of(mpPoints);
        mpPoints[0] = Point.of(1, 1);
        assertEquals(1, mp.geometries().size());
        assertEquals(Point.of(0, 0), mp.geometries().get(0));

        final LineString ring = LineString.of(
                Point.of(0, 0), Point.of(10, 0), Point.of(10, 10), Point.of(0, 0)
        );
        final LineString[] rings = {ring};
        final MultiLineString mls = MultiLineString.of(rings);
        rings[0] = LineString.of(
                Point.of(0, 0), Point.of(20, 0), Point.of(20, 20), Point.of(0, 0)
        );
        assertEquals(1, mls.geometries().size());
        assertEquals(ring, mls.geometries().get(0));

        final Polygon firstPolygon = Polygon.of(ring);
        final Polygon[] polygons = {firstPolygon};
        final MultiPolygon mpoly = MultiPolygon.of(polygons);
        polygons[0] = Polygon.empty();
        assertEquals(1, mpoly.geometries().size());
        assertEquals(firstPolygon, mpoly.geometries().get(0));

        final Geometry[] parts = {Point.of(0, 0), Point.of(1, 1)};
        final GeometryCollection gc = GeometryCollection.of(parts);
        parts[0] = Point.of(7, 7);
        assertEquals(Point.of(0, 0), gc.geometries().get(0));

        final LineString hole = LineString.of(
                Point.of(1, 1), Point.of(2, 1), Point.of(2, 2), Point.of(1, 1)
        );
        final LineString[] holes = {hole};
        final Polygon polygon = Polygon.of(ring, holes);
        holes[0] = ring;
        assertEquals(hole, polygon.interiorRings().get(0));
    }

    // isEmpty() deep check
    @Test
    public void testIsEmptyDeep() {
        assertTrue(Point.empty().isEmpty());
        assertTrue(GeometryCollection.empty().isEmpty());

        // GEOMETRYCOLLECTION(POINT EMPTY) is empty
        final GeometryCollection gcWithEmpty = GeometryCollection.of(Point.empty());
        assertTrue(gcWithEmpty.isEmpty());

        // MULTIPOINT(EMPTY, EMPTY) is empty
        final Geometry mp = Geometry.fromWkt("MULTIPOINT(EMPTY, EMPTY)");
        assertTrue(mp.isEmpty());
    }

    // GEOMETRYCOLLECTION(POINT EMPTY, POINT(1 2)) is not empty
    @Test
    public void testIsEmptyWithMixed() {
        final GeometryCollection gc = GeometryCollection.of(Point.empty(), Point.of(1, 2));
        assertFalse(gc.isEmpty());
    }

    // POINT Z EMPTY has hasZ() true
    @Test
    public void testEmptyPointZ() {
        final Geometry p = Geometry.fromWkt("POINT Z EMPTY");
        assertTrue(p.isEmpty());
        assertTrue(p.hasZ());
    }

    // Collections nested 32, 33 and 100000 levels deep
    @Test
    public void testNestingDepth() {
        // Build 32 levels - should succeed
        Geometry g = Point.of(0, 0);
        for (int i = 0; i < 32; i++) {
            g = GeometryCollection.of(g);
        }
        assertFalse(g.isEmpty());

        // 33 levels - should fail
        try {
            Geometry inner = Point.of(0, 0);
            for (int i = 0; i < 33; i++) {
                inner = GeometryCollection.of(inner);
            }
            fail("Expected IAE for 33-level nesting");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // 31 collection levels with a MultiPolygon inside - should succeed
        // (Multi* counts as one level, polygon rings don't count)
        final LineString ring = LineString.of(
                Point.of(0, 0), Point.of(1, 0), Point.of(1, 1), Point.of(0, 0)
        );
        final Polygon poly = Polygon.of(ring);
        final MultiPolygon mp = MultiPolygon.of(poly);
        g = mp;
        for (int i = 0; i < 31; i++) {
            g = GeometryCollection.of(g);
        }
        assertFalse(g.isEmpty());
    }

    // A very deep nesting attempt fails with IAE rather than a StackOverflowError
    @Test
    public void testHugeNestingThroughJavaApi() {
        Geometry g = Point.of(0, 0);
        int levels = 0;
        try {
            for (int i = 0; i < 100000; i++) {
                g = GeometryCollection.of(g);
                levels++;
            }
            fail("Expected IAE before reaching 100000 levels");
        } catch (IllegalArgumentException e) {
            assertEquals(32, levels);
        }
    }

    // Two Point.of(1, 2) - equal, same hashCode
    @Test
    public void testPointEquality() {
        final Point p1 = Point.of(1, 2);
        final Point p2 = Point.of(1, 2);
        assertEquals(p1, p2);
        assertEquals(p1.hashCode(), p2.hashCode());
    }

    // Same coordinates, SRID 0 and 4326 - not equal
    @Test
    public void testDifferentSridNotEqual() {
        final Point p1 = Point.of(1, 2);
        final Point p2 = Point.of(1, 2, Model.GEOGRAPHIC);
        assertNotEquals(p1, p2);
    }

    // POINT(1 2) and POINT Z (1 2 0) - not equal
    @Test
    public void test2Dvs3DNotEqual() {
        final Point p2d = Point.of(1, 2);
        final Point p3d = Point.of(1, 2, 0);
        assertNotEquals(p2d, p3d);
    }

    // POINT(0 0) and MULTIPOINT((0 0)) - not equal
    @Test
    public void testPointVsMultiPointNotEqual() {
        final Point p = Point.of(0, 0);
        final MultiPoint mp = MultiPoint.of(Point.of(0, 0));
        assertNotEquals(p, mp);
    }

    // LINESTRING(0 0, 1 1) and LINESTRING(1 1, 0 0) - not equal
    @Test
    public void testLineStringOrderMatters() {
        final LineString ls1 = LineString.of(Point.of(0, 0), Point.of(1, 1));
        final LineString ls2 = LineString.of(Point.of(1, 1), Point.of(0, 0));
        assertNotEquals(ls1, ls2);
    }

    // Polygon and same ring started at another vertex - not equal
    @Test
    public void testPolygonRingStartMatters() {
        final LineString ring1 = LineString.of(
                Point.of(0, 0), Point.of(1, 0), Point.of(1, 1), Point.of(0, 0)
        );
        final LineString ring2 = LineString.of(
                Point.of(1, 0), Point.of(1, 1), Point.of(0, 0), Point.of(1, 0)
        );
        final Polygon p1 = Polygon.of(ring1);
        final Polygon p2 = Polygon.of(ring2);
        assertNotEquals(p1, p2);
    }

    // GeometryCollection and same parts in another order - not equal
    @Test
    public void testCollectionOrderMatters() {
        final Point a = Point.of(0, 0);
        final Point b = Point.of(1, 1);
        final GeometryCollection gc1 = GeometryCollection.of(a, b);
        final GeometryCollection gc2 = GeometryCollection.of(b, a);
        assertNotEquals(gc1, gc2);
    }

    // POINT EMPTY and LINESTRING EMPTY - not equal
    @Test
    public void testEmptyDifferentKindsNotEqual() {
        final Point p = Point.empty();
        final LineString ls = LineString.empty();
        assertNotEquals(p, ls);
    }

    // POINT EMPTY in CARTESIAN and GEOGRAPHIC - not equal
    @Test
    public void testEmptyDifferentModelsNotEqual() {
        final Point p1 = Point.empty();
        final Point p2 = Point.empty(Model.GEOGRAPHIC);
        assertNotEquals(p1, p2);
    }

    // POINT EMPTY and POINT Z EMPTY - not equal
    @Test
    public void testEmpty2DVs3DNotEqual() {
        final Geometry p1 = Geometry.fromWkt("POINT EMPTY");
        final Geometry p2 = Geometry.fromWkt("POINT Z EMPTY");
        assertNotEquals(p1, p2);
    }

    // POINT(0 0) and POINT(-0.0 0) - equal, same hashCode, keeps own value
    @Test
    public void testZeroEquality() {
        final Point p1 = Point.of(0.0, 0.0);
        final Point p2 = Point.of(-0.0, 0.0);
        assertEquals(p1, p2);
        assertEquals(p1.hashCode(), p2.hashCode());
        // Each keeps its own value for WKT output
        assertEquals("POINT(0 0)", p1.toWKT());
        assertEquals("POINT(-0 0)", p2.toWKT());
    }

    // A geometry and null, a String or its own WKT - not equal, through Geometry.equals
    @Test
    public void testNotEqualToOtherTypes() {
        final Point p = Point.of(1, 2);
        assertNotEquals(p, null);
        assertFalse(p.equals(null));
        assertFalse(p.equals("POINT(1 2)"));
        assertFalse(p.equals(p.toWKT()));
        assertFalse(p.equals(1));

        final GeometryCollection gc = GeometryCollection.of(p);
        assertFalse(gc.equals(null));
        assertFalse(gc.equals(gc.toWKT()));
        assertFalse(gc.equals(MultiPoint.of(p)));

        final LineString ls = LineString.of(Point.of(0, 0), Point.of(1, 1));
        assertFalse(ls.equals(null));
        assertFalse(ls.equals(ls.toWKT()));

        final LineString ring = LineString.of(
                Point.of(0, 0), Point.of(1, 0), Point.of(1, 1), Point.of(0, 0)
        );
        assertFalse(Polygon.of(ring).equals(null));
        assertFalse(MultiPoint.of(p).equals(null));
        assertFalse(MultiLineString.of(ls).equals(null));
        assertFalse(MultiPolygon.of(Polygon.of(ring)).equals(null));
    }

    // An empty point cannot be a LineString vertex
    @Test
    public void testEmptyPointRejectedAsVertex() {
        assertIae(() -> LineString.of(Point.empty(), Point.of(1, 1)));
        assertIae(() -> LineString.of(Point.of(1, 1), Point.empty()));
        assertIae(() -> LineString.of(Point.of(0, 0), Point.empty(), Point.of(1, 1)));

        // A MultiPoint, by contrast, may hold empty points
        assertEquals(2, MultiPoint.of(Point.empty(), Point.of(1, 1)).geometries().size());
    }

    // An empty point cannot be a polygon ring vertex
    @Test
    public void testEmptyPointRejectedAsRingVertex() {
        assertIae(() -> LineString.of(
                Point.empty(), Point.of(1, 0), Point.of(1, 1), Point.empty()
        ));
    }

    // A polygon with an empty shell cannot have holes, and a hole cannot be empty
    @Test
    public void testEmptyShellWithHolesRejected() {
        final LineString hole = LineString.of(
                Point.of(1, 1), Point.of(2, 1), Point.of(2, 2), Point.of(1, 1)
        );
        assertIae(() -> Polygon.of(LineString.empty(), hole));

        final LineString shell = LineString.of(
                Point.of(0, 0), Point.of(10, 0), Point.of(10, 10), Point.of(0, 0)
        );
        assertIae(() -> Polygon.of(shell, LineString.empty()));
        assertIae(() -> Polygon.of(LineString.empty(), LineString.empty()));

        // An empty shell with no holes is the empty polygon
        final Polygon empty = Polygon.of(LineString.empty());
        assertTrue(empty.isEmpty());
        assertTrue(empty.interiorRings().isEmpty());
    }

    // Public factories reject a null element, list or array with IAE
    @Test
    public void testNullArgumentsRejected() {
        // Model arguments
        assertIae(() -> Point.of(1, 2, (Model) null));
        assertIae(() -> Point.of(1, 2, 3, (Model) null));
        assertIae(() -> Point.empty(null));
        assertIae(() -> LineString.empty(null));
        assertIae(() -> Polygon.empty(null));
        assertIae(() -> MultiPoint.empty(null));
        assertIae(() -> MultiLineString.empty(null));
        assertIae(() -> MultiPolygon.empty(null));
        assertIae(() -> GeometryCollection.empty(null));
        assertIae(() -> Geometry.fromWkt("POINT(0 0)", (Model) null));

        // Null arrays
        assertIae(() -> LineString.of((Point[]) null));
        assertIae(() -> MultiPoint.of((Point[]) null));
        assertIae(() -> MultiLineString.of((LineString[]) null));
        assertIae(() -> MultiPolygon.of((Polygon[]) null));
        assertIae(() -> GeometryCollection.of((Geometry[]) null));

        final LineString ring = LineString.of(
                Point.of(0, 0), Point.of(10, 0), Point.of(10, 10), Point.of(0, 0)
        );
        assertIae(() -> Polygon.of(ring, (LineString[]) null));
        assertIae(() -> Polygon.of(null, ring));

        // Null elements
        assertIae(() -> LineString.of(Point.of(0, 0), null));
        assertIae(() -> LineString.of(null, Point.of(0, 0)));
        assertIae(() -> MultiPoint.of(Point.of(0, 0), null));
        assertIae(() -> MultiPoint.of(null, Point.of(0, 0)));
        assertIae(() -> MultiLineString.of(ring, null));
        assertIae(() -> MultiLineString.of(null, ring));
        assertIae(() -> MultiPolygon.of(Polygon.of(ring), null));
        assertIae(() -> MultiPolygon.of(null, Polygon.of(ring)));
        assertIae(() -> GeometryCollection.of(Point.of(0, 0), null));
        assertIae(() -> GeometryCollection.of((Geometry) null));
        assertIae(() -> Polygon.of(ring, (LineString) null));

        // Null input to the parsers
        assertIae(() -> Geometry.fromWkt(null));
        assertIae(() -> Geometry.fromWkt(null, 4326));
        assertIae(() -> Geometry.fromWkb(null, 0));
    }

    private static void assertIae(final Runnable action) {
        try {
            action.run();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    private static void assertIaeMentioning(final String expectedInMessage, final Runnable action) {
        try {
            action.run();
            fail("Expected IllegalArgumentException mentioning " + expectedInMessage);
        } catch (IllegalArgumentException e) {
            assertTrue("Message should mention " + expectedInMessage + ", was: " + e.getMessage(),
                    e.getMessage().contains(expectedInMessage));
        }
    }

    private static void assertUnsupported(final Runnable action) {
        try {
            action.run();
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }
}

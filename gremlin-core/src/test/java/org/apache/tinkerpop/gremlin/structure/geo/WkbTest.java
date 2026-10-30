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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for WKB parsing and writing.
 */
public class WkbTest {

    // Each kind, 2D and Z, written then read - equal to original
    @Test
    public void testRoundTrip() {
        assertWkbRoundTrip(Point.of(1, 2));
        assertWkbRoundTrip(Point.of(1, 2, 3));
        assertWkbRoundTrip(LineString.of(Point.of(0, 0), Point.of(1, 1)));
        assertWkbRoundTrip(LineString.of(Point.of(0, 0, 0), Point.of(1, 1, 1)));

        final LineString ring = LineString.of(
                Point.of(0, 0), Point.of(1, 0), Point.of(1, 1), Point.of(0, 0)
        );
        assertWkbRoundTrip(Polygon.of(ring));

        final LineString ring3d = LineString.of(
                Point.of(0, 0, 0), Point.of(1, 0, 0), Point.of(1, 1, 0), Point.of(0, 0, 0)
        );
        assertWkbRoundTrip(Polygon.of(ring3d));

        assertWkbRoundTrip(MultiPoint.of(Point.of(0, 0), Point.of(1, 1)));
        assertWkbRoundTrip(MultiPoint.of(Point.of(0, 0, 0), Point.of(1, 1, 1)));

        assertWkbRoundTrip(MultiLineString.of(
                LineString.of(Point.of(0, 0), Point.of(1, 1)),
                LineString.of(Point.of(2, 2), Point.of(3, 3))
        ));

        assertWkbRoundTrip(MultiPolygon.of(Polygon.of(ring)));
        assertWkbRoundTrip(GeometryCollection.of(Point.of(0, 0), LineString.of(Point.of(0, 0), Point.of(1, 1))));
    }

    // Input in big-endian (XDR) and little-endian (NDR)
    @Test
    public void testBothByteOrders() {
        // Big-endian point
        final byte[] bigEndian = new byte[]{
                0x00, // XDR
                0x00, 0x00, 0x00, 0x01, // Point type
                0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // x = 2.0
                0x40, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00  // y = 3.0
        };
        final Geometry gBig = Geometry.fromWkb(bigEndian, 0);
        assertEquals(Point.of(2, 3), gBig);

        // Little-endian point
        final byte[] littleEndian = new byte[]{
                0x01, // NDR
                0x01, 0x00, 0x00, 0x00, // Point type
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // x = 2.0 (LE)
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40  // y = 3.0 (LE)
        };
        final Geometry gLittle = Geometry.fromWkb(littleEndian, 0);
        assertEquals(Point.of(2, 3), gLittle);

        assertEquals(gBig, gLittle);

        // Collection with mixed byte orders
        // MultiPoint with two points, first big-endian, second little-endian
        // We'll build this manually
        final ByteBuffer bb = ByteBuffer.allocate(100);
        bb.order(ByteOrder.BIG_ENDIAN);
        bb.put((byte) 0x00); // XDR outer
        bb.putInt(4); // MultiPoint
        bb.putInt(2); // count

        // First point: big-endian
        bb.put((byte) 0x00);
        bb.putInt(1);
        bb.putDouble(1.0);
        bb.putDouble(2.0);

        // Second point: little-endian
        bb.put((byte) 0x01);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(1);
        bb.putDouble(3.0);
        bb.putDouble(4.0);

        final byte[] mixed = new byte[bb.position()];
        bb.flip();
        bb.get(mixed);

        final Geometry mixedGeom = Geometry.fromWkb(mixed, 0);
        assertTrue(mixedGeom instanceof MultiPoint);
        final MultiPoint mp = (MultiPoint) mixedGeom;
        assertEquals(2, mp.geometries().size());
        assertEquals(Point.of(1, 2), mp.geometries().get(0));
        assertEquals(Point.of(3, 4), mp.geometries().get(1));
    }

    // Output declares big-endian at every geometry header, including nested parts
    @Test
    public void testOutputIsBigEndian() {
        final String[] values = {
                "POINT(1 2)", "POINT Z (1 2 3)",
                "LINESTRING(0 0, 1 1)", "LINESTRING Z (0 0 0, 1 1 1)",
                "POLYGON((0 0, 1 0, 1 1, 0 0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1))",
                "POLYGON Z ((0 0 0, 1 0 0, 1 1 0, 0 0 0))",
                "MULTIPOINT((0 0), (1 1))", "MULTIPOINT Z ((0 0 0), (1 1 1))",
                "MULTILINESTRING((0 0, 1 1), (2 2, 3 3))", "MULTILINESTRING Z ((0 0 0, 1 1 1))",
                "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)), ((2 2, 3 2, 3 3, 2 2)))",
                "MULTIPOLYGON Z (((0 0 0, 1 0 0, 1 1 0, 0 0 0)))",
                "GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 1))",
                "GEOMETRYCOLLECTION Z (POINT Z (0 0 0), LINESTRING Z (0 0 0, 1 1 1))",
                // every kind nested inside a collection, two levels deep
                "GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 1), "
                        + "POLYGON((0 0, 1 0, 1 1, 0 0)), MULTIPOINT((0 0), EMPTY), "
                        + "MULTILINESTRING(EMPTY, (0 0, 1 1)), MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0))), "
                        + "GEOMETRYCOLLECTION(POINT EMPTY, MULTIPOINT((2 2)), "
                        + "GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1))))",
                "GEOMETRYCOLLECTION Z (POINT Z (0 0 0), LINESTRING Z (0 0 0, 1 1 1), "
                        + "POLYGON Z ((0 0 0, 1 0 0, 1 1 0, 0 0 0)), MULTIPOINT Z ((0 0 0), EMPTY), "
                        + "MULTILINESTRING Z (EMPTY, (0 0 0, 1 1 1)), "
                        + "MULTIPOLYGON Z (((0 0 0, 1 0 0, 1 1 0, 0 0 0))), "
                        + "GEOMETRYCOLLECTION Z (POINT Z EMPTY, "
                        + "GEOMETRYCOLLECTION Z (LINESTRING Z (0 0 0, 1 1 1))))"
        };

        for (final String wkt : values) {
            final ByteBuffer bb = ByteBuffer.wrap(Geometry.fromWkt(wkt).toWKB()).order(ByteOrder.BIG_ENDIAN);
            final int headers = assertBigEndianHeaders(bb, wkt);
            assertTrue("No header read for " + wkt, headers > 0);
            assertFalse("Bytes left over after walking the WKB of " + wkt, bb.hasRemaining());
        }
    }

    /**
     * Walks one geometry in the buffer, asserting that its header and the header of every
     * nested part declares big-endian byte order. Returns the number of headers checked.
     */
    private static int assertBigEndianHeaders(final ByteBuffer bb, final String context) {
        assertEquals("Byte order byte in the WKB of " + context, 0x00, bb.get());

        final int typeCode = bb.getInt();
        final boolean z = typeCode > 1000;
        final int baseType = z ? typeCode - 1000 : typeCode;
        final int ordinates = z ? 3 : 2;
        int headers = 1;

        switch (baseType) {
            case 1: // Point, whose coordinate follows the header directly
                skipCoordinates(bb, 1, ordinates);
                break;
            case 2: // LineString
                skipCoordinates(bb, bb.getInt(), ordinates);
                break;
            case 3: // Polygon, whose rings carry no header of their own
                final int rings = bb.getInt();
                for (int i = 0; i < rings; i++) {
                    skipCoordinates(bb, bb.getInt(), ordinates);
                }
                break;
            case 4: // MultiPoint
            case 5: // MultiLineString
            case 6: // MultiPolygon
            case 7: // GeometryCollection
                final int parts = bb.getInt();
                for (int i = 0; i < parts; i++) {
                    headers += assertBigEndianHeaders(bb, context);
                }
                break;
            default:
                fail("Unexpected WKB type code " + typeCode + " in the WKB of " + context);
        }

        return headers;
    }

    private static void skipCoordinates(final ByteBuffer bb, final int count, final int ordinates) {
        bb.position(bb.position() + count * ordinates * Double.BYTES);
    }

    // Type codes
    @Test
    public void testTypeCodes() {
        // Point = 1, Point Z = 1001
        verifyTypeCode(Point.of(1, 2), 1);
        verifyTypeCode(Point.of(1, 2, 3), 1001);

        // LineString = 2, LineString Z = 1002
        verifyTypeCode(LineString.of(Point.of(0, 0), Point.of(1, 1)), 2);
        verifyTypeCode(LineString.of(Point.of(0, 0, 0), Point.of(1, 1, 1)), 1002);

        // Polygon = 3
        final LineString ring = LineString.of(Point.of(0, 0), Point.of(1, 0), Point.of(1, 1), Point.of(0, 0));
        verifyTypeCode(Polygon.of(ring), 3);

        // MultiPoint = 4
        verifyTypeCode(MultiPoint.of(Point.of(0, 0)), 4);

        // MultiLineString = 5
        verifyTypeCode(MultiLineString.of(LineString.of(Point.of(0, 0), Point.of(1, 1))), 5);

        // MultiPolygon = 6
        verifyTypeCode(MultiPolygon.of(Polygon.of(ring)), 6);

        // GeometryCollection = 7
        verifyTypeCode(GeometryCollection.of(Point.of(0, 0)), 7);
    }

    // POINT EMPTY as NaN coordinates, read back as empty
    @Test
    public void testEmptyPoint() {
        final Point empty = Point.empty();
        final byte[] wkb = empty.toWKB();

        // Should contain NaN coordinates
        final ByteBuffer bb = ByteBuffer.wrap(wkb);
        bb.order(ByteOrder.BIG_ENDIAN);
        bb.get(); // byte order
        bb.getInt(); // type
        final double x = bb.getDouble();
        final double y = bb.getDouble();
        assertTrue(Double.isNaN(x));
        assertTrue(Double.isNaN(y));

        // Read back as empty
        final Geometry read = Geometry.fromWkb(wkb, 0);
        assertTrue(read.isEmpty());

        // Empty point inside MultiPoint
        final MultiPoint mp = MultiPoint.of(Point.empty(), Point.of(1, 1));
        final Geometry mpRead = Geometry.fromWkb(mp.toWKB(), 0);
        assertTrue(mpRead instanceof MultiPoint);
        assertTrue(((MultiPoint) mpRead).geometries().get(0).isEmpty());
    }

    // Doubles including -0.0, subnormals, MAX_VALUE - bit-identical after round trip
    @Test
    public void testDoublePrecision() {
        // -0.0 preserved
        final Point negZero = Point.of(-0.0, 0.0);
        final byte[] wkb = negZero.toWKB();
        final Point read = (Point) Geometry.fromWkb(wkb, 0);
        assertEquals(Long.MIN_VALUE, Double.doubleToRawLongBits(read.x())); // -0.0

        // Subnormals
        final Point subnormal = Point.of(Double.MIN_VALUE, -Double.MIN_VALUE);
        final Point subRead = (Point) Geometry.fromWkb(subnormal.toWKB(), 0);
        assertEquals(Double.doubleToRawLongBits(Double.MIN_VALUE), Double.doubleToRawLongBits(subRead.x()));

        // MAX_VALUE
        final Point maxVal = Point.of(Double.MAX_VALUE, -Double.MAX_VALUE);
        final Point maxRead = (Point) Geometry.fromWkb(maxVal.toWKB(), 0);
        assertEquals(Double.MAX_VALUE, maxRead.x(), 0.0);
        assertEquals(-Double.MAX_VALUE, maxRead.y(), 0.0);
    }

    // fromWkb with SRID argument
    @Test
    public void testSridFromArgument() {
        final byte[] wkb = Point.of(1, 2).toWKB();

        final Geometry g1 = Geometry.fromWkb(wkb, 4326);
        assertEquals(Model.GEOGRAPHIC, g1.model());
        assertEquals(4326, g1.srid());

        final Geometry g2 = Geometry.fromWkb(wkb, 32618);
        assertEquals(Model.OTHER, g2.model());
        assertEquals(32618, g2.srid());
    }

    // WKT and WKB agree
    @Test
    public void testWktWkbAgree() {
        final Point p = Point.of(1.5, -2.5, Model.GEOGRAPHIC);
        final Geometry fromWkb = Geometry.fromWkb(p.toWKB(), p.srid());
        final Geometry fromEwkt = Geometry.fromWkt(p.toEWKT());
        assertEquals(p, fromWkb);
        assertEquals(p, fromEwkt);
    }

    // Truncated input, trailing bytes, unknown type code
    @Test
    public void testMalformedWkb() {
        // Truncated
        final byte[] truncated = new byte[]{0x00, 0x00, 0x00, 0x00, 0x01}; // no coordinates
        try {
            Geometry.fromWkb(truncated, 0);
            fail("Expected IAE for truncated input");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // Trailing bytes
        final byte[] withTrailing = new byte[21 + 5]; // point + 5 extra bytes
        System.arraycopy(Point.of(1, 2).toWKB(), 0, withTrailing, 0, 21);
        try {
            Geometry.fromWkb(withTrailing, 0);
            fail("Expected IAE for trailing bytes");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // Unknown type code (8)
        final byte[] unknownType = new byte[]{
                0x00, // XDR
                0x00, 0x00, 0x00, 0x08 // unknown type
        };
        try {
            Geometry.fromWkb(unknownType, 0);
            fail("Expected IAE for unknown type code");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    // Count larger than bytes remaining
    @Test
    public void testOversizedCount() {
        // LineString with count 0x7fffffff but only 20 bytes
        final byte[] oversized = new byte[]{
                0x00, // XDR
                0x00, 0x00, 0x00, 0x02, // LineString
                0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff // count = MAX_INT
        };
        try {
            Geometry.fromWkb(oversized, 0);
            fail("Expected IAE for oversized count");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("count") || e.getMessage().contains("exceed"));
        }
    }

    // EWKB input rejected
    @Test
    public void testEwkbRejected() {
        // Z flag (0x80000000)
        final byte[] ewkbZ = new byte[]{
                0x00, // XDR
                (byte) 0x80, 0x00, 0x00, 0x01, // type with Z flag
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // x
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00  // z
        };
        try {
            Geometry.fromWkb(ewkbZ, 0);
            fail("Expected IAE for EWKB Z flag");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("EWKB") && e.getMessage().contains("ISO WKB"));
        }

        // M flag (0x40000000)
        final byte[] ewkbM = new byte[]{
                0x00,
                0x40, 0x00, 0x00, 0x01
        };
        try {
            Geometry.fromWkb(ewkbM, 0);
            fail("Expected IAE for EWKB M flag");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("EWKB"));
        }

        // SRID flag (0x20000000)
        final byte[] ewkbSrid = new byte[]{
                0x00,
                0x20, 0x00, 0x00, 0x01
        };
        try {
            Geometry.fromWkb(ewkbSrid, 0);
            fail("Expected IAE for EWKB SRID flag");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("EWKB"));
        }
    }

    // Nesting depth limits
    @Test
    public void testNestingDepthWkb() {
        // Build 32 levels through WKT then convert to WKB
        StringBuilder wkt = new StringBuilder("POINT(0 0)");
        for (int i = 0; i < 32; i++) {
            wkt = new StringBuilder("GEOMETRYCOLLECTION(" + wkt + ")");
        }
        final Geometry g32 = Geometry.fromWkt(wkt.toString());
        final byte[] wkb32 = g32.toWKB();
        final Geometry read32 = Geometry.fromWkb(wkb32, 0);
        assertTrue(read32 instanceof GeometryCollection);

        // 33 levels - build WKB manually
        // Build 33 levels - this will fail on parsing
        wkt = new StringBuilder("POINT(0 0)");
        for (int i = 0; i < 33; i++) {
            wkt = new StringBuilder("GEOMETRYCOLLECTION(" + wkt + ")");
        }
        try {
            Geometry.fromWkt(wkt.toString());
            fail("Expected IAE for 33-level nesting from WKT");
        } catch (IllegalArgumentException e) {
            // expected - can't even create the WKT
        }
    }

    // M and ZM type codes rejected
    @Test
    public void testMCodesRejected() {
        // M Point (2001)
        final byte[] mPoint = new byte[]{
                0x00, // XDR
                0x00, 0x00, 0x07, (byte) 0xd1 // 2001
        };
        try {
            Geometry.fromWkb(mPoint, 0);
            fail("Expected IAE for M type code");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("M dimension"));
        }

        // ZM Point (3001)
        final byte[] zmPoint = new byte[]{
                0x00, // XDR
                0x00, 0x00, 0x0b, (byte) 0xb9 // 3001
        };
        try {
            Geometry.fromWkb(zmPoint, 0);
            fail("Expected IAE for ZM type code");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("ZM dimension"));
        }
    }

    // Collection with mixed dimension type codes
    @Test
    public void testMixedDimensionCodes() {
        // MultiPoint with first point 2D (type 1) and second 3D (type 1001)
        final ByteBuffer bb = ByteBuffer.allocate(100);
        bb.order(ByteOrder.BIG_ENDIAN);
        bb.put((byte) 0x00); // XDR
        bb.putInt(4); // MultiPoint
        bb.putInt(2); // count

        // First point: 2D
        bb.put((byte) 0x00);
        bb.putInt(1); // 2D Point
        bb.putDouble(1.0);
        bb.putDouble(2.0);

        // Second point: 3D (type 1001)
        bb.put((byte) 0x00);
        bb.putInt(1001); // 3D Point
        bb.putDouble(3.0);
        bb.putDouble(4.0);
        bb.putDouble(5.0);

        final byte[] mixed = new byte[bb.position()];
        bb.flip();
        bb.get(mixed);

        try {
            Geometry.fromWkb(mixed, 0);
            fail("Expected IAE for mixed dimension codes");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Mixed Z"));
        }
    }

    // JTS-written 3D WKB (EWKB format) rejected; 2D reads
    @Test
    public void testJtsEwkb() {
        // JTS writes 3D points with EWKB Z flag (0x80000001)
        // This is hand-constructed to match JTS WKBWriter output for 3D
        final byte[] jts3dWkb = new byte[]{
                0x00, // XDR (JTS uses big-endian by default)
                (byte) 0x80, 0x00, 0x00, 0x01, // type 0x80000001 (EWKB Z flag)
                0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // x = 2.0
                0x40, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y = 3.0
                0x40, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00  // z = 4.0
        };
        try {
            Geometry.fromWkb(jts3dWkb, 0);
            fail("Expected IAE for JTS 3D EWKB format");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("EWKB"));
        }

        // JTS 2D WKB is identical to ISO WKB, so it should work
        final byte[] jts2dWkb = new byte[]{
                0x00, // XDR
                0x00, 0x00, 0x00, 0x01, // Point type 1
                0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // x = 2.0
                0x40, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00  // y = 3.0
        };
        final Geometry g = Geometry.fromWkb(jts2dWkb, 0);
        assertEquals(Point.of(2, 3), g);
    }

    // Malformed WKB that JTS would repair - IAE, not repaired
    @Test
    public void testMalformedNotRepaired() {
        // 1-point LineString
        ByteBuffer bb = ByteBuffer.allocate(50);
        bb.order(ByteOrder.BIG_ENDIAN);
        bb.put((byte) 0x00);
        bb.putInt(2); // LineString
        bb.putInt(1); // only 1 point
        bb.putDouble(1.0);
        bb.putDouble(2.0);

        final byte[] onePoint = new byte[bb.position()];
        bb.flip();
        bb.get(onePoint);

        try {
            Geometry.fromWkb(onePoint, 0);
            fail("Expected IAE for 1-point LineString");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("at least 2 points"));
        }

        // Unclosed ring in polygon - needs 13 header bytes + 64 coordinate bytes = 77 bytes
        bb = ByteBuffer.allocate(100);
        bb.order(ByteOrder.BIG_ENDIAN);
        bb.put((byte) 0x00);
        bb.putInt(3); // Polygon
        bb.putInt(1); // 1 ring
        bb.putInt(4); // 4 points
        // Ring: (0,0), (1,0), (1,1), (0,1) - NOT closed
        bb.putDouble(0.0); bb.putDouble(0.0);
        bb.putDouble(1.0); bb.putDouble(0.0);
        bb.putDouble(1.0); bb.putDouble(1.0);
        bb.putDouble(0.0); bb.putDouble(1.0);

        final byte[] unclosed = new byte[bb.position()];
        bb.flip();
        bb.get(unclosed);

        try {
            Geometry.fromWkb(unclosed, 0);
            fail("Expected IAE for unclosed ring");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("closed"));
        }
    }

    private void assertWkbRoundTrip(final Geometry g) {
        final byte[] wkb = g.toWKB();
        final Geometry read = Geometry.fromWkb(wkb, g.srid());
        assertEquals(g, read);
    }

    // WKT and WKB agree, for every kind in 2D and Z
    @Test
    public void testWktWkbAgreeForEveryKind() {
        final String[] values = {
                "POINT(1.5 -2.5)", "POINT Z (1.5 -2.5 3.5)",
                "LINESTRING(0 0, 1 1, 2 -2.5)", "LINESTRING Z (0 0 0, 1 1 1)",
                "POLYGON((0 0, 1 0, 1 1, 0 0))",
                "POLYGON((0 0, 10 0, 10 10, 0 0), (1 1, 2 1, 2 2, 1 1))",
                "POLYGON Z ((0 0 0, 1 0 0, 1 1 0, 0 0 0))",
                "MULTIPOINT((0 0), (1 1))", "MULTIPOINT Z ((0 0 0), (1 1 1))",
                "MULTILINESTRING((0 0, 1 1), (2 2, 3 3))",
                "MULTILINESTRING Z ((0 0 0, 1 1 1))",
                "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)), ((2 2, 3 2, 3 3, 2 2)))",
                "MULTIPOLYGON Z (((0 0 0, 1 0 0, 1 1 0, 0 0 0)))",
                "GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 1))",
                "GEOMETRYCOLLECTION Z (POINT Z (0 0 0), LINESTRING Z (0 0 0, 1 1 1))",
                "POINT EMPTY", "POINT Z EMPTY", "LINESTRING EMPTY", "POLYGON EMPTY",
                "MULTIPOINT EMPTY", "MULTILINESTRING EMPTY", "MULTIPOLYGON EMPTY",
                "GEOMETRYCOLLECTION EMPTY",
                "MULTIPOINT(EMPTY, EMPTY)", "MULTIPOINT((0 0), EMPTY)", "MULTIPOINT(EMPTY, (1 1))",
                "MULTIPOINT Z (EMPTY, (1 1 1))",
                "MULTILINESTRING(EMPTY, EMPTY)", "MULTIPOLYGON(EMPTY)",
                "GEOMETRYCOLLECTION(POINT EMPTY)", "GEOMETRYCOLLECTION Z (POINT Z EMPTY)",
                "GEOMETRYCOLLECTION(MULTIPOINT(EMPTY, (1 1)), MULTIPOLYGON(EMPTY), "
                        + "MULTILINESTRING(EMPTY, EMPTY), GEOMETRYCOLLECTION(POINT EMPTY))"
        };

        for (final String wkt : values) {
            assertWktWkbAgree(Geometry.fromWkt(wkt));
            assertWktWkbAgree(Geometry.fromWkt(wkt, 32618));
            assertWktWkbAgree(Geometry.fromWkt("SRID=4326;" + wkt));
        }
    }

    // Every count site rejects a hostile count in a tiny buffer
    @Test
    public void testHostileCountsAtEveryCountSite() {
        final int[] hostile = {0x7FFFFFFF, 0x02000000};
        for (final int count : hostile) {
            // LineString point count
            assertIae(header(2, count));
            // Polygon ring count
            assertIae(header(3, count));
            // Polygon ring point count, with one declared ring
            assertIae(concat(header(3, 1), intBytes(count)));
            // MultiPoint, MultiLineString, MultiPolygon and GeometryCollection element counts
            assertIae(header(4, count));
            assertIae(header(5, count));
            assertIae(header(6, count));
            assertIae(header(7, count));
        }
    }

    // Nesting depth is enforced on hand-built WKB, without a StackOverflowError
    @Test
    public void testNestingDepthFromBytes() {
        final Geometry accepted = Geometry.fromWkb(nestedCollections(32), 0);
        assertTrue(accepted instanceof GeometryCollection);
        assertEquals(32, depthOf(accepted));

        assertIae(nestedCollections(33));
        assertIae(nestedCollections(100000));
    }

    // Empty parts keep their place through a WKB round trip
    @Test
    public void testEmptyPartsKeepStructure() {
        for (final String wkt : new String[]{
                "MULTIPOINT(EMPTY, EMPTY)", "MULTIPOINT((0 0), EMPTY)", "MULTIPOINT(EMPTY, (1 1))",
                "GEOMETRYCOLLECTION(POINT EMPTY)", "GEOMETRYCOLLECTION Z (POINT Z EMPTY)",
                "MULTIPOLYGON(EMPTY)", "MULTILINESTRING(EMPTY, EMPTY)",
                "GEOMETRYCOLLECTION(MULTIPOINT(EMPTY, EMPTY), MULTIPOLYGON(EMPTY), "
                        + "MULTILINESTRING(EMPTY, EMPTY), GEOMETRYCOLLECTION(POINT EMPTY))"}) {
            final Geometry g = Geometry.fromWkt(wkt);
            final Geometry fromWkb = Geometry.fromWkb(g.toWKB(), g.srid());
            assertEquals(g, fromWkb);
            assertEquals(wkt, fromWkb.toWKT());
        }
    }

    // A NaN vertex in a LineString or a ring is malformed, not an empty point
    @Test
    public void testEmptyPointAsVertexRejected() {
        final long nan = Double.doubleToRawLongBits(Double.NaN);

        // LineString with a NaN first vertex
        final byte[] lineString = concat(header(2, 2),
                longBytes(nan), longBytes(nan),
                doubleBytes(1.0), doubleBytes(1.0));
        assertIae(lineString);

        // LineString with a NaN second vertex
        final byte[] lineString2 = concat(header(2, 2),
                doubleBytes(0.0), doubleBytes(0.0),
                longBytes(nan), longBytes(nan));
        assertIae(lineString2);

        // Polygon ring with a NaN vertex
        final byte[] ring = concat(header(3, 1), intBytes(4),
                longBytes(nan), longBytes(nan),
                doubleBytes(1.0), doubleBytes(0.0),
                doubleBytes(1.0), doubleBytes(1.0),
                longBytes(nan), longBytes(nan));
        assertIae(ring);

        // A MultiPoint, by contrast, reads a NaN point as empty
        final byte[] multiPoint = concat(header(4, 1), new byte[]{0x00}, intBytes(1),
                longBytes(nan), longBytes(nan));
        final Geometry mp = Geometry.fromWkb(multiPoint, 0);
        assertTrue(((MultiPoint) mp).geometries().get(0).isEmpty());
    }

    private void assertWktWkbAgree(final Geometry g) {
        assertEquals(g, Geometry.fromWkb(g.toWKB(), g.srid()));
        assertEquals(g, Geometry.fromWkt(g.toEWKT()));
        assertEquals(Geometry.fromWkb(g.toWKB(), g.srid()), Geometry.fromWkt(g.toEWKT()));
    }

    private static void assertIae(final byte[] wkb) {
        try {
            Geometry.fromWkb(wkb, 0);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    /**
     * Returns a big-endian geometry header followed by one count.
     */
    private static byte[] header(final int typeCode, final int count) {
        return concat(new byte[]{0x00}, intBytes(typeCode), intBytes(count));
    }

    private static byte[] intBytes(final int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(value).array();
    }

    private static byte[] longBytes(final long value) {
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(value).array();
    }

    private static byte[] doubleBytes(final double value) {
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(value).array();
    }

    private static byte[] concat(final byte[]... parts) {
        int length = 0;
        for (final byte[] part : parts) {
            length += part.length;
        }
        final ByteBuffer bb = ByteBuffer.allocate(length);
        for (final byte[] part : parts) {
            bb.put(part);
        }
        return bb.array();
    }

    /**
     * Returns the WKB of the given number of GeometryCollection levels wrapping a point.
     */
    private static byte[] nestedCollections(final int levels) {
        final ByteBuffer bb = ByteBuffer.allocate(levels * 9 + 21).order(ByteOrder.BIG_ENDIAN);
        for (int i = 0; i < levels; i++) {
            bb.put((byte) 0x00);
            bb.putInt(7);
            bb.putInt(1);
        }
        bb.put((byte) 0x00);
        bb.putInt(1);
        bb.putDouble(0.0);
        bb.putDouble(0.0);
        return bb.array();
    }

    private static int depthOf(final Geometry g) {
        if (g instanceof GeometryCollection) {
            int max = 0;
            for (final Geometry child : ((GeometryCollection) g).geometries()) {
                max = Math.max(max, depthOf(child));
            }
            return 1 + max;
        }
        return 0;
    }

    private void verifyTypeCode(final Geometry g, final int expectedTypeCode) {
        final byte[] wkb = g.toWKB();
        final ByteBuffer bb = ByteBuffer.wrap(wkb);
        bb.order(ByteOrder.BIG_ENDIAN);
        bb.get(); // skip byte order
        final int typeCode = bb.getInt();
        assertEquals(expectedTypeCode, typeCode);
    }
}

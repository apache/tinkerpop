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

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for WKT and EWKT parsing and writing.
 */
public class WktTest {

    // Each of the 7 kinds, 2D and Z, parsed then written
    @Test
    public void testAllKinds2DAndZ() {
        // Point 2D
        assertRoundTrip("POINT(1 2)");
        // Point Z
        assertRoundTrip("POINT Z (1 2 3)");

        // LineString 2D
        assertRoundTrip("LINESTRING(0 0, 1 1)");
        // LineString Z
        assertRoundTrip("LINESTRING Z (0 0 0, 1 1 1)");

        // Polygon 2D
        assertRoundTrip("POLYGON((0 0, 1 0, 1 1, 0 0))");
        // Polygon Z
        assertRoundTrip("POLYGON Z ((0 0 0, 1 0 0, 1 1 0, 0 0 0))");

        // MultiPoint 2D
        assertRoundTrip("MULTIPOINT((0 0), (1 1))");
        // MultiPoint Z
        assertRoundTrip("MULTIPOINT Z ((0 0 0), (1 1 1))");

        // MultiLineString 2D
        assertRoundTrip("MULTILINESTRING((0 0, 1 1), (2 2, 3 3))");
        // MultiLineString Z
        assertRoundTrip("MULTILINESTRING Z ((0 0 0, 1 1 1), (2 2 2, 3 3 3))");

        // MultiPolygon 2D
        assertRoundTrip("MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)), ((2 2, 3 2, 3 3, 2 2)))");
        // MultiPolygon Z
        assertRoundTrip("MULTIPOLYGON Z (((0 0 0, 1 0 0, 1 1 0, 0 0 0)))");

        // GeometryCollection 2D
        assertRoundTrip("GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 1))");
        // GeometryCollection Z
        assertRoundTrip("GEOMETRYCOLLECTION Z (POINT Z (0 0 0), LINESTRING Z (0 0 0, 1 1 1))");
    }

    // EMPTY for each kind, and POINT Z EMPTY
    @Test
    public void testEmpty() {
        assertRoundTrip("POINT EMPTY");
        assertRoundTrip("POINT Z EMPTY");
        assertRoundTrip("LINESTRING EMPTY");
        assertRoundTrip("POLYGON EMPTY");
        assertRoundTrip("MULTIPOINT EMPTY");
        assertRoundTrip("MULTILINESTRING EMPTY");
        assertRoundTrip("MULTIPOLYGON EMPTY");
        assertRoundTrip("GEOMETRYCOLLECTION EMPTY");
    }

    // Lower case, extra whitespace, POINT (1 2) with space
    @Test
    public void testCaseAndWhitespace() {
        assertEquals("POINT(1 2)", Geometry.fromWkt("point(1 2)").toWKT());
        assertEquals("POINT(1 2)", Geometry.fromWkt("POINT   (1    2)").toWKT());
        assertEquals("POINT(1 2)", Geometry.fromWkt("POINT (1 2)").toWKT());
        assertEquals("LINESTRING(0 0, 1 1)", Geometry.fromWkt("linestring ( 0 0 , 1 1 )").toWKT());
    }

    // MULTIPOINT(0 0, 1 1) and MULTIPOINT((0 0), (1 1)) - both accepted, written with inner parens
    @Test
    public void testMultiPointForms() {
        final String withParens = "MULTIPOINT((0 0), (1 1))";
        final String withoutParens = "MULTIPOINT(0 0, 1 1)";

        final Geometry g1 = Geometry.fromWkt(withParens);
        final Geometry g2 = Geometry.fromWkt(withoutParens);

        assertEquals(g1, g2);
        assertEquals(withParens, g1.toWKT());
        assertEquals(withParens, g2.toWKT());
    }

    // toString() equals toWKT()
    @Test
    public void testToStringEqualsToWkt() {
        final Point p = Point.of(1, 2);
        assertEquals(p.toWKT(), p.toString());

        final LineString ls = LineString.of(Point.of(0, 0), Point.of(1, 1));
        assertEquals(ls.toWKT(), ls.toString());
    }

    // Integral and fractional values written without trailing .0
    @Test
    public void testNumberFormat() {
        assertEquals("POINT(-74 40.7)", Geometry.fromWkt("POINT(-74 40.7)").toWKT());
        assertEquals("POINT(-74 40)", Geometry.fromWkt("POINT(-74.0 40.0)").toWKT());

        // -0.0 written as -0
        final Point p = Point.of(-0.0, 0.0);
        assertEquals("POINT(-0 0)", p.toWKT());
    }

    // Arbitrary finite doubles round-trip through WKT and through WKB
    @Test
    public void testRandomDoublesRoundTrip() {
        // Draw bit patterns rather than values in [0,1), so every exponent range is covered
        final Random random = new Random(12345);
        int drawn = 0;
        while (drawn < 10000) {
            final double value = Double.longBitsToDouble(random.nextLong());
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                continue;
            }
            assertDoubleRoundTrip(value);
            drawn++;
        }

        // Subnormals and the edges of the range
        assertDoubleRoundTrip(Double.MIN_VALUE);
        assertDoubleRoundTrip(-Double.MIN_VALUE);
        assertDoubleRoundTrip(Double.MIN_VALUE * 3);
        assertDoubleRoundTrip(Double.MIN_NORMAL / 2);
        assertDoubleRoundTrip(Double.MIN_NORMAL / 1024);
        assertDoubleRoundTrip(Math.nextDown(Double.MIN_NORMAL));
        assertDoubleRoundTrip(Double.MIN_NORMAL);
        assertDoubleRoundTrip(-Double.MIN_NORMAL);
        assertDoubleRoundTrip(Double.MAX_VALUE);
        assertDoubleRoundTrip(-Double.MAX_VALUE);
        assertDoubleRoundTrip(Math.nextDown(Double.MAX_VALUE));
        assertDoubleRoundTrip(0.0);
        assertDoubleRoundTrip(-0.0);
    }

    /**
     * Asserts that a CARTESIAN point holding the value reads back equal from its WKT, and with
     * bit-identical coordinates from its WKB.
     */
    private void assertDoubleRoundTrip(final double value) {
        final Point p = Point.of(value, value);
        final String wkt = p.toWKT();

        assertEquals("WKT " + wkt + " of " + value + " did not read back equal", p, Geometry.fromWkt(wkt));

        final Point fromWkb = (Point) Geometry.fromWkb(p.toWKB(), p.srid());
        assertEquals("WKB x of " + value + " was not bit-identical",
                Double.doubleToRawLongBits(value), Double.doubleToRawLongBits(fromWkb.x()));
        assertEquals("WKB y of " + value + " was not bit-identical",
                Double.doubleToRawLongBits(value), Double.doubleToRawLongBits(fromWkb.y()));
    }

    // Values whose shortest form Double.toString and JTS miss
    @Test
    public void testShortestDecimal() {
        // 2.82879384806159E17 -> 282879384806159000
        assertEquals("POINT(282879384806159000 0)",
            Geometry.fromWkt("POINT(2.82879384806159E17 0)").toWKT());

        // 1E23 -> 100000000000000000000000
        assertEquals("POINT(100000000000000000000000 0)",
            Geometry.fromWkt("POINT(1E23 0)").toWKT());

        // 1E-7 -> 0.0000001
        assertEquals("POINT(0.0000001 0)",
            Geometry.fromWkt("POINT(1E-7 0)").toWKT());

        // 5E-324, the smallest subnormal, written out in full
        final StringBuilder minValue = new StringBuilder("0.");
        for (int i = 0; i < 323; i++) {
            minValue.append('0');
        }
        minValue.append('5');
        assertEquals("POINT(" + minValue + " 0)",
            Geometry.fromWkt("POINT(5E-324 0)").toWKT());

        // The values still round-trip through parsing
        final Geometry g1 = Geometry.fromWkt("POINT(2.82879384806159E17 0)");
        assertEquals(g1, Geometry.fromWkt(g1.toWKT()));

        final Geometry g2 = Geometry.fromWkt("POINT(1E23 0)");
        assertEquals(g2, Geometry.fromWkt(g2.toWKT()));

        final Geometry g3 = Geometry.fromWkt("POINT(5E-324 0)");
        assertEquals(g3, Geometry.fromWkt(g3.toWKT()));
    }

    // The written form is the shortest digit string that reads back to the same double
    @Test
    public void testOutputIsShortest() {
        assertShortestPlainForm(2.82879384806159E17);
        assertShortestPlainForm(1E23);
        assertShortestPlainForm(1E-7);
        assertShortestPlainForm(5E-324);
        assertShortestPlainForm(Double.MIN_VALUE);
        assertShortestPlainForm(Double.MAX_VALUE);
        assertShortestPlainForm(-74.0);
        assertShortestPlainForm(40.7);
        assertShortestPlainForm(0.1);
        assertShortestPlainForm(2000.0);
        assertShortestPlainForm(123456789.123456789);
    }

    /**
     * Asserts that the x ordinate written for the value is in plain notation, reads back to the
     * value, and that no shorter run of significant digits reads back to it. The shorter run is
     * measured against the exact binary expansion of the value, independently of the writer:
     * the only candidates of a given length that can read back are that expansion rounded down
     * and rounded up to the length.
     */
    private void assertShortestPlainForm(final double value) {
        final String wkt = Point.of(value, 0).toWKT();
        final String written = wkt.substring("POINT(".length(), wkt.indexOf(' '));

        assertFalse("Exponent notation in " + written, written.contains("E") || written.contains("e"));
        assertEquals(value, Double.parseDouble(written), 0.0);

        final int significantDigits = new BigDecimal(written).stripTrailingZeros().precision();
        if (significantDigits == 1) {
            return; // nothing is shorter than one significant digit
        }

        final BigDecimal exact = new BigDecimal(value);
        for (final RoundingMode mode : new RoundingMode[]{RoundingMode.FLOOR, RoundingMode.CEILING}) {
            final BigDecimal shorter = exact.round(new MathContext(significantDigits - 1, mode));
            assertNotEquals("The " + (significantDigits - 1) + "-digit " + mode + " form "
                            + shorter.toPlainString() + " of " + written + " also reads back to the value",
                    value, shorter.doubleValue(), 0.0);
        }
    }

    // fromWkt("SRID=4326;POINT(1 2)") -> GEOGRAPHIC
    @Test
    public void testEwktGeographic() {
        final Geometry g = Geometry.fromWkt("SRID=4326;POINT(1 2)");
        assertEquals(Model.GEOGRAPHIC, g.model());
        assertEquals(4326, g.srid());
    }

    // fromWkt with SRID 32618 -> OTHER
    @Test
    public void testEwktOther() {
        final Geometry g1 = Geometry.fromWkt("SRID=32618;POINT(1 2)");
        assertEquals(Model.OTHER, g1.model());
        assertEquals(32618, g1.srid());

        final Geometry g2 = Geometry.fromWkt("POINT(1 2)", 32618);
        assertEquals(Model.OTHER, g2.model());
        assertEquals(32618, g2.srid());
    }

    // fromWkt(wkt, GEOGRAPHIC) and fromWkt(wkt)
    @Test
    public void testModelArgument() {
        final Geometry g1 = Geometry.fromWkt("POINT(1 2)", Model.GEOGRAPHIC);
        assertEquals(4326, g1.srid());

        final Geometry g2 = Geometry.fromWkt("POINT(1 2)");
        assertEquals(0, g2.srid());
    }

    // EWKT prefix with conflicting model or SRID argument -> IAE
    @Test
    public void testConflictingEwkt() {
        // Conflicting SRID
        try {
            Geometry.fromWkt("SRID=4326;POINT(1 2)", 0);
            fail("Expected IAE for conflicting SRID");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // Conflicting model
        try {
            Geometry.fromWkt("SRID=4326;POINT(1 2)", Model.CARTESIAN);
            fail("Expected IAE for conflicting model");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    // toEWKT() for GEOGRAPHIC value
    @Test
    public void testToEwktGeographic() {
        final Point p = Point.of(1, 2, Model.GEOGRAPHIC);
        assertEquals("SRID=4326;POINT(1 2)", p.toEWKT());
    }

    // toEWKT() for CARTESIAN value - includes SRID=0
    @Test
    public void testToEwktCartesian() {
        final Point p = Point.of(1, 2);
        assertEquals("SRID=0;POINT(1 2)", p.toEWKT());
    }

    // Syntax errors
    @Test
    public void testSyntaxErrors() {
        // Missing parenthesis
        try {
            Geometry.fromWkt("POINT(1 2");
            fail("Expected IAE for missing parenthesis");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // Trailing text
        try {
            Geometry.fromWkt("POINT(1 2) extra");
            fail("Expected IAE for trailing text");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // Unknown keyword
        try {
            Geometry.fromWkt("UNKNOWN(1 2)");
            fail("Expected IAE for unknown keyword");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    // Every geometry type the reader names as unsupported
    @Test
    public void testUnsupportedTypes() {
        final String[] unsupported = {
            "CIRCULARSTRING(0 0, 1 1, 2 0)",
            "COMPOUNDCURVE((0 0, 1 1), CIRCULARSTRING(1 1, 2 2, 3 1))",
            "CURVEPOLYGON((0 0, 1 0, 1 1, 0 0))",
            "MULTICURVE((0 0, 1 1), CIRCULARSTRING(1 1, 2 2, 3 1))",
            "MULTISURFACE(((0 0, 1 0, 1 1, 0 0)))",
            "TRIANGLE((0 0, 1 0, 0 1, 0 0))",
            "TIN(((0 0, 1 0, 0 1, 0 0)))",
            "POLYHEDRALSURFACE(((0 0, 1 0, 1 1, 0 0)))",
            // the keyword is reported whatever follows it
            "CIRCULARSTRING EMPTY",
            "COMPOUNDCURVE EMPTY",
            "CURVEPOLYGON EMPTY",
            "MULTICURVE EMPTY",
            "MULTISURFACE EMPTY",
            "TRIANGLE EMPTY",
            "TIN EMPTY",
            "POLYHEDRALSURFACE EMPTY",
            // and in lower case, and nested in a collection
            "triangle((0 0, 1 0, 0 1, 0 0))",
            "GEOMETRYCOLLECTION(TIN(((0 0, 1 0, 0 1, 0 0))))"
        };
        for (final String wkt : unsupported) {
            try {
                Geometry.fromWkt(wkt);
                fail("Expected IAE for unsupported type: " + wkt);
            } catch (IllegalArgumentException e) {
                assertTrue("Message should report the type as unsupported, was: " + e.getMessage(),
                        e.getMessage().contains("Unsupported geometry type"));
            }
        }
    }

    // M and ZM not supported
    @Test
    public void testMNotSupported() {
        try {
            Geometry.fromWkt("POINT M (1 2 3)");
            fail("Expected IAE for M dimension");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            Geometry.fromWkt("POINT ZM (1 2 3 4)");
            fail("Expected IAE for ZM dimension");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    // Wrong ordinate count
    @Test
    public void testWrongOrdinateCount() {
        // POINT with 1 ordinate
        try {
            Geometry.fromWkt("POINT(1)");
            fail("Expected IAE for POINT with 1 ordinate");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // POINT Z with 2 ordinates
        try {
            Geometry.fromWkt("POINT Z (1 2)");
            fail("Expected IAE for POINT Z with 2 ordinates");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // POINT with 3 ordinates but no Z tag
        try {
            Geometry.fromWkt("POINT(1 2 3)");
            fail("Expected IAE for POINT with 3 ordinates but no Z");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // Collection mixing Z tags
        try {
            Geometry.fromWkt("GEOMETRYCOLLECTION(POINT(1 2), LINESTRING Z (0 0 0, 1 1 1))");
            fail("Expected IAE for mixed Z tags");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    // NaN and Infinity not allowed
    @Test
    public void testNaNInfinityRejected() {
        try {
            Geometry.fromWkt("POINT(NaN 0)");
            fail("Expected IAE for NaN");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            Geometry.fromWkt("POINT(Infinity 0)");
            fail("Expected IAE for Infinity");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            Geometry.fromWkt("POINT(-Infinity 0)");
            fail("Expected IAE for -Infinity");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    // Nesting depth limits
    @Test
    public void testNestingDepthWkt() {
        // Build 32 levels - should succeed
        StringBuilder wkt = new StringBuilder("POINT(0 0)");
        for (int i = 0; i < 32; i++) {
            wkt = new StringBuilder("GEOMETRYCOLLECTION(" + wkt + ")");
        }
        Geometry g = Geometry.fromWkt(wkt.toString());
        assertTrue(g instanceof GeometryCollection);

        // 33 levels - should fail
        wkt = new StringBuilder("POINT(0 0)");
        for (int i = 0; i < 33; i++) {
            wkt = new StringBuilder("GEOMETRYCOLLECTION(" + wkt + ")");
        }
        try {
            Geometry.fromWkt(wkt.toString());
            fail("Expected IAE for 33-level nesting");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    // Exponents accepted on input
    @Test
    public void testExponentsAccepted() {
        final Geometry g1 = Geometry.fromWkt("POINT(1e-7 2E3)");
        assertEquals(0.0000001, ((Point) g1).x(), 1e-15);
        assertEquals(2000.0, ((Point) g1).y(), 0.0);
        assertEquals("POINT(0.0000001 2000)", g1.toWKT());

        final Geometry g2 = Geometry.fromWkt("POINT(-1.5E+2 0)");
        assertEquals(-150.0, ((Point) g2).x(), 0.0);
        assertEquals("POINT(-150 0)", g2.toWKT());
    }

    private void assertRoundTrip(final String wkt) {
        final Geometry g = Geometry.fromWkt(wkt);
        assertEquals(wkt, g.toWKT());
    }

    // A collection keeps its empty parts, in WKT and in WKB
    @Test
    public void testEmptyPartsKeepStructure() {
        assertStructureRoundTrip("MULTIPOINT(EMPTY, EMPTY)");
        assertStructureRoundTrip("MULTIPOINT((0 0), EMPTY)");
        assertStructureRoundTrip("MULTIPOINT(EMPTY, (1 1))");
        assertStructureRoundTrip("MULTIPOINT(EMPTY)");
        assertStructureRoundTrip("MULTIPOINT(EMPTY, (1 1), EMPTY, (2 2))");
        assertStructureRoundTrip("MULTIPOINT Z (EMPTY, (1 1 1))");
        assertStructureRoundTrip("GEOMETRYCOLLECTION(POINT EMPTY)");
        assertStructureRoundTrip("GEOMETRYCOLLECTION Z (POINT Z EMPTY)");
        assertStructureRoundTrip("GEOMETRYCOLLECTION(POINT EMPTY, POINT(1 2))");
        assertStructureRoundTrip("MULTIPOLYGON(EMPTY)");
        assertStructureRoundTrip("MULTIPOLYGON(EMPTY, ((0 0, 1 0, 1 1, 0 0)))");
        assertStructureRoundTrip("MULTILINESTRING(EMPTY, EMPTY)");
        assertStructureRoundTrip("MULTILINESTRING(EMPTY, (0 0, 1 1))");
        assertStructureRoundTrip("MULTILINESTRING Z (EMPTY, (0 0 0, 1 1 1))");

        // A nested collection of the same
        assertStructureRoundTrip("GEOMETRYCOLLECTION(MULTIPOINT(EMPTY, EMPTY), "
                + "MULTIPOLYGON(EMPTY), MULTILINESTRING(EMPTY, EMPTY), "
                + "GEOMETRYCOLLECTION(POINT EMPTY), POINT(1 2))");
        assertStructureRoundTrip("GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(MULTIPOINT(EMPTY)), "
                + "MULTIPOINT((0 0), EMPTY))");
    }

    // A collection whose parts are all empty is still not written as EMPTY
    @Test
    public void testDeepEmptyIsNotStructurallyEmpty() {
        final Geometry allPartsEmpty = Geometry.fromWkt("MULTIPOINT(EMPTY, EMPTY)");
        assertTrue(allPartsEmpty.isEmpty());
        assertEquals("MULTIPOINT(EMPTY, EMPTY)", allPartsEmpty.toWKT());
        assertNotEquals(Geometry.fromWkt("MULTIPOINT EMPTY"), allPartsEmpty);

        final Geometry gc = Geometry.fromWkt("GEOMETRYCOLLECTION(POINT EMPTY)");
        assertTrue(gc.isEmpty());
        assertEquals("GEOMETRYCOLLECTION(POINT EMPTY)", gc.toWKT());
        assertNotEquals(Geometry.fromWkt("GEOMETRYCOLLECTION EMPTY"), gc);
    }

    // A Z tag on a nested empty part is written with a single space
    @Test
    public void testNestedEmptyZSpacing() {
        assertEquals("GEOMETRYCOLLECTION Z (POINT Z EMPTY)",
                Geometry.fromWkt("GEOMETRYCOLLECTION Z (POINT Z EMPTY)").toWKT());
        assertEquals("GEOMETRYCOLLECTION Z (LINESTRING Z EMPTY, POLYGON Z EMPTY, POINT Z (0 0 0))",
                Geometry.fromWkt("GEOMETRYCOLLECTION Z (LINESTRING Z EMPTY, POLYGON Z EMPTY, POINT Z (0 0 0))").toWKT());
        assertEquals("GEOMETRYCOLLECTION Z (MULTIPOINT Z EMPTY)",
                Geometry.fromWkt("GEOMETRYCOLLECTION Z (MULTIPOINT Z EMPTY)").toWKT());
    }

    // An empty point cannot be a LineString vertex or a ring vertex in WKT
    @Test
    public void testEmptyVertexRejected() {
        assertIae(() -> Geometry.fromWkt("LINESTRING(EMPTY, EMPTY)"));
        assertIae(() -> Geometry.fromWkt("LINESTRING(0 0, EMPTY)"));
        assertIae(() -> Geometry.fromWkt("POLYGON((EMPTY))"));
        assertIae(() -> Geometry.fromWkt("POLYGON((0 0, 1 0, 1 1, 0 0), EMPTY)"));
        assertIae(() -> Geometry.fromWkt("MULTILINESTRING((0 0, EMPTY))"));
    }

    // ZM is reported as unsupported, including without a space
    @Test
    public void testZmReported() {
        for (final String wkt : new String[]{"POINT ZM (0 0)", "POINT ZM (1 2 3 4)",
                "POINT Z M (1 2 3 4)", "LINESTRING ZM (0 0 0 0, 1 1 1 1)",
                "GEOMETRYCOLLECTION ZM (POINT ZM (1 2 3 4))"}) {
            try {
                Geometry.fromWkt(wkt);
                fail("Expected IAE for " + wkt);
            } catch (IllegalArgumentException e) {
                assertTrue("Message should name ZM, was: " + e.getMessage(),
                        e.getMessage().contains("ZM"));
            }
        }
    }

    // A very deep nesting attempt fails with IAE rather than a StackOverflowError
    @Test
    public void testHugeNestingWkt() {
        final int levels = 100000;
        final StringBuilder wkt = new StringBuilder(levels * 20);
        for (int i = 0; i < levels; i++) {
            wkt.append("GEOMETRYCOLLECTION(");
        }
        wkt.append("POINT(0 0)");
        for (int i = 0; i < levels; i++) {
            wkt.append(')');
        }
        assertIae(() -> Geometry.fromWkt(wkt.toString()));
    }

    // An exception message never echoes unbounded input
    @Test
    public void testExceptionMessagesAreBounded() {
        final StringBuilder tail = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            tail.append('x');
        }

        try {
            Geometry.fromWkt("POINT(1 2) " + tail);
            fail("Expected IAE for trailing text");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("trailing text"));
            assertTrue("Message echoed too much input: " + e.getMessage().length(),
                    e.getMessage().length() <= 100);
            assertTrue(e.getMessage().endsWith("..."));
        }

        final StringBuilder digits = new StringBuilder();
        for (int i = 0; i < 300; i++) {
            digits.append('1');
        }
        try {
            Geometry.fromWkt("POINT(" + digits + "e 0)");
            fail("Expected IAE for an invalid number");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Invalid number"));
            assertTrue("Message echoed too much input: " + e.getMessage().length(),
                    e.getMessage().length() <= 100);
            assertTrue(e.getMessage().endsWith("..."));
        }
    }

    // The EWKT SRID digit run is bounded, and a sign on its own is a clean IAE
    @Test
    public void testEwktSridIsBounded() {
        // Sign with no digits
        try {
            Geometry.fromWkt("SRID=-;POINT(0 0)");
            fail("Expected IAE for a sign-only SRID");
        } catch (IllegalArgumentException e) {
            assertFalse("Should not surface a NumberFormatException: " + e.getMessage(),
                    e instanceof NumberFormatException);
            assertFalse(e.getMessage().contains("POINT(0 0)"));
        }

        // More than 10 digits
        try {
            Geometry.fromWkt("SRID=123456789012345678901234567890;POINT(0 0)");
            fail("Expected IAE for an over-long SRID");
        } catch (IllegalArgumentException e) {
            assertFalse(e instanceof NumberFormatException);
            assertTrue(e.getMessage().contains("10 digits"));
        }

        // Ten digits that do not fit in an int
        try {
            Geometry.fromWkt("SRID=9999999999;POINT(0 0)");
            fail("Expected IAE for an out-of-range SRID");
        } catch (IllegalArgumentException e) {
            assertFalse(e instanceof NumberFormatException);
            assertTrue(e.getMessage().contains("out of range"));
        }

        // A negative SRID is still read
        assertEquals(-1, Geometry.fromWkt("SRID=-1;POINT(0 0)").srid());
    }

    // A null Model argument is rejected rather than silently treated as CARTESIAN
    @Test
    public void testNullModelRejected() {
        assertIae(() -> Geometry.fromWkt("POINT(0 0)", (Model) null));
    }

    /**
     * Asserts that the text is written back unchanged, and that the value's WKT and WKB
     * describe the same structure.
     */
    private void assertStructureRoundTrip(final String wkt) {
        final Geometry g = Geometry.fromWkt(wkt);
        assertEquals(wkt, g.toWKT());
        assertEquals(g, Geometry.fromWkt(g.toWKT()));
        assertEquals(g, Geometry.fromWkb(g.toWKB(), g.srid()));
        assertEquals(Geometry.fromWkt(g.toWKT()), Geometry.fromWkb(g.toWKB(), g.srid()));
    }

    private static void assertIae(final Runnable action) {
        try {
            action.run();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
}

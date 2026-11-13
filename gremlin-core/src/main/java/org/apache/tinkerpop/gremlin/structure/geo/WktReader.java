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

import java.util.ArrayList;
import java.util.List;

/**
 * Package-private WKT/EWKT parser.
 */
final class WktReader {

    /**
     * Longest run of input echoed back in an exception message.
     */
    private static final int MAX_ECHOED_INPUT = 64;

    /**
     * Longest run of SRID digits accepted after an EWKT "SRID=" prefix.
     */
    private static final int MAX_SRID_DIGITS = 10;

    /**
     * Message for a coordinate that carries a third ordinate in text read under ISO rules.
     */
    private static final String MSG_UNTAGGED_THIRD_ORDINATE = "Coordinate has 3 ordinates but no Z tag";

    /**
     * Type names carrying a ZM dimension written against the name, the form PostGIS writes.
     */
    private static final String[] ZM_SUFFIXED_TYPES = {
        "POINTZM", "LINESTRINGZM", "POLYGONZM", "MULTIPOINTZM", "MULTILINESTRINGZM",
        "MULTIPOLYGONZM", "GEOMETRYCOLLECTIONZM"
    };

    /**
     * Type names carrying an M dimension written against the name, the form PostGIS writes.
     */
    private static final String[] M_SUFFIXED_TYPES = {
        "POINTM", "LINESTRINGM", "POLYGONM", "MULTIPOINTM", "MULTILINESTRINGM", "MULTIPOLYGONM",
        "GEOMETRYCOLLECTIONM"
    };

    private final String input;

    /**
     * True when a part that carries no Z tag is read as 3D, the form PostGIS EWKT writes.
     */
    private final boolean readUntaggedAsZ;

    /**
     * True when the text holds no Z tag at all, so it can be read again as 3D.
     */
    private final boolean zTagAbsent;

    private int pos;
    private int depth; // collection nesting depth
    private int srid;
    private Boolean hasZ; // null = not yet determined

    private WktReader(final String input, final boolean readUntaggedAsZ) {
        this.input = input;
        this.readUntaggedAsZ = readUntaggedAsZ;
        this.zTagAbsent = input.indexOf('Z') < 0 && input.indexOf('z') < 0;
        this.pos = 0;
        this.depth = 0;
    }

    /**
     * Parses WKT or EWKT text. ISO rules are applied first, and text that carries no Z tag at
     * all is read again as 3D when a coordinate holds a third ordinate, which is how PostGIS
     * EWKT writes a 3D value.
     *
     * @param wktOrEwkt the input text
     * @param model explicit model, or null to derive from EWKT prefix
     * @param srid explicit SRID, or null to derive from EWKT prefix
     * @return the parsed geometry
     */
    static Geometry parse(final String wktOrEwkt, final Model model, final Integer srid) {
        try {
            return parse(wktOrEwkt, model, srid, false);
        } catch (UntaggedThirdOrdinate e) {
            // the second pass reads every untagged part as 3D, so the third-ordinate signal
            // cannot be raised again and every exception this call raises is a plain
            // IllegalArgumentException
            return parse(wktOrEwkt, model, srid, true);
        }
    }

    private static Geometry parse(final String wktOrEwkt, final Model model, final Integer srid,
                                  final boolean readUntaggedAsZ) {
        if (wktOrEwkt == null || wktOrEwkt.trim().isEmpty()) {
            throw new IllegalArgumentException("WKT input cannot be null or empty");
        }

        final WktReader reader = new WktReader(wktOrEwkt, readUntaggedAsZ);
        reader.skipWhitespace();

        // Check for EWKT prefix
        Integer ewktSrid = null;
        if (reader.matchExact("SRID=")) {
            ewktSrid = reader.parseSrid();
            reader.expect(';');
            reader.skipWhitespace();
        }

        // Determine final SRID
        if (ewktSrid != null && srid != null && !ewktSrid.equals(srid)) {
            throw new IllegalArgumentException("EWKT SRID " + ewktSrid + " conflicts with argument SRID " + srid);
        }
        if (ewktSrid != null && model != null) {
            final int modelSrid = CoordinateValidator.sridForModel(model);
            if (!ewktSrid.equals(modelSrid)) {
                throw new IllegalArgumentException("EWKT SRID " + ewktSrid + " conflicts with model " + model);
            }
        }

        final int finalSrid;
        if (srid != null) {
            finalSrid = srid;
        } else if (ewktSrid != null) {
            finalSrid = ewktSrid;
        } else if (model != null) {
            finalSrid = CoordinateValidator.sridForModel(model);
        } else {
            finalSrid = CoordinateValidator.SRID_CARTESIAN;
        }

        reader.srid = finalSrid;

        // Parse geometry
        final Geometry g = reader.parseGeometry();

        // Check for trailing text
        reader.skipWhitespace();
        if (reader.pos < reader.input.length()) {
            throw new IllegalArgumentException("Unexpected trailing text: " + truncate(reader.input.substring(reader.pos)));
        }

        return g;
    }

    private Geometry parseGeometry() {
        skipWhitespace();
        rejectSuffixedDimension();

        if (matchIgnoreCase("POINT")) {
            return parsePoint();
        } else if (matchIgnoreCase("LINESTRING")) {
            return parseLineString();
        } else if (matchIgnoreCase("POLYGON")) {
            return parsePolygon();
        } else if (matchIgnoreCase("MULTIPOINT")) {
            return parseMultiPoint();
        } else if (matchIgnoreCase("MULTILINESTRING")) {
            return parseMultiLineString();
        } else if (matchIgnoreCase("MULTIPOLYGON")) {
            return parseMultiPolygon();
        } else if (matchIgnoreCase("GEOMETRYCOLLECTION")) {
            return parseGeometryCollection();
        } else {
            // Check for unsupported types
            if (matchIgnoreCase("CIRCULARSTRING") || matchIgnoreCase("COMPOUNDCURVE") ||
                matchIgnoreCase("CURVEPOLYGON") || matchIgnoreCase("MULTICURVE") ||
                matchIgnoreCase("MULTISURFACE") || matchIgnoreCase("TRIANGLE") ||
                matchIgnoreCase("TIN") || matchIgnoreCase("POLYHEDRALSURFACE")) {
                throw new IllegalArgumentException("Unsupported geometry type");
            }
            throw new IllegalArgumentException("Unknown geometry type at position " + pos);
        }
    }

    private Point parsePoint() {
        final boolean z = parseZTag();
        skipWhitespace();

        if (matchIgnoreCase("EMPTY")) {
            recordZTag(z);
            return Point.emptyWithZ(srid, z);
        }

        expect('(');
        final Point p = parseCoordinate(z);
        expect(')');

        recordZTag(z);
        return p;
    }

    /**
     * Parses one coordinate, "x y" or "x y z", into a point.
     */
    private Point parseCoordinate(final boolean z) {
        skipWhitespace();
        final double x = parseNumber();
        skipWhitespace();
        final double y = parseNumber();
        skipWhitespace();

        if (z) {
            if (readUntaggedAsZ && !startsNumber(peek())) {
                throw new IllegalArgumentException("Value mixes 2D and 3D coordinates");
            }
            final double zVal = parseNumber();
            skipWhitespace();
            rejectExtraOrdinates();
            return Point.ofWithSrid(x, y, zVal, srid);
        }

        if (startsNumber(peek())) {
            parseNumber();
            skipWhitespace();
            rejectExtraOrdinates();
            if (zTagAbsent) {
                if (readUntaggedAsZ) {
                    // every untagged part reads as 3D here, so this branch is unreachable
                    throw new IllegalStateException("Untagged third ordinate signalled while reading untagged text as 3D");
                }
                // PostGIS EWKT writes a 3D value with no Z tag, so read the whole value again as 3D
                throw new UntaggedThirdOrdinate();
            }
            throw new IllegalArgumentException(MSG_UNTAGGED_THIRD_ORDINATE);
        }
        return Point.ofWithSrid(x, y, srid);
    }

    /**
     * Rejects a coordinate that carries a further ordinate at a position where three ordinates
     * have been read, the XYZM form.
     */
    private void rejectExtraOrdinates() {
        if (startsNumber(peek())) {
            throw new IllegalArgumentException("Coordinate has more than 3 ordinates but the M dimension is not supported");
        }
    }

    /**
     * Rejects an M or ZM dimension written against the type name, as in "POINTM(0 0 0)" or
     * "POINTZM(0 0 0 0)".
     */
    private void rejectSuffixedDimension() {
        for (final String type : ZM_SUFFIXED_TYPES) {
            if (peekWord(type)) {
                throw new IllegalArgumentException("ZM dimension not supported");
            }
        }
        for (final String type : M_SUFFIXED_TYPES) {
            if (peekWord(type)) {
                throw new IllegalArgumentException("M dimension not supported");
            }
        }
    }

    /**
     * Returns true if the character can start a number.
     */
    private static boolean startsNumber(final char c) {
        return Character.isDigit(c) || c == '-' || c == '+' || c == '.';
    }

    /**
     * Records the Z tag of a geometry, which must agree across one value.
     */
    private void recordZTag(final boolean z) {
        if (hasZ == null) {
            hasZ = z;
        } else if (hasZ != z) {
            throw new IllegalArgumentException("Mixed Z tags in collection");
        }
    }

    private LineString parseLineString() {
        final boolean z = parseZTag();
        skipWhitespace();

        if (matchIgnoreCase("EMPTY")) {
            recordZTag(z);
            return LineString.emptyWithSrid(srid, z);
        }

        expect('(');
        final List<Point> points = parseCoordinateList(z);
        expect(')');

        recordZTag(z);

        return LineString.ofWithSrid(points, srid, z);
    }

    private Polygon parsePolygon() {
        final boolean z = parseZTag();
        skipWhitespace();

        if (matchIgnoreCase("EMPTY")) {
            recordZTag(z);
            return Polygon.emptyWithSrid(srid, z);
        }

        expect('(');
        final List<LineString> rings = parseRingList(z);
        expect(')');

        recordZTag(z);

        return polygonFromRings(rings, z);
    }

    /**
     * Parses the parenthesized ring list of a polygon body, after the opening parenthesis.
     */
    private List<LineString> parseRingList(final boolean z) {
        final List<LineString> rings = new ArrayList<>();
        skipWhitespace();
        while (true) {
            expect('(');
            rings.add(LineString.ofWithSrid(parseCoordinateList(z), srid, z));
            expect(')');
            skipWhitespace();
            if (peek() != ',') {
                return rings;
            }
            advance();
            skipWhitespace();
        }
    }

    /**
     * Builds a polygon from a parsed ring list, the first ring being the shell.
     */
    private Polygon polygonFromRings(final List<LineString> rings, final boolean z) {
        final LineString shell = rings.get(0);
        final List<LineString> holes = new ArrayList<>(rings.subList(1, rings.size()));
        return Polygon.ofWithSrid(shell, holes, srid, z);
    }

    private MultiPoint parseMultiPoint() {
        depth++;
        CoordinateValidator.validateNestingDepth(depth);

        final boolean z = parseZTag();
        skipWhitespace();

        if (matchIgnoreCase("EMPTY")) {
            depth--;
            recordZTag(z);
            return MultiPoint.emptyWithSrid(srid, z);
        }

        expect('(');
        final List<Point> points = new ArrayList<>();

        skipWhitespace();
        while (true) {
            // Accept EMPTY, the parenthesized form "(0 0)" and the bare form "0 0" at any position
            if (matchIgnoreCase("EMPTY")) {
                points.add(Point.emptyWithZ(srid, z));
            } else if (peek() == '(') {
                advance();
                points.add(parseCoordinate(z));
                expect(')');
            } else {
                points.add(parseCoordinate(z));
            }
            skipWhitespace();
            if (peek() != ',') {
                break;
            }
            advance();
            skipWhitespace();
        }

        expect(')');

        depth--;
        recordZTag(z);

        return MultiPoint.ofWithSrid(points, srid, z);
    }

    private MultiLineString parseMultiLineString() {
        depth++;
        CoordinateValidator.validateNestingDepth(depth);

        final boolean z = parseZTag();
        skipWhitespace();

        if (matchIgnoreCase("EMPTY")) {
            depth--;
            recordZTag(z);
            return MultiLineString.emptyWithSrid(srid, z);
        }

        expect('(');
        final List<LineString> lineStrings = new ArrayList<>();

        skipWhitespace();
        while (true) {
            if (matchIgnoreCase("EMPTY")) {
                lineStrings.add(LineString.emptyWithSrid(srid, z));
            } else {
                expect('(');
                lineStrings.add(LineString.ofWithSrid(parseCoordinateList(z), srid, z));
                expect(')');
            }
            skipWhitespace();
            if (peek() != ',') {
                break;
            }
            advance();
            skipWhitespace();
        }

        expect(')');

        depth--;
        recordZTag(z);

        return MultiLineString.ofWithSrid(lineStrings, srid, z);
    }

    private MultiPolygon parseMultiPolygon() {
        depth++;
        CoordinateValidator.validateNestingDepth(depth);

        final boolean z = parseZTag();
        skipWhitespace();

        if (matchIgnoreCase("EMPTY")) {
            depth--;
            recordZTag(z);
            return MultiPolygon.emptyWithSrid(srid, z);
        }

        expect('(');
        final List<Polygon> polygons = new ArrayList<>();

        skipWhitespace();
        while (true) {
            if (matchIgnoreCase("EMPTY")) {
                polygons.add(Polygon.emptyWithSrid(srid, z));
            } else {
                expect('(');
                polygons.add(polygonFromRings(parseRingList(z), z));
                expect(')');
            }
            skipWhitespace();
            if (peek() != ',') {
                break;
            }
            advance();
            skipWhitespace();
        }

        expect(')');

        depth--;
        recordZTag(z);

        return MultiPolygon.ofWithSrid(polygons, srid, z);
    }

    private GeometryCollection parseGeometryCollection() {
        depth++;
        CoordinateValidator.validateNestingDepth(depth);

        final boolean z = parseZTag();
        skipWhitespace();

        if (matchIgnoreCase("EMPTY")) {
            depth--;
            recordZTag(z);
            return GeometryCollection.emptyWithSrid(srid, z);
        }

        expect('(');
        final List<Geometry> geometries = new ArrayList<>();

        skipWhitespace();
        while (true) {
            geometries.add(parseGeometry());
            skipWhitespace();
            if (peek() != ',') {
                break;
            }
            advance();
            skipWhitespace();
        }

        expect(')');

        depth--;

        // Every part must carry the same Z tag as the collection
        for (final Geometry g : geometries) {
            if (g.hasZ() != z) {
                throw new IllegalArgumentException("Mixed Z tags in collection");
            }
        }

        recordZTag(z);

        return GeometryCollection.createWithSrid(geometries, srid, z);
    }

    private List<Point> parseCoordinateList(final boolean hasZ) {
        final List<Point> points = new ArrayList<>();
        skipWhitespace();

        while (true) {
            points.add(parseCoordinate(hasZ));
            skipWhitespace();
            if (peek() != ',') {
                return points;
            }
            advance();
            skipWhitespace();
        }
    }

    private boolean parseZTag() {
        skipWhitespace();
        if (matchIgnoreCase("ZM")) {
            throw new IllegalArgumentException("ZM dimension not supported");
        }
        if (matchIgnoreCase("Z")) {
            skipWhitespace();
            // A separated M tag means ZM
            if (matchIgnoreCase("M")) {
                throw new IllegalArgumentException("ZM dimension not supported");
            }
            return true;
        }
        if (matchIgnoreCase("M")) {
            throw new IllegalArgumentException("M dimension not supported");
        }
        return readUntaggedAsZ;
    }

    private double parseNumber() {
        skipWhitespace();
        final int start = pos;

        // Check for NaN or Infinity tokens
        if (matchIgnoreCase("NaN")) {
            throw new IllegalArgumentException("NaN is not allowed in WKT");
        }
        if (matchIgnoreCase("Infinity") || matchIgnoreCase("-Infinity") || matchIgnoreCase("+Infinity")) {
            throw new IllegalArgumentException("Infinity is not allowed in WKT");
        }

        // Parse sign
        if (pos < input.length() && (input.charAt(pos) == '-' || input.charAt(pos) == '+')) {
            pos++;
        }

        // Parse integer part
        while (pos < input.length() && Character.isDigit(input.charAt(pos))) {
            pos++;
        }

        // Parse decimal part
        if (pos < input.length() && input.charAt(pos) == '.') {
            pos++;
            while (pos < input.length() && Character.isDigit(input.charAt(pos))) {
                pos++;
            }
        }

        // Parse exponent (accept on input)
        if (pos < input.length() && (input.charAt(pos) == 'e' || input.charAt(pos) == 'E')) {
            pos++;
            if (pos < input.length() && (input.charAt(pos) == '-' || input.charAt(pos) == '+')) {
                pos++;
            }
            while (pos < input.length() && Character.isDigit(input.charAt(pos))) {
                pos++;
            }
        }

        if (start == pos) {
            throw new IllegalArgumentException("Expected number at position " + pos);
        }

        final String numStr = input.substring(start, pos);
        try {
            return Double.parseDouble(numStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid number: " + truncate(numStr));
        }
    }

    /**
     * Parses the SRID of an EWKT prefix. The digit run is bounded, so an arbitrarily long
     * run of digits is rejected rather than echoed or parsed.
     */
    private int parseSrid() {
        skipWhitespace();
        final int start = pos;

        if (pos < input.length() && (input.charAt(pos) == '-' || input.charAt(pos) == '+')) {
            pos++;
        }

        final int digitsStart = pos;
        while (pos < input.length() && Character.isDigit(input.charAt(pos)) && pos - digitsStart < MAX_SRID_DIGITS) {
            pos++;
        }

        if (pos == digitsStart) {
            throw new IllegalArgumentException("Expected SRID digits at position " + digitsStart);
        }
        if (pos < input.length() && Character.isDigit(input.charAt(pos))) {
            throw new IllegalArgumentException("SRID has more than " + MAX_SRID_DIGITS + " digits");
        }

        try {
            return Integer.parseInt(input.substring(start, pos));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("SRID is out of range for a 32-bit integer");
        }
    }

    /**
     * Shortens a run of input for use in an exception message.
     */
    private static String truncate(final String s) {
        if (s.length() <= MAX_ECHOED_INPUT) {
            return s;
        }
        return s.substring(0, MAX_ECHOED_INPUT) + "...";
    }

    private void expect(final char c) {
        skipWhitespace();
        if (pos >= input.length() || input.charAt(pos) != c) {
            throw new IllegalArgumentException("Expected '" + c + "' at position " + pos);
        }
        pos++;
    }

    private void skipWhitespace() {
        while (pos < input.length() && Character.isWhitespace(input.charAt(pos))) {
            pos++;
        }
    }

    private boolean matchIgnoreCase(final String s) {
        if (!peekWord(s)) {
            return false;
        }
        pos += s.length();
        return true;
    }

    /**
     * Returns true if the word is at the current position, case-insensitively and bounded by a
     * non-word character, without consuming it.
     */
    private boolean peekWord(final String s) {
        if (pos + s.length() > input.length()) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            if (Character.toUpperCase(input.charAt(pos + i)) != Character.toUpperCase(s.charAt(i))) {
                return false;
            }
        }
        // Make sure it's not a prefix of a longer word
        if (pos + s.length() < input.length()) {
            final char next = input.charAt(pos + s.length());
            if (Character.isLetterOrDigit(next) || next == '_') {
                return false;
            }
        }
        return true;
    }

    /**
     * Matches a prefix exactly (case-insensitive), without word boundary checking.
     */
    private boolean matchExact(final String s) {
        if (pos + s.length() > input.length()) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            if (Character.toUpperCase(input.charAt(pos + i)) != Character.toUpperCase(s.charAt(i))) {
                return false;
            }
        }
        pos += s.length();
        return true;
    }

    private char peek() {
        if (pos >= input.length()) {
            return '\0';
        }
        return input.charAt(pos);
    }

    private void advance() {
        if (pos < input.length()) {
            pos++;
        }
    }

    /**
     * Signals that an untagged coordinate carries a third ordinate in text that holds no Z tag,
     * so the whole value can be read again as 3D.
     */
    private static final class UntaggedThirdOrdinate extends IllegalArgumentException {

        private static final long serialVersionUID = 1L;

        private UntaggedThirdOrdinate() {
            super(MSG_UNTAGGED_THIRD_ORDINATE);
        }

        /**
         * The exception steers the second parse and is never surfaced, so it carries no stack
         * trace.
         */
        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }
}

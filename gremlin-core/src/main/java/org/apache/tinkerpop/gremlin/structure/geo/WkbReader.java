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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Package-private WKB parser. Accepts ISO WKB only; rejects EWKB.
 * <p>
 * Reads exactly one geometry from an {@link InputStream}, leaving the stream on the next byte.
 * Each geometry declares its own byte order, so a little-endian geometry is swapped as it is
 * read. A stream that ends early raises an {@link EOFException}, and malformed bytes raise an
 * {@link IllegalArgumentException}.
 * <p>
 * {@link #parse(byte[], int)} uses the same code over a {@link ByteArrayInputStream}, reports an
 * early end as an {@link IllegalArgumentException}, rejects bytes left after the geometry, and
 * checks a count read off the input against the bytes left before reading its items.
 * <p>
 * ISO WKB has no length prefix for a point, so an empty point is encoded as a point whose
 * x and y are NaN. A 3D point with NaN x and y therefore reads as {@code POINT Z EMPTY},
 * and its z ordinate is discarded, because an empty point carries no coordinate at all.
 */
final class WkbReader {

    private static final int TYPE_POINT = 1;
    private static final int TYPE_LINESTRING = 2;
    private static final int TYPE_POLYGON = 3;
    private static final int TYPE_MULTIPOINT = 4;
    private static final int TYPE_MULTILINESTRING = 5;
    private static final int TYPE_MULTIPOLYGON = 6;
    private static final int TYPE_GEOMETRYCOLLECTION = 7;

    // EWKB flags
    private static final int EWKB_Z_FLAG = 0x80000000;
    private static final int EWKB_M_FLAG = 0x40000000;
    private static final int EWKB_SRID_FLAG = 0x20000000;

    /**
     * Largest list capacity reserved up front for a count read from the input. A larger
     * count grows the list as the items are actually read.
     */
    private static final int MAX_INITIAL_CAPACITY = 1024;

    /**
     * Set where the input is a byte array, whose number of bytes left to read is known, and
     * null where the input is a stream.
     */
    private final ByteArrayInputStream source;
    private final DataInputStream in;
    private final int srid;
    private int depth;
    private Boolean hasZ; // null = not yet determined

    private WkbReader(final InputStream in, final int srid) {
        // never buffered, so the input is left on the byte after the geometry
        this.in = new DataInputStream(in);
        this.source = null;
        this.srid = srid;
        this.depth = 0;
    }

    private WkbReader(final ByteArrayInputStream in, final int srid) {
        // never buffered, so available() on the source is the number of bytes left to read
        this.in = new DataInputStream(in);
        this.source = in;
        this.srid = srid;
        this.depth = 0;
    }

    /**
     * Parses ISO WKB bytes.
     *
     * @param wkb the WKB bytes
     * @param srid the spatial reference ID to use
     * @return the parsed geometry
     */
    static Geometry parse(final byte[] wkb, final int srid) {
        if (wkb == null || wkb.length == 0) {
            throw new IllegalArgumentException("WKB input cannot be null or empty");
        }

        final ByteArrayInputStream bytes = new ByteArrayInputStream(wkb);
        final WkbReader reader = new WkbReader(bytes, srid);
        final Geometry g;
        try {
            g = reader.readGeometry();
        } catch (EOFException eofe) {
            throw new IllegalArgumentException("Truncated WKB input: unexpected end of input", eofe);
        } catch (IOException ioe) {
            // reading bytes already in memory cannot fail for any other reason
            throw new IllegalArgumentException("Malformed WKB input: " + ioe.getMessage(), ioe);
        }

        // Check for trailing bytes
        if (bytes.available() > 0) {
            throw new IllegalArgumentException("Unexpected trailing bytes in WKB input");
        }

        return g;
    }

    /**
     * Parses one ISO WKB geometry from a stream, leaving the stream on the byte after it.
     *
     * @param in the stream to read from
     * @param srid the spatial reference ID to use
     * @return the parsed geometry
     * @throws EOFException if the stream ends inside the geometry
     * @throws IOException if the stream cannot be read
     */
    static Geometry parse(final InputStream in, final int srid) throws IOException {
        if (in == null) {
            throw new IllegalArgumentException("WKB input cannot be null");
        }

        return new WkbReader(in, srid).readGeometry();
    }

    private Geometry readGeometry() throws IOException {
        final byte byteOrder = readByte();
        final boolean littleEndian;
        if (byteOrder == 0x00) {
            littleEndian = false;
        } else if (byteOrder == 0x01) {
            littleEndian = true;
        } else {
            throw new IllegalArgumentException("Invalid WKB byte order: " + byteOrder);
        }

        final int typeCode = readInt(littleEndian);

        // Check for EWKB flags
        if ((typeCode & EWKB_Z_FLAG) != 0 || (typeCode & EWKB_M_FLAG) != 0 || (typeCode & EWKB_SRID_FLAG) != 0) {
            throw new IllegalArgumentException("EWKB format detected (type code " + String.format("0x%08X", typeCode) +
                    "); ISO WKB is expected. EWKB with Z, M, or SRID flags is not supported.");
        }

        // Check for M dimension (2000s) and ZM dimension (3000s)
        if (typeCode >= 2001 && typeCode <= 2007) {
            throw new IllegalArgumentException("M dimension not supported (type code " + typeCode + ")");
        }
        if (typeCode >= 3001 && typeCode <= 3007) {
            throw new IllegalArgumentException("ZM dimension not supported (type code " + typeCode + ")");
        }

        // Parse type and Z flag from ISO codes
        final int baseType;
        final boolean z;
        if (typeCode >= 1001 && typeCode <= 1007) {
            baseType = typeCode - 1000;
            z = true;
        } else if (typeCode >= 1 && typeCode <= 7) {
            baseType = typeCode;
            z = false;
        } else {
            throw new IllegalArgumentException("Unknown WKB type code: " + typeCode);
        }

        // Validate Z consistency
        if (hasZ == null) {
            hasZ = z;
        } else if (hasZ != z) {
            throw new IllegalArgumentException("Mixed Z dimensions in WKB: some geometries have Z, some do not");
        }

        switch (baseType) {
            case TYPE_POINT:
                return readPoint(z, littleEndian);
            case TYPE_LINESTRING:
                return readLineString(z, littleEndian);
            case TYPE_POLYGON:
                return readPolygon(z, littleEndian);
            case TYPE_MULTIPOINT:
                return readMultiPoint(z, littleEndian);
            case TYPE_MULTILINESTRING:
                return readMultiLineString(z, littleEndian);
            case TYPE_MULTIPOLYGON:
                return readMultiPolygon(z, littleEndian);
            case TYPE_GEOMETRYCOLLECTION:
                return readGeometryCollection(z, littleEndian);
            default:
                throw new IllegalArgumentException("Unknown WKB type: " + baseType);
        }
    }

    /**
     * Reads one point. NaN x and y encode an empty point, so such a 3D point reads as an
     * empty point with a Z tag and its z ordinate is read and discarded.
     */
    private Point readPoint(final boolean z, final boolean littleEndian) throws IOException {
        final double x = readDouble(littleEndian);
        final double y = readDouble(littleEndian);

        // Check for empty point (NaN coordinates)
        if (Double.isNaN(x) && Double.isNaN(y)) {
            if (z) {
                readDouble(littleEndian); // read and discard z
            }
            return Point.emptyWithZ(srid, z);
        }

        // Validate coordinates
        CoordinateValidator.validateX(x, srid);
        CoordinateValidator.validateY(y, srid);

        if (z) {
            final double zVal = readDouble(littleEndian);
            CoordinateValidator.validateZ(zVal);
            return Point.ofWithSrid(x, y, zVal, srid);
        } else {
            return Point.ofWithSrid(x, y, srid);
        }
    }

    private LineString readLineString(final boolean z, final boolean littleEndian) throws IOException {
        final int numPoints = readInt(littleEndian);

        if (numPoints < 0) {
            throw new IllegalArgumentException("Invalid point count: " + numPoints);
        }

        if (numPoints == 0) {
            return LineString.emptyWithSrid(srid, z);
        }

        if (numPoints == 1) {
            throw new IllegalArgumentException("LineString must have at least 2 points, got 1");
        }

        final int coordSize = z ? 24 : 16;
        checkRemainingForCount(numPoints, coordSize, "linestring coordinates");

        final List<Point> points = new ArrayList<>(initialCapacity(numPoints));
        for (int i = 0; i < numPoints; i++) {
            points.add(readCoordinate(z, littleEndian, "linestring vertex"));
        }

        return LineString.ofWithSrid(points, srid, z);
    }

    /**
     * Reads one coordinate as a vertex of a line string or ring. A vertex must be a real
     * coordinate, so NaN, which encodes an empty point, is malformed here.
     */
    private Point readCoordinate(final boolean z, final boolean littleEndian, final String context) throws IOException {
        final double x = readDouble(littleEndian);
        final double y = readDouble(littleEndian);
        if (Double.isNaN(x) && Double.isNaN(y)) {
            throw new IllegalArgumentException("A " + context
                    + " cannot be an empty point; a vertex must be a real coordinate");
        }
        CoordinateValidator.validateX(x, srid);
        CoordinateValidator.validateY(y, srid);

        if (z) {
            final double zVal = readDouble(littleEndian);
            CoordinateValidator.validateZ(zVal);
            return Point.ofWithSrid(x, y, zVal, srid);
        }
        return Point.ofWithSrid(x, y, srid);
    }

    private Polygon readPolygon(final boolean z, final boolean littleEndian) throws IOException {
        final int numRings = readInt(littleEndian);

        if (numRings < 0) {
            throw new IllegalArgumentException("Invalid ring count: " + numRings);
        }

        if (numRings == 0) {
            return Polygon.emptyWithSrid(srid, z);
        }

        // Each ring needs at least its own 4-byte point count, so check the declared ring
        // count against the bytes remaining before reserving anything for it
        checkRemainingForCount(numRings, 4, "polygon rings");

        final List<LineString> rings = new ArrayList<>(initialCapacity(numRings));
        for (int i = 0; i < numRings; i++) {
            final int numPoints = readInt(littleEndian);

            if (numPoints < 0) {
                throw new IllegalArgumentException("Invalid point count in ring: " + numPoints);
            }

            if (numPoints < 4) {
                throw new IllegalArgumentException("Polygon ring must have at least 4 points, got " + numPoints);
            }

            final int coordSize = z ? 24 : 16;
            checkRemainingForCount(numPoints, coordSize, "ring coordinates");

            final List<Point> points = new ArrayList<>(initialCapacity(numPoints));
            for (int j = 0; j < numPoints; j++) {
                points.add(readCoordinate(z, littleEndian, "ring vertex"));
            }

            final LineString ring = LineString.ofWithSrid(points, srid, z);
            // Validate ring is closed
            CoordinateValidator.validateRing(ring, i == 0 ? "Shell ring" : "Hole ring");
            rings.add(ring);
        }

        final LineString shell = rings.get(0);
        final List<LineString> holes = new ArrayList<>(rings.subList(1, rings.size()));
        return Polygon.ofWithSrid(shell, holes, srid, z);
    }

    private MultiPoint readMultiPoint(final boolean z, final boolean littleEndian) throws IOException {
        depth++;
        CoordinateValidator.validateNestingDepth(depth);

        final int numGeoms = readInt(littleEndian);

        if (numGeoms < 0) {
            throw new IllegalArgumentException("Invalid geometry count: " + numGeoms);
        }

        if (numGeoms == 0) {
            depth--;
            return MultiPoint.emptyWithSrid(srid, z);
        }

        // Basic size check: at least 5 bytes per geometry (1 byte order + 4 type)
        checkRemainingForCount(numGeoms, 5, "multi-point geometries");

        final List<Point> points = new ArrayList<>(initialCapacity(numGeoms));
        for (int i = 0; i < numGeoms; i++) {
            final Geometry g = readGeometry();
            if (!(g instanceof Point)) {
                throw new IllegalArgumentException("Expected Point in MultiPoint, got " + g.getClass().getSimpleName());
            }
            points.add((Point) g);
        }

        depth--;
        return MultiPoint.ofWithSrid(points, srid, z);
    }

    private MultiLineString readMultiLineString(final boolean z, final boolean littleEndian) throws IOException {
        depth++;
        CoordinateValidator.validateNestingDepth(depth);

        final int numGeoms = readInt(littleEndian);

        if (numGeoms < 0) {
            throw new IllegalArgumentException("Invalid geometry count: " + numGeoms);
        }

        if (numGeoms == 0) {
            depth--;
            return MultiLineString.emptyWithSrid(srid, z);
        }

        checkRemainingForCount(numGeoms, 5, "multi-linestring geometries");

        final List<LineString> lineStrings = new ArrayList<>(initialCapacity(numGeoms));
        for (int i = 0; i < numGeoms; i++) {
            final Geometry g = readGeometry();
            if (!(g instanceof LineString)) {
                throw new IllegalArgumentException("Expected LineString in MultiLineString, got " + g.getClass().getSimpleName());
            }
            lineStrings.add((LineString) g);
        }

        depth--;
        return MultiLineString.ofWithSrid(lineStrings, srid, z);
    }

    private MultiPolygon readMultiPolygon(final boolean z, final boolean littleEndian) throws IOException {
        depth++;
        CoordinateValidator.validateNestingDepth(depth);

        final int numGeoms = readInt(littleEndian);

        if (numGeoms < 0) {
            throw new IllegalArgumentException("Invalid geometry count: " + numGeoms);
        }

        if (numGeoms == 0) {
            depth--;
            return MultiPolygon.emptyWithSrid(srid, z);
        }

        checkRemainingForCount(numGeoms, 5, "multi-polygon geometries");

        final List<Polygon> polygons = new ArrayList<>(initialCapacity(numGeoms));
        for (int i = 0; i < numGeoms; i++) {
            final Geometry g = readGeometry();
            if (!(g instanceof Polygon)) {
                throw new IllegalArgumentException("Expected Polygon in MultiPolygon, got " + g.getClass().getSimpleName());
            }
            polygons.add((Polygon) g);
        }

        depth--;
        return MultiPolygon.ofWithSrid(polygons, srid, z);
    }

    private GeometryCollection readGeometryCollection(final boolean z, final boolean littleEndian) throws IOException {
        depth++;
        CoordinateValidator.validateNestingDepth(depth);

        final int numGeoms = readInt(littleEndian);

        if (numGeoms < 0) {
            throw new IllegalArgumentException("Invalid geometry count: " + numGeoms);
        }

        if (numGeoms == 0) {
            depth--;
            return GeometryCollection.emptyWithSrid(srid, z);
        }

        checkRemainingForCount(numGeoms, 5, "geometry collection geometries");

        final List<Geometry> geometries = new ArrayList<>(initialCapacity(numGeoms));
        for (int i = 0; i < numGeoms; i++) {
            geometries.add(readGeometry());
        }

        depth--;
        return GeometryCollection.createWithSrid(geometries, srid, z);
    }

    private byte readByte() throws IOException {
        return in.readByte();
    }

    private int readInt(final boolean littleEndian) throws IOException {
        final int value = in.readInt();
        return littleEndian ? Integer.reverseBytes(value) : value;
    }

    private double readDouble(final boolean littleEndian) throws IOException {
        final long bits = in.readLong();
        return Double.longBitsToDouble(littleEndian ? Long.reverseBytes(bits) : bits);
    }

    private void checkRemainingForCount(final int count, final int bytesPerItem, final String context) {
        if (source == null) {
            return;
        }
        // Check for overflow when computing byte requirement
        if (count < 0 || (bytesPerItem > 0 && count > Integer.MAX_VALUE / bytesPerItem)) {
            throw new IllegalArgumentException("Declared count " + count + " for " + context + " is invalid or would overflow");
        }
        // Check that there's enough data
        final int bytesNeeded = count * bytesPerItem;
        if (source.available() < bytesNeeded) {
            throw new IllegalArgumentException("Declared count " + count + " for " + context +
                    " exceeds available bytes (" + source.available() + " remaining)");
        }
    }

    /**
     * Bounds the capacity reserved for a count read from the input, so a hostile count
     * cannot force a large allocation before the items are read.
     */
    private static int initialCapacity(final int count) {
        return Math.min(count, MAX_INITIAL_CAPACITY);
    }
}

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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Package-private WKB parser. Accepts ISO WKB only; rejects EWKB.
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

    private final ByteBuffer buffer;
    private final int srid;
    private int depth;
    private Boolean hasZ; // null = not yet determined

    private WkbReader(final byte[] wkb, final int srid) {
        this.buffer = ByteBuffer.wrap(wkb);
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

        final WkbReader reader = new WkbReader(wkb, srid);
        final Geometry g = reader.readGeometry();

        // Check for trailing bytes
        if (reader.buffer.hasRemaining()) {
            throw new IllegalArgumentException("Unexpected trailing bytes in WKB input");
        }

        return g;
    }

    private Geometry readGeometry() {
        checkRemaining(1, "byte order");
        final byte byteOrder = buffer.get();
        if (byteOrder == 0x00) {
            buffer.order(ByteOrder.BIG_ENDIAN);
        } else if (byteOrder == 0x01) {
            buffer.order(ByteOrder.LITTLE_ENDIAN);
        } else {
            throw new IllegalArgumentException("Invalid WKB byte order: " + byteOrder);
        }

        checkRemaining(4, "type code");
        final int typeCode = buffer.getInt();

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
                return readPoint(z);
            case TYPE_LINESTRING:
                return readLineString(z);
            case TYPE_POLYGON:
                return readPolygon(z);
            case TYPE_MULTIPOINT:
                return readMultiPoint(z);
            case TYPE_MULTILINESTRING:
                return readMultiLineString(z);
            case TYPE_MULTIPOLYGON:
                return readMultiPolygon(z);
            case TYPE_GEOMETRYCOLLECTION:
                return readGeometryCollection(z);
            default:
                throw new IllegalArgumentException("Unknown WKB type: " + baseType);
        }
    }

    /**
     * Reads one point. NaN x and y encode an empty point, so such a 3D point reads as an
     * empty point with a Z tag and its z ordinate is read and discarded.
     */
    private Point readPoint(final boolean z) {
        final int coordSize = z ? 24 : 16; // 3 or 2 doubles
        checkRemaining(coordSize, "point coordinates");

        final double x = buffer.getDouble();
        final double y = buffer.getDouble();

        // Check for empty point (NaN coordinates)
        if (Double.isNaN(x) && Double.isNaN(y)) {
            if (z) {
                buffer.getDouble(); // read and discard z
            }
            return Point.emptyWithZ(srid, z);
        }

        // Validate coordinates
        CoordinateValidator.validateX(x, srid);
        CoordinateValidator.validateY(y, srid);

        if (z) {
            final double zVal = buffer.getDouble();
            CoordinateValidator.validateZ(zVal);
            return Point.ofWithSrid(x, y, zVal, srid);
        } else {
            return Point.ofWithSrid(x, y, srid);
        }
    }

    private LineString readLineString(final boolean z) {
        checkRemaining(4, "point count");
        final int numPoints = buffer.getInt();

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
            points.add(readCoordinate(z, "linestring vertex"));
        }

        return LineString.ofWithSrid(points, srid, z);
    }

    /**
     * Reads one coordinate as a vertex of a line string or ring. A vertex must be a real
     * coordinate, so NaN, which encodes an empty point, is malformed here.
     */
    private Point readCoordinate(final boolean z, final String context) {
        final double x = buffer.getDouble();
        final double y = buffer.getDouble();
        if (Double.isNaN(x) && Double.isNaN(y)) {
            throw new IllegalArgumentException("A " + context
                    + " cannot be an empty point; a vertex must be a real coordinate");
        }
        CoordinateValidator.validateX(x, srid);
        CoordinateValidator.validateY(y, srid);

        if (z) {
            final double zVal = buffer.getDouble();
            CoordinateValidator.validateZ(zVal);
            return Point.ofWithSrid(x, y, zVal, srid);
        }
        return Point.ofWithSrid(x, y, srid);
    }

    private Polygon readPolygon(final boolean z) {
        checkRemaining(4, "ring count");
        final int numRings = buffer.getInt();

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
            checkRemaining(4, "point count for ring");
            final int numPoints = buffer.getInt();

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
                points.add(readCoordinate(z, "ring vertex"));
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

    private MultiPoint readMultiPoint(final boolean z) {
        depth++;
        CoordinateValidator.validateNestingDepth(depth);

        checkRemaining(4, "geometry count");
        final int numGeoms = buffer.getInt();

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

    private MultiLineString readMultiLineString(final boolean z) {
        depth++;
        CoordinateValidator.validateNestingDepth(depth);

        checkRemaining(4, "geometry count");
        final int numGeoms = buffer.getInt();

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

    private MultiPolygon readMultiPolygon(final boolean z) {
        depth++;
        CoordinateValidator.validateNestingDepth(depth);

        checkRemaining(4, "geometry count");
        final int numGeoms = buffer.getInt();

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

    private GeometryCollection readGeometryCollection(final boolean z) {
        depth++;
        CoordinateValidator.validateNestingDepth(depth);

        checkRemaining(4, "geometry count");
        final int numGeoms = buffer.getInt();

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

    private void checkRemaining(final int bytes, final String context) {
        if (buffer.remaining() < bytes) {
            throw new IllegalArgumentException("Truncated WKB input: need " + bytes + " bytes for " + context +
                    ", but only " + buffer.remaining() + " remaining");
        }
    }

    private void checkRemainingForCount(final int count, final int bytesPerItem, final String context) {
        // Check for overflow when computing byte requirement
        if (count < 0 || (bytesPerItem > 0 && count > Integer.MAX_VALUE / bytesPerItem)) {
            throw new IllegalArgumentException("Declared count " + count + " for " + context + " is invalid or would overflow");
        }
        // Check that there's enough data
        final int bytesNeeded = count * bytesPerItem;
        if (buffer.remaining() < bytesNeeded) {
            throw new IllegalArgumentException("Declared count " + count + " for " + context +
                    " exceeds available bytes (" + buffer.remaining() + " remaining)");
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

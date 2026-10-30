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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Package-private WKB writer. Writes ISO WKB in big-endian byte order.
 */
final class WkbWriter {

    private static final byte BIG_ENDIAN = 0x00;

    private WkbWriter() {
    }

    /**
     * Writes a geometry to ISO WKB format (big-endian).
     */
    static byte[] write(final Geometry g) {
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutputStream dos = new DataOutputStream(baos);
            writeGeometry(dos, g);
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Unexpected I/O error writing WKB", e);
        }
    }

    private static void writeGeometry(final DataOutputStream dos, final Geometry g) throws IOException {
        // Byte order: big-endian
        dos.writeByte(BIG_ENDIAN);

        // Type code: base type + 1000 for Z
        int typeCode = g.wkbTypeCode();
        if (g.hasZ()) {
            typeCode += 1000;
        }
        dos.writeInt(typeCode);

        // Write geometry-specific data
        if (g instanceof Point) {
            writePoint(dos, (Point) g);
        } else if (g instanceof LineString) {
            writeLineString(dos, (LineString) g);
        } else if (g instanceof Polygon) {
            writePolygon(dos, (Polygon) g);
        } else if (g instanceof MultiPoint) {
            writeMultiPoint(dos, (MultiPoint) g);
        } else if (g instanceof MultiLineString) {
            writeMultiLineString(dos, (MultiLineString) g);
        } else if (g instanceof MultiPolygon) {
            writeMultiPolygon(dos, (MultiPolygon) g);
        } else if (g instanceof GeometryCollection) {
            writeGeometryCollection(dos, (GeometryCollection) g);
        } else {
            throw new IllegalStateException("No WKB encoding for geometry type " + g.getClass().getName());
        }
    }

    private static void writePoint(final DataOutputStream dos, final Point p) throws IOException {
        writeCoordinate(dos, p, p.hasZ());
    }

    /**
     * Writes the coordinates of a point, with z included when the enclosing geometry is 3D.
     */
    private static void writeCoordinate(final DataOutputStream dos, final Point p, final boolean hasZ) throws IOException {
        if (p.isEmpty()) {
            // ISO WKB encodes an empty point as NaN coordinates
            dos.writeDouble(Double.NaN);
            dos.writeDouble(Double.NaN);
            if (hasZ) {
                dos.writeDouble(Double.NaN);
            }
            return;
        }
        dos.writeDouble(p.x());
        dos.writeDouble(p.y());
        if (hasZ) {
            dos.writeDouble(p.z());
        }
    }

    private static void writeLineString(final DataOutputStream dos, final LineString ls) throws IOException {
        final List<Point> points = ls.points();
        dos.writeInt(points.size());
        for (final Point p : points) {
            writeCoordinate(dos, p, ls.hasZ());
        }
    }

    private static void writePolygon(final DataOutputStream dos, final Polygon poly) throws IOException {
        final int ringCount = poly.ringCount();
        dos.writeInt(ringCount);

        if (ringCount > 0) {
            writeRing(dos, poly.exteriorRing());
            for (final LineString hole : poly.interiorRings()) {
                writeRing(dos, hole);
            }
        }
    }

    private static void writeRing(final DataOutputStream dos, final LineString ring) throws IOException {
        final List<Point> points = ring.points();
        dos.writeInt(points.size());
        for (final Point p : points) {
            writeCoordinate(dos, p, ring.hasZ());
        }
    }

    private static void writeMultiPoint(final DataOutputStream dos, final MultiPoint mp) throws IOException {
        final List<Point> points = mp.geometries();
        dos.writeInt(points.size());
        for (final Point p : points) {
            writeGeometry(dos, p);
        }
    }

    private static void writeMultiLineString(final DataOutputStream dos, final MultiLineString mls) throws IOException {
        final List<LineString> lineStrings = mls.geometries();
        dos.writeInt(lineStrings.size());
        for (final LineString ls : lineStrings) {
            writeGeometry(dos, ls);
        }
    }

    private static void writeMultiPolygon(final DataOutputStream dos, final MultiPolygon mpoly) throws IOException {
        final List<Polygon> polygons = mpoly.geometries();
        dos.writeInt(polygons.size());
        for (final Polygon p : polygons) {
            writeGeometry(dos, p);
        }
    }

    private static void writeGeometryCollection(final DataOutputStream dos, final GeometryCollection gc) throws IOException {
        final List<? extends Geometry> geometries = gc.geometries();
        dos.writeInt(geometries.size());
        for (final Geometry g : geometries) {
            writeGeometry(dos, g);
        }
    }
}

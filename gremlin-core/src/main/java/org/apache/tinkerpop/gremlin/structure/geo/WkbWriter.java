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
import java.io.UncheckedIOException;
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
        } catch (UncheckedIOException e) {
            // the body writers run behind the visitor, which carries no checked exception
            throw new RuntimeException("Unexpected I/O error writing WKB", e.getCause());
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
        g.accept(new BodyVisitor(dos));
    }

    /**
     * Writes the WKB body of each kind, the part that follows the byte order marker and the type
     * code.
     */
    private static final class BodyVisitor implements GeometryVisitor<Void> {

        private final DataOutputStream dos;

        private BodyVisitor(final DataOutputStream dos) {
            this.dos = dos;
        }

        @Override
        public Void visitPoint(final Point p) {
            return write(() -> writeCoordinate(dos, p, p.hasZ()));
        }

        @Override
        public Void visitLineString(final LineString ls) {
            return write(() -> writeLineString(dos, ls));
        }

        @Override
        public Void visitPolygon(final Polygon poly) {
            return write(() -> writePolygon(dos, poly));
        }

        @Override
        public Void visitMultiPoint(final MultiPoint mp) {
            return write(() -> writeElements(dos, mp.geometries()));
        }

        @Override
        public Void visitMultiLineString(final MultiLineString mls) {
            return write(() -> writeElements(dos, mls.geometries()));
        }

        @Override
        public Void visitMultiPolygon(final MultiPolygon mpoly) {
            return write(() -> writeElements(dos, mpoly.geometries()));
        }

        @Override
        public Void visitGeometryCollection(final GeometryCollection gc) {
            return write(() -> writeElements(dos, gc.geometries()));
        }

        /**
         * Runs a body writer, turning the I/O failure it declares into an unchecked one. The
         * stream writes to a byte array, so no such failure arises in practice.
         */
        private Void write(final BodyWriter writer) {
            try {
                writer.write();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return null;
        }
    }

    /**
     * A body writer, which may declare an I/O failure the visitor cannot.
     */
    @FunctionalInterface
    private interface BodyWriter {
        void write() throws IOException;
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

    /**
     * Writes a sequence of points, the point count followed by each coordinate. A polygon ring
     * carries the same form.
     */
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
            writeLineString(dos, poly.exteriorRing());
            for (final LineString hole : poly.interiorRings()) {
                writeLineString(dos, hole);
            }
        }
    }

    /**
     * Writes the elements of a collection, each as a geometry of its own with its byte order
     * marker and type code.
     */
    private static void writeElements(final DataOutputStream dos, final List<? extends Geometry> elements) throws IOException {
        dos.writeInt(elements.size());
        for (final Geometry element : elements) {
            writeGeometry(dos, element);
        }
    }
}

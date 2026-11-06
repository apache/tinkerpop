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

import java.util.Arrays;

/**
 * Package-private helper that walks the coordinates of a geometry in WKB order.
 */
final class Coordinates {

    private Coordinates() {
    }

    /**
     * Receives the non-empty points of a geometry.
     */
    interface PointVisitor {
        void visit(Point point);
    }

    /**
     * Visits the non-empty points of a geometry in WKB order: the point itself, the points of a
     * line string, the shell ring and then the hole rings of a polygon, and the parts of a
     * collection in order. An empty point holds no coordinate, so it is skipped.
     */
    static void visitPoints(final Geometry geometry, final PointVisitor visitor) {
        if (geometry instanceof Point) {
            final Point point = (Point) geometry;
            if (!point.isEmpty()) {
                visitor.visit(point);
            }
            return;
        }
        for (final Geometry part : geometry.structuralParts()) {
            visitPoints(part, visitor);
        }
    }

    /**
     * Returns the coordinates of a geometry in WKB order as x and y, followed by z for a 3D
     * geometry. The array is empty when the geometry holds no coordinate.
     */
    static double[] flatten(final Geometry geometry) {
        final CollectingVisitor collector = new CollectingVisitor();
        visitPoints(geometry, collector);
        return collector.toArray();
    }

    /**
     * Returns the 2D bounding box of a geometry as {@code {minX, minY, maxX, maxY}}, or null when
     * the geometry holds no coordinate.
     */
    static double[] boundingBox(final Geometry geometry) {
        final BoundingBoxVisitor box = new BoundingBoxVisitor();
        visitPoints(geometry, box);
        return box.toArray();
    }

    /**
     * Collects x, y and, for a 3D geometry, z of every visited point.
     */
    private static final class CollectingVisitor implements PointVisitor {

        private double[] values = new double[16]; // fits a point or a short line; the array doubles in size when full
        private int size;

        @Override
        public void visit(final Point point) {
            add(point.x());
            add(point.y());
            if (point.hasZ()) {
                add(point.z());
            }
        }

        private void add(final double value) {
            if (size == values.length) {
                values = Arrays.copyOf(values, size * 2);
            }
            values[size++] = value;
        }

        private double[] toArray() {
            return size == values.length ? values : Arrays.copyOf(values, size);
        }
    }

    /**
     * Tracks the 2D extent of every visited point.
     */
    private static final class BoundingBoxVisitor implements PointVisitor {

        private double minX;
        private double minY;
        private double maxX;
        private double maxY;
        private boolean any;

        @Override
        public void visit(final Point point) {
            final double x = point.x();
            final double y = point.y();
            if (!any) {
                minX = x;
                maxX = x;
                minY = y;
                maxY = y;
                any = true;
                return;
            }
            if (x < minX) minX = x;
            if (x > maxX) maxX = x;
            if (y < minY) minY = y;
            if (y > maxY) maxY = y;
        }

        private double[] toArray() {
            return any ? new double[]{minX, minY, maxX, maxY} : null;
        }
    }
}

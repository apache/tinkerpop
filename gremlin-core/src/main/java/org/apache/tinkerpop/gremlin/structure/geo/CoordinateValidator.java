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

import java.util.List;

/**
 * Package-private helper for coordinate validation.
 */
final class CoordinateValidator {

    static final int SRID_CARTESIAN = 0;
    static final int SRID_GEOGRAPHIC = 4326;
    static final int MAX_NESTING_DEPTH = 32;

    private CoordinateValidator() {
    }

    /**
     * Returns the Model for a given SRID.
     */
    static Model modelForSrid(final int srid) {
        if (srid == SRID_CARTESIAN) {
            return Model.CARTESIAN;
        } else if (srid == SRID_GEOGRAPHIC) {
            return Model.GEOGRAPHIC;
        } else {
            return Model.OTHER;
        }
    }

    /**
     * Returns the SRID for a given Model.
     */
    static int sridForModel(final Model model) {
        if (model == null) {
            throw new IllegalArgumentException("Model cannot be null");
        }
        switch (model) {
            case CARTESIAN:
                return SRID_CARTESIAN;
            case GEOGRAPHIC:
                return SRID_GEOGRAPHIC;
            default:
                throw new IllegalArgumentException("Model.OTHER cannot be used in constructors; use fromWkt(wkt, srid) or fromWkb(wkb, srid) with an explicit SRID");
        }
    }

    /**
     * Validates x coordinate (longitude for GEOGRAPHIC).
     */
    static void validateX(final double x, final int srid) {
        if (Double.isNaN(x) || Double.isInfinite(x)) {
            throw new IllegalArgumentException("x coordinate cannot be NaN or infinite");
        }
        if (srid == SRID_GEOGRAPHIC) {
            if (x < -180.0 || x > 180.0) {
                throw new IllegalArgumentException("GEOGRAPHIC longitude must be in [-180, 180], got: " + x);
            }
        }
    }

    /**
     * Validates y coordinate (latitude for GEOGRAPHIC).
     */
    static void validateY(final double y, final int srid) {
        if (Double.isNaN(y) || Double.isInfinite(y)) {
            throw new IllegalArgumentException("y coordinate cannot be NaN or infinite");
        }
        if (srid == SRID_GEOGRAPHIC) {
            if (y < -90.0 || y > 90.0) {
                throw new IllegalArgumentException("GEOGRAPHIC latitude must be in [-90, 90], got: " + y);
            }
        }
    }

    /**
     * Validates z coordinate.
     */
    static void validateZ(final double z) {
        if (Double.isNaN(z)) {
            throw new IllegalArgumentException("z coordinate cannot be NaN when provided; use 2D constructor for 2D points");
        }
        if (Double.isInfinite(z)) {
            throw new IllegalArgumentException("z coordinate cannot be infinite");
        }
    }

    /**
     * Validates that the nesting depth does not exceed the maximum.
     */
    static void validateNestingDepth(final int depth) {
        if (depth > MAX_NESTING_DEPTH) {
            throw new IllegalArgumentException("Collection nesting depth exceeds maximum of " + MAX_NESTING_DEPTH);
        }
    }

    /**
     * Computes the collection nesting depth of a geometry.
     * Collections and Multi* types count as 1 level plus their deepest child.
     * Non-collection types have depth 0.
     */
    static int computeDepth(final Geometry g) {
        if (g instanceof GeometryCollection) {
            final GeometryCollection gc = (GeometryCollection) g;
            int maxChildDepth = 0;
            for (final Geometry child : gc.geometries()) {
                maxChildDepth = Math.max(maxChildDepth, computeDepth(child));
            }
            return 1 + maxChildDepth;
        }
        return 0;
    }

    /**
     * Validates that all geometries in a collection have the same SRID.
     */
    static void validateSameSrid(final int expected, final int actual, final String context) {
        if (expected != actual) {
            throw new IllegalArgumentException("Mixed SRIDs in " + context + ": expected " + expected + ", got " + actual);
        }
    }

    /**
     * Validates that all geometries have the same z-dimension flag.
     */
    static void validateSameHasZ(final boolean expected, final boolean actual, final String context) {
        if (expected != actual) {
            throw new IllegalArgumentException("Mixed z dimensions in " + context + ": some coordinates have z, some do not");
        }
    }

    /**
     * Validates a ring (for polygons): must be closed and have at least 4 points.
     */
    static void validateRing(final LineString ring, final String context) {
        if (ring.isEmpty()) {
            return; // an empty ring is only valid as the whole polygon being empty
        }
        final List<Point> points = ring.points();
        if (points.size() < 4) {
            throw new IllegalArgumentException(context + " must have at least 4 points, got: " + points.size());
        }
        final Point first = points.get(0);
        final Point last = points.get(points.size() - 1);
        if (!first.equals(last)) {
            throw new IllegalArgumentException(context + " must be closed (first and last point must be equal)");
        }
    }

    /**
     * Validates that a point is usable as a vertex of a line string or polygon ring.
     * A vertex must be a real coordinate, so an empty point is rejected.
     */
    static void validateVertex(final Point point, final String context, final int index) {
        requireNonNullElement(point, context, index);
        if (point.isEmpty()) {
            throw new IllegalArgumentException(context + " vertex at index " + index
                    + " cannot be an empty point; a vertex must be a real coordinate");
        }
    }

    /**
     * Validates that a list or array argument was supplied.
     */
    static void requireNonNullCollection(final Object collection, final String context) {
        if (collection == null) {
            throw new IllegalArgumentException(context + " cannot be null");
        }
    }

    /**
     * Validates that an element of a list or array was supplied.
     */
    static void requireNonNullElement(final Object element, final String context, final int index) {
        if (element == null) {
            throw new IllegalArgumentException(context + " element at index " + index + " cannot be null");
        }
    }

    /**
     * Computes a hash code for a coordinate that treats -0.0 and 0.0 as equal.
     */
    static long coordinateHashBits(final double value) {
        // Normalize zero values so -0.0 and 0.0 hash the same
        final double normalized = (value == 0.0) ? 0.0 : value;
        return Double.doubleToLongBits(normalized);
    }
}

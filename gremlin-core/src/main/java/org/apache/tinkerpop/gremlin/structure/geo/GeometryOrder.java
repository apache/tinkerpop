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
 * Package-private implementation of the geometry order described in
 * {@link Geometry#compareTo(Geometry)}. The order is total, and {@code compare(a, b) == 0} holds
 * exactly when {@code a.equals(b)}.
 */
final class GeometryOrder {

    private GeometryOrder() {
    }

    /**
     * Compares two geometries by SRID, emptiness, Hilbert key, coordinates, kind and structure, in
     * that order.
     */
    static int compare(final Geometry first, final Geometry second) {
        if (first == second) {
            return 0;
        }

        // 1: the SRID
        int result = Integer.compare(first.srid(), second.srid());
        if (result != 0) {
            return result;
        }

        // 2: empty geometries first
        final boolean firstEmpty = first.isEmpty();
        final boolean secondEmpty = second.isEmpty();
        if (firstEmpty != secondEmpty) {
            return firstEmpty ? -1 : 1;
        }

        // 3: the Hilbert key of the bounding box centre, which an empty geometry does not have
        if (!firstEmpty) {
            result = Integer.compareUnsigned(first.hilbertKey(), second.hilbertKey());
            if (result != 0) {
                return result;
            }
        }

        // 4: the coordinates
        result = compareCoordinates(first, second);
        if (result != 0) {
            return result;
        }

        // 5: the kind, by WKB type code
        result = Integer.compare(first.wkbTypeCode(), second.wkbTypeCode());
        if (result != 0) {
            return result;
        }

        // 6: the structure, then a geometry without z before one with z
        result = compareStructure(first, second);
        if (result != 0) {
            return result;
        }
        return Boolean.compare(first.hasZ(), second.hasZ());
    }

    /**
     * Compares the coordinates in WKB order, numerically, so that -0.0 and 0.0 tie as they do in
     * {@code equals}. The shorter sequence sorts first. Coordinates are never NaN, since a
     * geometry rejects a NaN coordinate at construction and an empty point is skipped.
     */
    private static int compareCoordinates(final Geometry first, final Geometry second) {
        final double[] firstCoordinates = Coordinates.flatten(first);
        final double[] secondCoordinates = Coordinates.flatten(second);
        final int shared = Math.min(firstCoordinates.length, secondCoordinates.length);
        for (int i = 0; i < shared; i++) {
            final double a = firstCoordinates[i];
            final double b = secondCoordinates[i];
            if (a < b) {
                return -1;
            }
            if (a > b) {
                return 1;
            }
        }
        return Integer.compare(firstCoordinates.length, secondCoordinates.length);
    }

    /**
     * Compares the structure of two geometries: the kind, the number of parts and whether it holds
     * parts of its own, for each nested geometry, recursively. The parts of a line string are its
     * points, the parts of a polygon are its rings, and the parts of a collection are its elements,
     * so this covers the part, ring and point counts at every level. A geometry that holds no parts
     * sorts before one that does, which only separates an empty point from a point with a
     * coordinate, since for every other kind holding no parts means a part count of zero.
     */
    private static int compareStructure(final Geometry first, final Geometry second) {
        int result = Integer.compare(first.wkbTypeCode(), second.wkbTypeCode());
        if (result != 0) {
            return result;
        }

        final List<? extends Geometry> firstParts = first.structuralParts();
        final List<? extends Geometry> secondParts = second.structuralParts();
        result = Integer.compare(firstParts.size(), secondParts.size());
        if (result != 0) {
            return result;
        }

        // a node that holds no parts sorts before one that does, at the same kind and part count
        result = Boolean.compare(second.hasNoParts(), first.hasNoParts());
        if (result != 0) {
            return result;
        }

        for (int i = 0; i < firstParts.size(); i++) {
            result = compareStructure(firstParts.get(i), secondParts.get(i));
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }
}

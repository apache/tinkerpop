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

/**
 * Package-private locality key used as the third key of the geometry order. The key is the
 * level-16 Hilbert index of the cell holding the 2D bounding box centre of a geometry, so that
 * geometries which lie near each other on the plane or the globe sort near each other.
 * <p>
 * The key is a 32-bit value. Compare it as unsigned: keys above 2^31 are negative as an int.
 * The grid and curve are specified in gremlin-semantics.asciidoc.
 */
final class HilbertKey {

    /**
     * Bits each axis is quantised to.
     */
    private static final int ORDER = 16;

    /**
     * Cells along one axis.
     */
    private static final int SIDE = 1 << ORDER;

    /**
     * Highest cell index on one axis.
     */
    static final int MAX_CELL = SIDE - 1;

    private HilbertKey() {
    }

    /**
     * Returns the key of a geometry. A geometry that holds no coordinate has key 0; it is empty,
     * and the second key of the order sorts empty geometries before any other, so its key is
     * never reached.
     */
    static int of(final Geometry geometry) {
        final double[] box = Coordinates.boundingBox(geometry);
        if (box == null) {
            return 0;
        }
        final double centreX = normalizeZero((box[0] + box[2]) / 2);
        final double centreY = normalizeZero((box[1] + box[3]) / 2);
        if (geometry.model() == Model.GEOGRAPHIC) {
            return encode(cellForLongitude(centreX), cellForLatitude(centreY));
        }
        return encode(cellForValue(centreX), cellForValue(centreY));
    }

    /**
     * Returns the value with negative zero replaced by positive zero, so that coordinates which
     * are equal get the same key.
     */
    static double normalizeZero(final double value) {
        return value == 0.0 ? 0.0 : value;
    }

    /**
     * Quantises a longitude in [-180, 180] to a cell index, the whole range spread over the axis.
     */
    static int cellForLongitude(final double longitude) {
        return clamp(Math.floor((longitude + 180.0) / 360.0 * SIDE));
    }

    /**
     * Quantises a latitude in [-90, 90] to a cell index, the whole range spread over the axis.
     */
    static int cellForLatitude(final double latitude) {
        return clamp(Math.floor((latitude + 90.0) / 180.0 * SIDE));
    }

    /**
     * Quantises an unbounded coordinate to a cell index, by taking the top 16 bits of the
     * order-preserving 64 bit form of the double: all bits are flipped when the sign bit is set,
     * otherwise only the sign bit is flipped. The mapping is non-decreasing in the coordinate.
     * Negative zero is normalised to positive zero first, so that coordinates which are equal get
     * the same cell.
     */
    static int cellForValue(final double value) {
        long bits = Double.doubleToRawLongBits(normalizeZero(value));
        if (bits < 0) {
            bits = ~bits;
        } else {
            bits ^= Long.MIN_VALUE;
        }
        return (int) (bits >>> (64 - ORDER));
    }

    /**
     * Returns the level-16 Hilbert index of the cell (x, y). Both arguments are cell indexes in
     * 0..65535, and the result is a 32 bit value to be compared as an unsigned integer.
     */
    static int encode(final int x, final int y) {
        int cellX = x;
        int cellY = y;
        long index = 0;
        for (int step = SIDE / 2; step > 0; step /= 2) {
            final int quadrantX = (cellX & step) > 0 ? 1 : 0;
            final int quadrantY = (cellY & step) > 0 ? 1 : 0;
            index += (long) step * step * ((3 * quadrantX) ^ quadrantY);
            // reflect and transpose the cell, so the next finer step is read in the frame the
            // curve enters that quadrant with
            if (quadrantY == 0) {
                if (quadrantX == 1) {
                    cellX = MAX_CELL - cellX;
                    cellY = MAX_CELL - cellY;
                }
                final int transposed = cellX;
                cellX = cellY;
                cellY = transposed;
            }
        }
        return (int) index;
    }

    private static int clamp(final double cell) {
        if (cell <= 0) {
            return 0;
        }
        return cell >= MAX_CELL ? MAX_CELL : (int) cell;
    }
}

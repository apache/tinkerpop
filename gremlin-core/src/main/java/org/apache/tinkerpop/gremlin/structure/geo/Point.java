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

import java.util.Collections;
import java.util.List;

/**
 * A point geometry representing a single location in 2D or 3D space.
 * Points are immutable.
 *
 * @since 4.0.0
 */
public final class Point extends Geometry {

    private static final int WKB_TYPE = 1;

    private final double x;
    private final double y;
    private final double z;
    private final boolean empty;

    /**
     * Package-private constructor for a non-empty 2D point.
     */
    Point(final double x, final double y, final int srid) {
        super(srid, false);
        this.x = x;
        this.y = y;
        this.z = Double.NaN;
        this.empty = false;
    }

    /**
     * Package-private constructor for a non-empty 3D point.
     */
    Point(final double x, final double y, final double z, final int srid) {
        super(srid, true);
        this.x = x;
        this.y = y;
        this.z = z;
        this.empty = false;
    }

    /**
     * Package-private constructor for an empty point.
     */
    Point(final int srid, final boolean hasZ) {
        super(srid, hasZ);
        this.x = Double.NaN;
        this.y = Double.NaN;
        this.z = Double.NaN;
        this.empty = true;
    }

    // ------------------------------------------------------------------------
    // Factory methods
    // ------------------------------------------------------------------------

    /**
     * Creates a 2D point with {@link Model#CARTESIAN} model (SRID 0).
     *
     * @param x the x coordinate
     * @param y the y coordinate
     * @return the point
     * @throws IllegalArgumentException if x or y is NaN or infinite
     * @since 4.0.0
     */
    public static Point of(final double x, final double y) {
        return of(x, y, Model.CARTESIAN);
    }

    /**
     * Creates a 2D point with the specified model.
     *
     * @param x the x coordinate (longitude for {@link Model#GEOGRAPHIC})
     * @param y the y coordinate (latitude for {@link Model#GEOGRAPHIC})
     * @param model {@link Model#CARTESIAN} or {@link Model#GEOGRAPHIC}
     * @return the point
     * @throws IllegalArgumentException if x or y is NaN or infinite, if {@code model} is
     *         {@link Model#GEOGRAPHIC} and x is outside -180 to 180 or y is outside -90 to 90,
     *         or if {@code model} is {@link Model#OTHER} or null
     * @since 4.0.0
     */
    public static Point of(final double x, final double y, final Model model) {
        final int srid = CoordinateValidator.sridForModel(model);
        CoordinateValidator.validateX(x, srid);
        CoordinateValidator.validateY(y, srid);
        return new Point(x, y, srid);
    }

    /**
     * Creates a 3D point with {@link Model#CARTESIAN} model (SRID 0).
     *
     * @param x the x coordinate
     * @param y the y coordinate
     * @param z the z coordinate
     * @return the point
     * @throws IllegalArgumentException if any coordinate is NaN or infinite
     * @since 4.0.0
     */
    public static Point of(final double x, final double y, final double z) {
        return of(x, y, z, Model.CARTESIAN);
    }

    /**
     * Creates a 3D point with the specified model.
     *
     * @param x the x coordinate (longitude for {@link Model#GEOGRAPHIC})
     * @param y the y coordinate (latitude for {@link Model#GEOGRAPHIC})
     * @param z the z coordinate (elevation)
     * @param model {@link Model#CARTESIAN} or {@link Model#GEOGRAPHIC}
     * @return the point
     * @throws IllegalArgumentException if x, y or z is NaN or infinite, if {@code model} is
     *         {@link Model#GEOGRAPHIC} and x is outside -180 to 180 or y is outside -90 to 90,
     *         or if {@code model} is {@link Model#OTHER} or null
     * @since 4.0.0
     */
    public static Point of(final double x, final double y, final double z, final Model model) {
        final int srid = CoordinateValidator.sridForModel(model);
        CoordinateValidator.validateX(x, srid);
        CoordinateValidator.validateY(y, srid);
        CoordinateValidator.validateZ(z);
        return new Point(x, y, z, srid);
    }

    /**
     * Creates an empty 2D {@link Model#CARTESIAN} point.
     *
     * @return an empty point
     * @since 4.0.0
     */
    public static Point empty() {
        return empty(Model.CARTESIAN);
    }

    /**
     * Creates an empty 2D point with the specified model.
     *
     * @param model {@link Model#CARTESIAN} or {@link Model#GEOGRAPHIC}
     * @return an empty point
     * @throws IllegalArgumentException if {@code model} is {@link Model#OTHER} or null
     * @since 4.0.0
     */
    public static Point empty(final Model model) {
        final int srid = CoordinateValidator.sridForModel(model);
        return new Point(srid, false);
    }

    /**
     * Package-private: creates an empty point with explicit hasZ flag.
     */
    static Point emptyWithZ(final int srid, final boolean hasZ) {
        return new Point(srid, hasZ);
    }

    /**
     * Package-private: creates a point with explicit SRID (for WKT/WKB parsing).
     */
    static Point ofWithSrid(final double x, final double y, final int srid) {
        CoordinateValidator.validateX(x, srid);
        CoordinateValidator.validateY(y, srid);
        return new Point(x, y, srid);
    }

    /**
     * Package-private: creates a 3D point with explicit SRID (for WKT/WKB parsing).
     */
    static Point ofWithSrid(final double x, final double y, final double z, final int srid) {
        CoordinateValidator.validateX(x, srid);
        CoordinateValidator.validateY(y, srid);
        CoordinateValidator.validateZ(z);
        return new Point(x, y, z, srid);
    }

    // ------------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------------

    /**
     * Returns the x coordinate (longitude for {@link Model#GEOGRAPHIC}).
     *
     * @return the x coordinate
     * @throws IllegalStateException if this point is empty
     * @since 4.0.0
     */
    public double x() {
        if (empty) {
            throw new IllegalStateException("Cannot get x() from an empty point");
        }
        return x;
    }

    /**
     * Returns the y coordinate (latitude for {@link Model#GEOGRAPHIC}).
     *
     * @return the y coordinate
     * @throws IllegalStateException if this point is empty
     * @since 4.0.0
     */
    public double y() {
        if (empty) {
            throw new IllegalStateException("Cannot get y() from an empty point");
        }
        return y;
    }

    /**
     * Returns the z coordinate, or NaN if this is a 2D point.
     *
     * @return the z coordinate or NaN
     * @throws IllegalStateException if this point is empty
     * @since 4.0.0
     */
    public double z() {
        if (empty) {
            throw new IllegalStateException("Cannot get z() from an empty point");
        }
        return z;
    }

    @Override
    public boolean isEmpty() {
        return empty;
    }

    // ------------------------------------------------------------------------
    // Object methods
    // ------------------------------------------------------------------------

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof Point)) return false;
        final Point other = (Point) o;
        if (srid() != other.srid()) return false;
        if (hasZ() != other.hasZ()) return false;
        if (empty != other.empty) return false;
        if (empty) return true; // both empty with same SRID and hasZ
        // Compare coordinates numerically (so -0.0 == 0.0)
        // Note: Double.compare treats -0.0 < 0.0, but we want them equal
        if (x != other.x) return false;
        if (y != other.y) return false;
        if (hasZ()) {
            return z == other.z;
        }
        return true;
    }

    @Override
    public int hashCode() {
        // Seed 1 and multiplier 31, as in Objects.hash, written out to avoid boxing every coordinate.
        int result = 1;
        result = 31 * result + WKB_TYPE;
        result = 31 * result + srid();
        result = 31 * result + (hasZ() ? 1 : 0);
        if (!empty) {
            result = 31 * result + Long.hashCode(CoordinateValidator.coordinateHashBits(x));
            result = 31 * result + Long.hashCode(CoordinateValidator.coordinateHashBits(y));
            if (hasZ()) {
                result = 31 * result + Long.hashCode(CoordinateValidator.coordinateHashBits(z));
            }
        }
        return result;
    }

    // ------------------------------------------------------------------------
    // Package-private helpers
    // ------------------------------------------------------------------------

    @Override
    int wkbTypeCode() {
        return WKB_TYPE;
    }

    @Override
    List<Geometry> structuralParts() {
        return Collections.emptyList();
    }

    @Override
    boolean hasNoParts() {
        return empty;
    }

    @Override
    String wktTypeName() {
        return "POINT";
    }

    @Override
    void writeWktBody(final StringBuilder sb) {
        if (empty) {
            // an empty point has no coordinates; the writer emits the EMPTY keyword
            return;
        }
        sb.append(WktWriter.formatDouble(x));
        sb.append(' ');
        sb.append(WktWriter.formatDouble(y));
        if (hasZ()) {
            sb.append(' ');
            sb.append(WktWriter.formatDouble(z));
        }
    }
}

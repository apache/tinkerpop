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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A multi-point geometry containing zero or more points.
 * MultiPoints are immutable.
 *
 * @since 4.0.0
 */
public final class MultiPoint extends GeometryCollection {

    private static final int WKB_TYPE = 4;

    private final List<Point> points;

    /**
     * Private constructor. The list of points must already be unmodifiable and not held by the
     * caller.
     */
    private MultiPoint(final List<Point> points, final int srid, final boolean hasZ) {
        super(srid, hasZ);
        this.points = points;
    }

    /**
     * Package-private constructor for empty MultiPoint.
     */
    MultiPoint(final int srid, final boolean hasZ) {
        this(Collections.emptyList(), srid, hasZ);
    }

    // ------------------------------------------------------------------------
    // Factory methods
    // ------------------------------------------------------------------------

    /**
     * Creates a multi-point from an array of points.
     * All points must have the same SRID and z-dimension flag.
     * A multi-point must have at least 1 point; use {@link #empty()} or
     * {@link #empty(Model)} for an empty multi-point. An element may be an empty point.
     *
     * @param points the points, at least 1 of them
     * @return the multi-point
     * @throws IllegalArgumentException if {@code points} is null, if it holds no points, if any
     *         element is null, or if the points do not all have the same SRID and the same z
     *         dimension
     * @since 4.0.0
     */
    public static MultiPoint of(final Point... points) {
        CoordinateValidator.requireNonNullCollection(points, "MultiPoint points");
        if (points.length == 0) {
            throw new IllegalArgumentException("MultiPoint must have at least 1 point;"
                    + " use MultiPoint.empty() or MultiPoint.empty(Model) for an empty multi-point");
        }

        final Point first = points[0];
        CoordinateValidator.requireNonNullElement(first, "MultiPoint points", 0);
        final int srid = first.srid();
        final boolean hasZ = first.hasZ();

        for (int i = 1; i < points.length; i++) {
            final Point p = points[i];
            CoordinateValidator.requireNonNullElement(p, "MultiPoint points", i);
            CoordinateValidator.validateSameSrid(srid, p.srid(), "MultiPoint");
            CoordinateValidator.validateSameHasZ(hasZ, p.hasZ(), "MultiPoint");
        }

        return new MultiPoint(Collections.unmodifiableList(new ArrayList<>(Arrays.asList(points))), srid, hasZ);
    }

    /**
     * Creates an empty multi-point with {@link Model#CARTESIAN} model.
     *
     * @return an empty multi-point
     * @since 4.0.0
     */
    public static MultiPoint empty() {
        return empty(Model.CARTESIAN);
    }

    /**
     * Creates an empty multi-point with the specified model.
     *
     * @param model {@link Model#CARTESIAN} or {@link Model#GEOGRAPHIC}
     * @return an empty multi-point
     * @throws IllegalArgumentException if {@code model} is {@link Model#OTHER} or null
     * @since 4.0.0
     */
    public static MultiPoint empty(final Model model) {
        final int srid = CoordinateValidator.sridForModel(model);
        return new MultiPoint(srid, false);
    }

    /**
     * Package-private: creates an empty MultiPoint with explicit parameters.
     */
    static MultiPoint emptyWithSrid(final int srid, final boolean hasZ) {
        return new MultiPoint(srid, hasZ);
    }

    /**
     * Package-private: creates a MultiPoint with explicit SRID (for WKT/WKB parsing).
     * Takes ownership of the given list, which the caller must not retain or mutate.
     */
    static MultiPoint ofWithSrid(final List<Point> points, final int srid, final boolean hasZ) {
        if (points.isEmpty()) {
            return new MultiPoint(srid, hasZ);
        }

        for (final Point p : points) {
            CoordinateValidator.validateSameSrid(srid, p.srid(), "MultiPoint");
            CoordinateValidator.validateSameHasZ(hasZ, p.hasZ(), "MultiPoint");
        }

        return new MultiPoint(Collections.unmodifiableList(points), srid, hasZ);
    }

    // ------------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------------

    /**
     * Returns an unmodifiable list of the points in this multi-point.
     *
     * @return the points, an empty list when this multi-point holds no parts
     * @since 4.0.0
     */
    @Override
    public List<Point> geometries() {
        // The field is always built as an unmodifiable list, so it can be handed out directly.
        return points;
    }

    @Override
    public boolean isEmpty() {
        // Deep empty check: empty if all points are empty
        if (points.isEmpty()) {
            return true;
        }
        for (final Point p : points) {
            if (!p.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    // ------------------------------------------------------------------------
    // Object methods
    // ------------------------------------------------------------------------

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof MultiPoint)) return false;
        final MultiPoint other = (MultiPoint) o;
        if (srid() != other.srid()) return false;
        if (hasZ() != other.hasZ()) return false;
        return points.equals(other.points);
    }

    @Override
    public int hashCode() {
        int result = 1;   // as in Point.hashCode
        result = 31 * result + WKB_TYPE;
        result = 31 * result + srid();
        result = 31 * result + (hasZ() ? 1 : 0);
        result = 31 * result + points.hashCode();
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
    List<Point> structuralParts() {
        return geometries();
    }

    @Override
    boolean hasNoParts() {
        return points.isEmpty();
    }

    @Override
    String wktTypeName() {
        return "MULTIPOINT";
    }

    @Override
    void writeWktBody(final StringBuilder sb) {
        for (int i = 0; i < points.size(); i++) {
            if (i > 0) sb.append(", ");
            final Point p = points.get(i);
            if (p.hasNoParts()) {
                sb.append("EMPTY");
            } else {
                sb.append('(');
                p.writeWktBody(sb);
                sb.append(')');
            }
        }
    }
}

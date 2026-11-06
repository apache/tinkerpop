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
 * A line string geometry representing a sequence of connected line segments.
 * LineStrings are immutable.
 *
 * @since 4.0.0
 */
public final class LineString extends Curve {

    private static final int WKB_TYPE = 2;

    private final List<Point> points;

    /**
     * Private constructor. The list of points must already be unmodifiable and not held by the
     * caller.
     */
    private LineString(final List<Point> points, final int srid, final boolean hasZ) {
        super(srid, hasZ);
        this.points = points;
    }

    /**
     * Package-private constructor for empty linestring.
     */
    LineString(final int srid, final boolean hasZ) {
        this(Collections.emptyList(), srid, hasZ);
    }

    // ------------------------------------------------------------------------
    // Factory methods
    // ------------------------------------------------------------------------

    /**
     * Creates a line string from an array of points.
     * All points must have the same SRID and z-dimension flag, and none may be empty.
     * A line string must have at least 2 points; use {@link #empty()} or
     * {@link #empty(Model)} for an empty line string.
     *
     * @param points the points, at least 2 of them
     * @return the line string
     * @throws IllegalArgumentException if {@code points} is null, if it holds fewer than 2
     *         points, if any element is null, if any element is an empty point, or if the points
     *         do not all have the same SRID and the same z dimension
     * @since 4.0.0
     */
    public static LineString of(final Point... points) {
        CoordinateValidator.requireNonNullCollection(points, "LineString points");
        if (points.length == 0) {
            throw new IllegalArgumentException("LineString must have at least 2 points, got 0;"
                    + " use LineString.empty() for an empty line string");
        }
        if (points.length == 1) {
            throw new IllegalArgumentException("LineString must have at least 2 points, got 1");
        }

        final Point first = points[0];
        CoordinateValidator.validateVertex(first, "LineString", 0);
        final int srid = first.srid();
        final boolean hasZ = first.hasZ();

        for (int i = 1; i < points.length; i++) {
            final Point p = points[i];
            CoordinateValidator.validateVertex(p, "LineString", i);
            CoordinateValidator.validateSameSrid(srid, p.srid(), "LineString");
            CoordinateValidator.validateSameHasZ(hasZ, p.hasZ(), "LineString");
        }

        return new LineString(Collections.unmodifiableList(new ArrayList<>(Arrays.asList(points))), srid, hasZ);
    }

    /**
     * Creates an empty line string with {@link Model#CARTESIAN} model.
     *
     * @return an empty line string
     * @since 4.0.0
     */
    public static LineString empty() {
        return empty(Model.CARTESIAN);
    }

    /**
     * Creates an empty line string with the specified model.
     *
     * @param model {@link Model#CARTESIAN} or {@link Model#GEOGRAPHIC}
     * @return an empty line string
     * @throws IllegalArgumentException if {@code model} is {@link Model#OTHER} or null
     * @since 4.0.0
     */
    public static LineString empty(final Model model) {
        final int srid = CoordinateValidator.sridForModel(model);
        return new LineString(srid, false);
    }

    /**
     * Package-private: creates an empty linestring with explicit parameters.
     */
    static LineString emptyWithSrid(final int srid, final boolean hasZ) {
        return new LineString(srid, hasZ);
    }

    /**
     * Package-private: creates a linestring with explicit SRID (for WKT/WKB parsing).
     * Takes ownership of the given list, which the caller must not retain or mutate.
     */
    static LineString ofWithSrid(final List<Point> points, final int srid, final boolean hasZ) {
        if (points.isEmpty()) {
            return new LineString(srid, hasZ);
        }

        if (points.size() == 1) {
            throw new IllegalArgumentException("LineString must have at least 2 points, got 1");
        }

        for (int i = 0; i < points.size(); i++) {
            final Point p = points.get(i);
            CoordinateValidator.validateVertex(p, "LineString", i);
            CoordinateValidator.validateSameSrid(srid, p.srid(), "LineString");
            CoordinateValidator.validateSameHasZ(hasZ, p.hasZ(), "LineString");
        }

        return new LineString(Collections.unmodifiableList(points), srid, hasZ);
    }

    // ------------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------------

    /**
     * Returns an unmodifiable list of points in this line string.
     *
     * @return the points, an empty list when this line string is empty
     * @since 4.0.0
     */
    public List<Point> points() {
        return points;
    }

    @Override
    public boolean isEmpty() {
        return points.isEmpty();
    }

    // ------------------------------------------------------------------------
    // Object methods
    // ------------------------------------------------------------------------

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof LineString)) return false;
        final LineString other = (LineString) o;
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
        return points;
    }

    @Override
    boolean hasNoParts() {
        return points.isEmpty();
    }

    @Override
    String wktTypeName() {
        return "LINESTRING";
    }

    @Override
    void writeWktBody(final StringBuilder sb) {
        if (points.isEmpty()) {
            return;
        }
        for (int i = 0; i < points.size(); i++) {
            if (i > 0) sb.append(", ");
            points.get(i).writeWktBody(sb);
        }
    }
}

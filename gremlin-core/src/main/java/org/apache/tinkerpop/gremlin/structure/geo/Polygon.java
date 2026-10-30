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
 * A polygon geometry representing an area enclosed by a shell with optional holes.
 * Polygons are immutable. Each ring must be closed (first and last points equal)
 * and have at least 4 points.
 *
 * @since 4.0.0
 */
public final class Polygon extends Surface {

    private static final int WKB_TYPE = 3;

    private final LineString shell;
    private final List<LineString> holes;

    /**
     * Private constructor. The list of holes must already be unmodifiable and not held by the
     * caller.
     */
    private Polygon(final LineString shell, final List<LineString> holes, final int srid, final boolean hasZ) {
        super(srid, hasZ);
        this.shell = shell;
        this.holes = holes;
    }

    /**
     * Package-private constructor for empty polygon.
     */
    Polygon(final int srid, final boolean hasZ) {
        this(LineString.emptyWithSrid(srid, hasZ), Collections.emptyList(), srid, hasZ);
    }

    // ------------------------------------------------------------------------
    // Factory methods
    // ------------------------------------------------------------------------

    /**
     * Creates a polygon from a shell ring and optional hole rings.
     * All rings must be closed (first and last point equal) and have at least 4 points.
     * All rings must have the same SRID and z-dimension flag. A hole ring must not be empty,
     * and an empty shell is only valid for a polygon with no holes.
     *
     * @param shell the exterior ring
     * @param holes the interior rings (holes)
     * @return the polygon
     * @throws IllegalArgumentException if {@code shell} is null, if {@code holes} is null or
     *         holds a null element, if a ring that is not empty has fewer than 4 points or does
     *         not have equal first and last points, if any hole is empty, if {@code shell} is
     *         empty and any hole is given, or if the rings do not all have the same SRID and the
     *         same z dimension
     * @since 4.0.0
     */
    public static Polygon of(final LineString shell, final LineString... holes) {
        if (shell == null) {
            throw new IllegalArgumentException("Shell ring cannot be null");
        }
        CoordinateValidator.requireNonNullCollection(holes, "Polygon holes");

        CoordinateValidator.validateRing(shell, "Shell ring");

        final int srid = shell.srid();
        final boolean hasZ = shell.hasZ();

        final List<LineString> holeList = Arrays.asList(holes);
        validateHoles(shell, holeList, srid, hasZ);

        return new Polygon(shell, Collections.unmodifiableList(new ArrayList<>(holeList)), srid, hasZ);
    }

    /**
     * Validates the hole rings of a polygon against its shell.
     */
    private static void validateHoles(final LineString shell, final List<LineString> holes,
                                      final int srid, final boolean hasZ) {
        for (int i = 0; i < holes.size(); i++) {
            final LineString hole = holes.get(i);
            CoordinateValidator.requireNonNullElement(hole, "Polygon holes", i);
            if (hole.isEmpty()) {
                throw new IllegalArgumentException("Hole ring at index " + i + " cannot be empty");
            }
            if (shell.isEmpty()) {
                throw new IllegalArgumentException("Polygon with an empty shell cannot have holes");
            }
            CoordinateValidator.validateSameSrid(srid, hole.srid(), "Polygon");
            CoordinateValidator.validateSameHasZ(hasZ, hole.hasZ(), "Polygon");
            CoordinateValidator.validateRing(hole, "Hole ring");
        }
    }

    /**
     * Creates an empty polygon with {@link Model#CARTESIAN} model.
     *
     * @return an empty polygon
     * @since 4.0.0
     */
    public static Polygon empty() {
        return empty(Model.CARTESIAN);
    }

    /**
     * Creates an empty polygon with the specified model.
     *
     * @param model {@link Model#CARTESIAN} or {@link Model#GEOGRAPHIC}
     * @return an empty polygon
     * @throws IllegalArgumentException if {@code model} is {@link Model#OTHER} or null
     * @since 4.0.0
     */
    public static Polygon empty(final Model model) {
        final int srid = CoordinateValidator.sridForModel(model);
        return new Polygon(srid, false);
    }

    /**
     * Package-private: creates an empty polygon with explicit parameters.
     */
    static Polygon emptyWithSrid(final int srid, final boolean hasZ) {
        return new Polygon(srid, hasZ);
    }

    /**
     * Package-private: creates a polygon with explicit SRID (for WKT/WKB parsing).
     * Takes ownership of the given hole list, which the caller must not retain or mutate.
     */
    static Polygon ofWithSrid(final LineString shell, final List<LineString> holes, final int srid, final boolean hasZ) {
        if (shell.isEmpty() && holes.isEmpty()) {
            return new Polygon(srid, hasZ);
        }

        if (!shell.isEmpty()) {
            CoordinateValidator.validateRing(shell, "Shell ring");
            CoordinateValidator.validateSameSrid(srid, shell.srid(), "Polygon");
            CoordinateValidator.validateSameHasZ(hasZ, shell.hasZ(), "Polygon");
        }

        validateHoles(shell, holes, srid, hasZ);

        return new Polygon(shell, Collections.unmodifiableList(holes), srid, hasZ);
    }

    // ------------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------------

    /**
     * Returns the exterior ring (shell) of this polygon.
     *
     * @return the exterior ring, an empty line string when this polygon is empty
     * @since 4.0.0
     */
    public LineString exteriorRing() {
        return shell;
    }

    /**
     * Returns an unmodifiable list of interior rings (holes).
     *
     * @return the holes, an empty list when this polygon has no holes
     * @since 4.0.0
     */
    public List<LineString> interiorRings() {
        return holes;
    }

    @Override
    public boolean isEmpty() {
        return shell.isEmpty();
    }

    // ------------------------------------------------------------------------
    // Object methods
    // ------------------------------------------------------------------------

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof Polygon)) return false;
        final Polygon other = (Polygon) o;
        if (srid() != other.srid()) return false;
        if (hasZ() != other.hasZ()) return false;
        if (!shell.equals(other.shell)) return false;
        return holes.equals(other.holes);
    }

    @Override
    public int hashCode() {
        int result = 1;   // as in Point.hashCode
        result = 31 * result + WKB_TYPE;
        result = 31 * result + srid();
        result = 31 * result + (hasZ() ? 1 : 0);
        result = 31 * result + shell.hashCode();
        result = 31 * result + holes.hashCode();
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
    List<LineString> structuralParts() {
        if (shell.hasNoParts()) {
            return Collections.emptyList();
        }
        if (holes.isEmpty()) {
            return Collections.singletonList(shell);
        }
        final List<LineString> rings = new ArrayList<>(1 + holes.size());
        rings.add(shell);
        rings.addAll(holes);
        return Collections.unmodifiableList(rings);
    }

    @Override
    boolean hasNoParts() {
        return shell.hasNoParts();
    }

    @Override
    String wktTypeName() {
        return "POLYGON";
    }

    @Override
    void writeWktBody(final StringBuilder sb) {
        if (shell.hasNoParts()) {
            return;
        }
        // Write shell
        sb.append('(');
        shell.writeWktBody(sb);
        sb.append(')');
        // Write holes
        for (final LineString hole : holes) {
            sb.append(", (");
            hole.writeWktBody(sb);
            sb.append(')');
        }
    }

    /**
     * Package-private: returns the total number of rings (1 shell + holes).
     */
    int ringCount() {
        if (shell.hasNoParts()) {
            return 0;
        }
        return 1 + holes.size();
    }
}

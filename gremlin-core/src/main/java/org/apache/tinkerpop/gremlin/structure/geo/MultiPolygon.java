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
 * A multi-polygon geometry containing zero or more polygons.
 * MultiPolygons are immutable.
 *
 * @since 4.0.0
 */
public final class MultiPolygon extends MultiSurface {

    private static final int WKB_TYPE = 6;

    private final List<Polygon> polygons;

    /**
     * Private constructor. The list of polygons must already be unmodifiable and not held by the
     * caller.
     */
    private MultiPolygon(final List<Polygon> polygons, final int srid, final boolean hasZ) {
        super(srid, hasZ);
        this.polygons = polygons;
    }

    /**
     * Package-private constructor for empty MultiPolygon.
     */
    MultiPolygon(final int srid, final boolean hasZ) {
        this(Collections.emptyList(), srid, hasZ);
    }

    // ------------------------------------------------------------------------
    // Factory methods
    // ------------------------------------------------------------------------

    /**
     * Creates a multi-polygon from an array of polygons.
     * All polygons must have the same SRID and z-dimension flag.
     * A multi-polygon must have at least 1 polygon; use {@link #empty()} or
     * {@link #empty(Model)} for an empty multi-polygon. An element may be an empty polygon.
     *
     * @param polygons the polygons, at least 1 of them
     * @return the multi-polygon
     * @throws IllegalArgumentException if {@code polygons} is null, if it holds no polygons, if
     *         any element is null, or if the polygons do not all have the same SRID and the same
     *         z dimension
     * @since 4.0.0
     */
    public static MultiPolygon of(final Polygon... polygons) {
        CoordinateValidator.requireNonNullCollection(polygons, "MultiPolygon polygons");
        if (polygons.length == 0) {
            throw new IllegalArgumentException("MultiPolygon must have at least 1 polygon;"
                    + " use MultiPolygon.empty() or MultiPolygon.empty(Model) for an empty multi-polygon");
        }

        final Polygon first = polygons[0];
        CoordinateValidator.requireNonNullElement(first, "MultiPolygon polygons", 0);
        final int srid = first.srid();
        final boolean hasZ = first.hasZ();

        for (int i = 1; i < polygons.length; i++) {
            final Polygon p = polygons[i];
            CoordinateValidator.requireNonNullElement(p, "MultiPolygon polygons", i);
            CoordinateValidator.validateSameSrid(srid, p.srid(), "MultiPolygon");
            CoordinateValidator.validateSameHasZ(hasZ, p.hasZ(), "MultiPolygon");
        }

        return new MultiPolygon(Collections.unmodifiableList(new ArrayList<>(Arrays.asList(polygons))), srid, hasZ);
    }

    /**
     * Creates an empty multi-polygon with {@link Model#CARTESIAN} model.
     *
     * @return an empty multi-polygon
     * @since 4.0.0
     */
    public static MultiPolygon empty() {
        return empty(Model.CARTESIAN);
    }

    /**
     * Creates an empty multi-polygon with the specified model.
     *
     * @param model {@link Model#CARTESIAN} or {@link Model#GEOGRAPHIC}
     * @return an empty multi-polygon
     * @throws IllegalArgumentException if {@code model} is {@link Model#OTHER} or null
     * @since 4.0.0
     */
    public static MultiPolygon empty(final Model model) {
        final int srid = CoordinateValidator.sridForModel(model);
        return new MultiPolygon(srid, false);
    }

    /**
     * Package-private: creates an empty MultiPolygon with explicit parameters.
     */
    static MultiPolygon emptyWithSrid(final int srid, final boolean hasZ) {
        return new MultiPolygon(srid, hasZ);
    }

    /**
     * Package-private: creates a MultiPolygon with explicit SRID (for WKT/WKB parsing).
     * Takes ownership of the given list, which the caller must not retain or mutate.
     */
    static MultiPolygon ofWithSrid(final List<Polygon> polygons, final int srid, final boolean hasZ) {
        if (polygons.isEmpty()) {
            return new MultiPolygon(srid, hasZ);
        }

        for (final Polygon p : polygons) {
            CoordinateValidator.validateSameSrid(srid, p.srid(), "MultiPolygon");
            CoordinateValidator.validateSameHasZ(hasZ, p.hasZ(), "MultiPolygon");
        }

        return new MultiPolygon(Collections.unmodifiableList(polygons), srid, hasZ);
    }

    // ------------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------------

    /**
     * Returns an unmodifiable list of the polygons in this multi-polygon.
     *
     * @return the polygons, an empty list when this multi-polygon holds no parts
     * @since 4.0.0
     */
    @Override
    public List<Polygon> geometries() {
        // The field is always built as an unmodifiable list, so it can be handed out directly.
        return polygons;
    }

    @Override
    public boolean isEmpty() {
        // Deep empty check: empty if all polygons are empty
        if (polygons.isEmpty()) {
            return true;
        }
        for (final Polygon p : polygons) {
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
        if (!(o instanceof MultiPolygon)) return false;
        final MultiPolygon other = (MultiPolygon) o;
        if (srid() != other.srid()) return false;
        if (hasZ() != other.hasZ()) return false;
        return polygons.equals(other.polygons);
    }

    @Override
    public int hashCode() {
        int result = 1;   // as in Point.hashCode
        result = 31 * result + WKB_TYPE;
        result = 31 * result + srid();
        result = 31 * result + (hasZ() ? 1 : 0);
        result = 31 * result + polygons.hashCode();
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
    List<Polygon> structuralParts() {
        return geometries();
    }

    @Override
    boolean hasNoParts() {
        return polygons.isEmpty();
    }

    @Override
    String wktTypeName() {
        return "MULTIPOLYGON";
    }

    @Override
    void writeWktBody(final StringBuilder sb) {
        for (int i = 0; i < polygons.size(); i++) {
            if (i > 0) sb.append(", ");
            final Polygon p = polygons.get(i);
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

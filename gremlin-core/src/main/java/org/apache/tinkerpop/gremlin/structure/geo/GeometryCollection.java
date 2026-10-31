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
 * A geometry collection containing any mix of geometry types.
 * GeometryCollections are immutable.
 *
 * @since 4.0.0
 */
public class GeometryCollection extends Geometry {

    private static final int WKB_TYPE = 7;

    private final List<Geometry> geometries;

    /**
     * Private constructor. The list of geometries must already be unmodifiable and not held by
     * the caller.
     */
    private GeometryCollection(final List<Geometry> geometries, final int srid, final boolean hasZ) {
        super(srid, hasZ);
        this.geometries = geometries;
    }

    /**
     * Package-private constructor for an empty collection, and for subclasses that hold
     * their parts in a list of their own element type.
     */
    GeometryCollection(final int srid, final boolean hasZ) {
        this(Collections.emptyList(), srid, hasZ);
    }

    // ------------------------------------------------------------------------
    // Factory methods
    // ------------------------------------------------------------------------

    /**
     * Creates a geometry collection from an array of geometries.
     * All geometries must have the same SRID and z-dimension flag.
     * A collection must have at least 1 geometry; use {@link #empty()} or
     * {@link #empty(Model)} for an empty collection. An element may be empty, and may itself be
     * a collection as long as the nesting limit is respected.
     *
     * @param geometries the geometries, at least 1 of them
     * @return the collection
     * @throws IllegalArgumentException if {@code geometries} is null, if it holds no geometries,
     *         if any element is null, if the geometries do not all have the same SRID and the
     *         same z dimension, or if this collection taken together with its deepest nested
     *         element would exceed 32 levels of collection nesting, where every collection,
     *         including a {@link MultiPoint}, {@link MultiLineString} or {@link MultiPolygon},
     *         counts as one level
     * @since 4.0.0
     */
    public static GeometryCollection of(final Geometry... geometries) {
        CoordinateValidator.requireNonNullCollection(geometries, "GeometryCollection geometries");
        if (geometries.length == 0) {
            throw new IllegalArgumentException("GeometryCollection must have at least 1 geometry;"
                    + " use GeometryCollection.empty() or GeometryCollection.empty(Model)"
                    + " for an empty geometry collection");
        }

        final Geometry first = geometries[0];
        CoordinateValidator.requireNonNullElement(first, "GeometryCollection geometries", 0);
        final int srid = first.srid();
        final boolean hasZ = first.hasZ();

        int maxChildDepth = 0;
        for (int i = 0; i < geometries.length; i++) {
            final Geometry g = geometries[i];
            CoordinateValidator.requireNonNullElement(g, "GeometryCollection geometries", i);
            CoordinateValidator.validateSameSrid(srid, g.srid(), "GeometryCollection");
            CoordinateValidator.validateSameHasZ(hasZ, g.hasZ(), "GeometryCollection");
            maxChildDepth = Math.max(maxChildDepth, CoordinateValidator.computeDepth(g));
        }

        // This level plus the deepest child must stay within the nesting limit
        CoordinateValidator.validateNestingDepth(1 + maxChildDepth);

        return new GeometryCollection(Collections.unmodifiableList(new ArrayList<>(Arrays.asList(geometries))),
                srid, hasZ);
    }

    /**
     * Creates an empty geometry collection with {@link Model#CARTESIAN} model.
     *
     * @return an empty collection
     * @since 4.0.0
     */
    public static GeometryCollection empty() {
        return empty(Model.CARTESIAN);
    }

    /**
     * Creates an empty geometry collection with the specified model.
     *
     * @param model {@link Model#CARTESIAN} or {@link Model#GEOGRAPHIC}
     * @return an empty collection
     * @throws IllegalArgumentException if {@code model} is {@link Model#OTHER} or null
     * @since 4.0.0
     */
    public static GeometryCollection empty(final Model model) {
        final int srid = CoordinateValidator.sridForModel(model);
        return new GeometryCollection(srid, false);
    }

    /**
     * Package-private: creates an empty collection with explicit parameters.
     */
    static GeometryCollection emptyWithSrid(final int srid, final boolean hasZ) {
        return new GeometryCollection(srid, hasZ);
    }

    /**
     * Package-private: creates a collection with explicit SRID (for WKT/WKB parsing).
     * Takes ownership of the given list, which the caller must not retain or mutate.
     */
    static GeometryCollection createWithSrid(final List<Geometry> geometries, final int srid, final boolean hasZ) {
        if (geometries.isEmpty()) {
            return new GeometryCollection(srid, hasZ);
        }

        for (final Geometry g : geometries) {
            CoordinateValidator.validateSameSrid(srid, g.srid(), "GeometryCollection");
            CoordinateValidator.validateSameHasZ(hasZ, g.hasZ(), "GeometryCollection");
        }

        return new GeometryCollection(Collections.unmodifiableList(geometries), srid, hasZ);
    }

    // ------------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------------

    /**
     * Returns an unmodifiable list of geometries in this collection. Subclasses that admit only
     * one kind of part narrow the element type: {@link MultiPoint#geometries()} returns
     * {@code List<Point>}, {@link MultiLineString#geometries()} returns {@code List<LineString>}
     * and {@link MultiPolygon#geometries()} returns {@code List<Polygon>}.
     *
     * @return the geometries as a list of {@link Geometry} or a narrower element type, an empty
     *         list when this collection holds no parts
     * @since 4.0.0
     */
    public List<? extends Geometry> geometries() {
        return geometries;
    }

    @Override
    public boolean isEmpty() {
        // Deep empty check: collection is empty if all parts are empty
        if (geometries.isEmpty()) {
            return true;
        }
        for (final Geometry g : geometries) {
            if (!g.isEmpty()) {
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
        if (o == null || getClass() != o.getClass()) return false;
        final GeometryCollection other = (GeometryCollection) o;
        if (srid() != other.srid()) return false;
        if (hasZ() != other.hasZ()) return false;
        return geometries.equals(other.geometries);
    }

    @Override
    public int hashCode() {
        int result = 1;   // as in Point.hashCode
        result = 31 * result + wkbTypeCode();
        result = 31 * result + srid();
        result = 31 * result + (hasZ() ? 1 : 0);
        result = 31 * result + geometries.hashCode();
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
    List<? extends Geometry> structuralParts() {
        // The wildcard stays here so that subclasses holding one kind of part can narrow the
        // element type: List<Point> is a subtype of List<? extends Geometry>, not of List<Geometry>.
        return geometries;
    }

    @Override
    boolean hasNoParts() {
        return geometries.isEmpty();
    }

    @Override
    String wktTypeName() {
        return "GEOMETRYCOLLECTION";
    }

    @Override
    void writeWktBody(final StringBuilder sb) {
        for (int i = 0; i < geometries.size(); i++) {
            if (i > 0) sb.append(", ");
            WktWriter.writeTagged(sb, geometries.get(i));
        }
    }
}

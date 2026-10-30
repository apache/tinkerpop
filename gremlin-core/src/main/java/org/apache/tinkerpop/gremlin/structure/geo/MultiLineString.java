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
 * A multi-line string geometry containing zero or more line strings.
 * MultiLineStrings are immutable.
 *
 * @since 4.0.0
 */
public final class MultiLineString extends MultiCurve {

    private static final int WKB_TYPE = 5;

    private final List<LineString> lineStrings;

    /**
     * Private constructor. The list of line strings must already be unmodifiable and not held by
     * the caller.
     */
    private MultiLineString(final List<LineString> lineStrings, final int srid, final boolean hasZ) {
        super(srid, hasZ);
        this.lineStrings = lineStrings;
    }

    /**
     * Package-private constructor for empty MultiLineString.
     */
    MultiLineString(final int srid, final boolean hasZ) {
        this(Collections.emptyList(), srid, hasZ);
    }

    // ------------------------------------------------------------------------
    // Factory methods
    // ------------------------------------------------------------------------

    /**
     * Creates a multi-line string from an array of line strings.
     * All line strings must have the same SRID and z-dimension flag.
     * A multi-line string must have at least 1 line string; use {@link #empty()} or
     * {@link #empty(Model)} for an empty multi-line string. An element may be an empty line
     * string.
     *
     * @param lineStrings the line strings, at least 1 of them
     * @return the multi-line string
     * @throws IllegalArgumentException if {@code lineStrings} is null, if it holds no line
     *         strings, if any element is null, or if the line strings do not all have the same
     *         SRID and the same z dimension
     * @since 4.0.0
     */
    public static MultiLineString of(final LineString... lineStrings) {
        CoordinateValidator.requireNonNullCollection(lineStrings, "MultiLineString line strings");
        if (lineStrings.length == 0) {
            throw new IllegalArgumentException("MultiLineString must have at least 1 line string;"
                    + " use MultiLineString.empty() or MultiLineString.empty(Model)"
                    + " for an empty multi-line string");
        }

        final LineString first = lineStrings[0];
        CoordinateValidator.requireNonNullElement(first, "MultiLineString line strings", 0);
        final int srid = first.srid();
        final boolean hasZ = first.hasZ();

        for (int i = 1; i < lineStrings.length; i++) {
            final LineString ls = lineStrings[i];
            CoordinateValidator.requireNonNullElement(ls, "MultiLineString line strings", i);
            CoordinateValidator.validateSameSrid(srid, ls.srid(), "MultiLineString");
            CoordinateValidator.validateSameHasZ(hasZ, ls.hasZ(), "MultiLineString");
        }

        return new MultiLineString(Collections.unmodifiableList(new ArrayList<>(Arrays.asList(lineStrings))),
                srid, hasZ);
    }

    /**
     * Creates an empty multi-line string with {@link Model#CARTESIAN} model.
     *
     * @return an empty multi-line string
     * @since 4.0.0
     */
    public static MultiLineString empty() {
        return empty(Model.CARTESIAN);
    }

    /**
     * Creates an empty multi-line string with the specified model.
     *
     * @param model {@link Model#CARTESIAN} or {@link Model#GEOGRAPHIC}
     * @return an empty multi-line string
     * @throws IllegalArgumentException if {@code model} is {@link Model#OTHER} or null
     * @since 4.0.0
     */
    public static MultiLineString empty(final Model model) {
        final int srid = CoordinateValidator.sridForModel(model);
        return new MultiLineString(srid, false);
    }

    /**
     * Package-private: creates an empty MultiLineString with explicit parameters.
     */
    static MultiLineString emptyWithSrid(final int srid, final boolean hasZ) {
        return new MultiLineString(srid, hasZ);
    }

    /**
     * Package-private: creates a MultiLineString with explicit SRID (for WKT/WKB parsing).
     * Takes ownership of the given list, which the caller must not retain or mutate.
     */
    static MultiLineString ofWithSrid(final List<LineString> lineStrings, final int srid, final boolean hasZ) {
        if (lineStrings.isEmpty()) {
            return new MultiLineString(srid, hasZ);
        }

        for (final LineString ls : lineStrings) {
            CoordinateValidator.validateSameSrid(srid, ls.srid(), "MultiLineString");
            CoordinateValidator.validateSameHasZ(hasZ, ls.hasZ(), "MultiLineString");
        }

        return new MultiLineString(Collections.unmodifiableList(lineStrings), srid, hasZ);
    }

    // ------------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------------

    /**
     * Returns an unmodifiable list of the line strings in this multi-line string.
     *
     * @return the line strings, an empty list when this multi-line string holds no parts
     * @since 4.0.0
     */
    @Override
    public List<LineString> geometries() {
        // The field is always built as an unmodifiable list, so it can be handed out directly.
        return lineStrings;
    }

    @Override
    public boolean isEmpty() {
        // Deep empty check: empty if all line strings are empty
        if (lineStrings.isEmpty()) {
            return true;
        }
        for (final LineString ls : lineStrings) {
            if (!ls.isEmpty()) {
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
        if (!(o instanceof MultiLineString)) return false;
        final MultiLineString other = (MultiLineString) o;
        if (srid() != other.srid()) return false;
        if (hasZ() != other.hasZ()) return false;
        return lineStrings.equals(other.lineStrings);
    }

    @Override
    public int hashCode() {
        int result = 1;   // as in Point.hashCode
        result = 31 * result + WKB_TYPE;
        result = 31 * result + srid();
        result = 31 * result + (hasZ() ? 1 : 0);
        result = 31 * result + lineStrings.hashCode();
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
        return geometries();
    }

    @Override
    boolean hasNoParts() {
        return lineStrings.isEmpty();
    }

    @Override
    String wktTypeName() {
        return "MULTILINESTRING";
    }

    @Override
    void writeWktBody(final StringBuilder sb) {
        for (int i = 0; i < lineStrings.size(); i++) {
            if (i > 0) sb.append(", ");
            final LineString ls = lineStrings.get(i);
            if (ls.hasNoParts()) {
                sb.append("EMPTY");
            } else {
                sb.append('(');
                ls.writeWktBody(sb);
                sb.append(')');
            }
        }
    }
}

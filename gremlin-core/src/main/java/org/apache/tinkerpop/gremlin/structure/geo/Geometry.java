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
 * Base class for all geometry types. Geometry values are immutable.
 * <p>
 * Construction from standard formats:
 * <ul>
 *   <li>{@link #fromWkt(String)} - parses WKT or EWKT; model from EWKT prefix or CARTESIAN</li>
 *   <li>{@link #fromWkt(String, Model)} - parses WKT with explicit model</li>
 *   <li>{@link #fromWkt(String, int)} - parses WKT with explicit SRID</li>
 *   <li>{@link #fromWkb(byte[], int)} - parses ISO WKB with explicit SRID</li>
 * </ul>
 * <p>
 * Output formats:
 * <ul>
 *   <li>{@link #toWKT()} - ISO WKT without SRID</li>
 *   <li>{@link #toEWKT()} - EWKT with "SRID=n;" prefix</li>
 *   <li>{@link #toWKB()} - ISO WKB in big-endian byte order</li>
 * </ul>
 * <p>
 * <b>Order.</b> Geometries have a total natural ordering that is consistent with
 * {@link #equals(Object)}, defined by {@link #compareTo(Geometry)}.
 *
 * @since 4.0.0
 */
public abstract class Geometry implements Comparable<Geometry> {

    /**
     * Marks the Hilbert key as not yet computed. A computed key is held as an unsigned 32 bit
     * value, so it is never negative.
     */
    private static final long HILBERT_KEY_ABSENT = -1L;

    private final int srid;
    private final boolean hasZ;

    /**
     * The Hilbert key of this value, computed on first use. The value is immutable, so a race
     * between two threads computes the same key.
     */
    private volatile long hilbertKeyCache = HILBERT_KEY_ABSENT;

    /**
     * Package-private constructor.
     */
    Geometry(final int srid, final boolean hasZ) {
        this.srid = srid;
        this.hasZ = hasZ;
    }

    // ------------------------------------------------------------------------
    // Factory methods
    // ------------------------------------------------------------------------

    /**
     * Parses WKT or EWKT text. If an EWKT prefix is present (for example
     * {@code "SRID=4326;POINT(1 2)"}), the SRID and so the model are derived from it. Otherwise
     * the model is {@link Model#CARTESIAN}.
     *
     * @param wktOrEwkt the WKT or EWKT string
     * @return the parsed geometry
     * @throws IllegalArgumentException if {@code wktOrEwkt} is null, empty or only whitespace;
     *         if an {@code SRID=} prefix has no digits, has more than 10 digits, holds a value
     *         outside the range of a 32-bit integer, or is not followed by {@code ';'};
     *         if the geometry type name is unknown, or is one of the unsupported types
     *         {@code CIRCULARSTRING}, {@code COMPOUNDCURVE}, {@code CURVEPOLYGON},
     *         {@code MULTICURVE}, {@code MULTISURFACE}, {@code TRIANGLE}, {@code TIN} and
     *         {@code POLYHEDRALSURFACE}; if an {@code M} or {@code ZM} dimension tag is present;
     *         if a coordinate holds {@code NaN} or {@code Infinity}, is not a number, or has a
     *         third ordinate without a {@code Z} tag; if a parenthesis, comma or other delimiter
     *         is missing, or text follows the geometry; if the parts of one value do not all
     *         carry the same {@code Z} tag; if a line string has exactly 1 point; if a polygon
     *         ring has fewer than 4 points or is not closed; if collection nesting is deeper than
     *         32 levels; or if the SRID resolves to 4326 and an x is outside -180 to 180 or a y
     *         is outside -90 to 90
     * @since 4.0.0
     */
    public static Geometry fromWkt(final String wktOrEwkt) {
        return WktReader.parse(wktOrEwkt, null, null);
    }

    /**
     * Parses WKT text with an explicit model. An EWKT {@code SRID=} prefix is accepted only when
     * its SRID is the SRID of {@code model}, which is 0 for {@link Model#CARTESIAN} and 4326 for
     * {@link Model#GEOGRAPHIC}.
     *
     * @param wkt the WKT string
     * @param model {@link Model#CARTESIAN} or {@link Model#GEOGRAPHIC}
     * @return the parsed geometry
     * @throws IllegalArgumentException if {@code model} is null or {@link Model#OTHER}; if the
     *         text carries an EWKT {@code SRID=} prefix whose SRID is not the SRID of
     *         {@code model}; or if the text is malformed in any of the ways listed on
     *         {@link #fromWkt(String)}, which with {@link Model#GEOGRAPHIC} includes an x outside
     *         -180 to 180 or a y outside -90 to 90
     * @since 4.0.0
     */
    public static Geometry fromWkt(final String wkt, final Model model) {
        if (model == null) {
            throw new IllegalArgumentException("Model cannot be null; use fromWkt(wkt) for the default CARTESIAN model");
        }
        if (model == Model.OTHER) {
            throw new IllegalArgumentException("Model.OTHER cannot be used in constructors; use fromWkt(wkt, srid) with an explicit SRID");
        }
        return WktReader.parse(wkt, model, null);
    }

    /**
     * Parses WKT text with an explicit SRID. An EWKT {@code SRID=} prefix is accepted only when
     * its SRID equals {@code srid}.
     *
     * @param wkt the WKT string
     * @param srid the spatial reference ID, applied as given and not itself validated
     * @return the parsed geometry
     * @throws IllegalArgumentException if the text carries an EWKT {@code SRID=} prefix whose
     *         SRID is not {@code srid}, or if the text is malformed in any of the ways listed on
     *         {@link #fromWkt(String)}, which when {@code srid} is 4326 includes an x outside
     *         -180 to 180 or a y outside -90 to 90
     * @since 4.0.0
     */
    public static Geometry fromWkt(final String wkt, final int srid) {
        return WktReader.parse(wkt, null, srid);
    }

    /**
     * Parses ISO WKB bytes with an explicit SRID.
     * <p>
     * This method accepts ISO WKB only. EWKB (PostGIS extended format with
     * flag bits 0x80000000, 0x40000000, or 0x20000000 in the type code) is rejected.
     * <p>
     * ISO WKB has no length prefix for a point, so a point whose x and y are both {@code NaN}
     * reads as an empty point. Such a point is accepted on its own and as an element of a
     * {@link MultiPoint}, but not as a vertex of a {@link LineString} or a polygon ring. Its z
     * ordinate, if the type code carries a Z, is read and discarded without being checked.
     *
     * @param wkb the ISO WKB bytes
     * @param srid the spatial reference ID, applied as given and not itself validated
     * @return the parsed geometry
     * @throws IllegalArgumentException if {@code wkb} is null or zero length; if a byte order
     *         marker is neither {@code 0x00} for big-endian nor {@code 0x01} for little-endian;
     *         if a type code carries an EWKB flag bit, {@code 0x80000000} for Z,
     *         {@code 0x40000000} for M or {@code 0x20000000} for SRID; if a type code is in the
     *         M range 2001 to 2007 or the ZM range 3001 to 3007; if a type code is outside
     *         1 to 7 and 1001 to 1007; if some geometries carry a Z type code and others do not;
     *         if the input is truncated, a declared point, ring or geometry count is negative or
     *         too large to address, or a count needs more bytes than remain; if bytes remain
     *         after the geometry; if a line string declares exactly 1 point; if a polygon ring
     *         declares fewer than 4 points or is not closed; if a line string or ring vertex has
     *         {@code NaN} x and y, which encodes an empty point; if only one of x and y is
     *         {@code NaN}, an x or y is infinite, or the z of a point that is not empty is
     *         {@code NaN} or infinite; if an element of a {@link MultiPoint},
     *         {@link MultiLineString} or {@link MultiPolygon} is not of that collection's kind;
     *         if collection nesting is deeper than 32 levels; or if {@code srid} is 4326 and an
     *         x is outside -180 to 180 or a y is outside -90 to 90
     * @since 4.0.0
     */
    public static Geometry fromWkb(final byte[] wkb, final int srid) {
        return WkbReader.parse(wkb, srid);
    }

    // ------------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------------

    /**
     * Returns the coordinate interpretation model derived from the SRID.
     * SRID 0 is CARTESIAN, SRID 4326 is GEOGRAPHIC, all others are OTHER.
     *
     * @return the model
     * @since 4.0.0
     */
    public final Model model() {
        return CoordinateValidator.modelForSrid(srid);
    }

    /**
     * Returns the spatial reference ID.
     *
     * @return the SRID (0 for CARTESIAN, 4326 for GEOGRAPHIC, or custom)
     * @since 4.0.0
     */
    public final int srid() {
        return srid;
    }

    /**
     * Returns true if this geometry is empty (has no coordinates).
     * For collections, returns true if all parts are empty.
     *
     * @return true if empty
     * @since 4.0.0
     */
    public abstract boolean isEmpty();

    /**
     * Returns true if this geometry has z coordinates.
     *
     * @return true if 3D
     * @since 4.0.0
     */
    public final boolean hasZ() {
        return hasZ;
    }

    // ------------------------------------------------------------------------
    // Output formats
    // ------------------------------------------------------------------------

    /**
     * Returns ISO WKT without the SRID prefix.
     *
     * @return the WKT string
     * @since 4.0.0
     */
    public final String toWKT() {
        return WktWriter.write(this);
    }

    /**
     * Returns EWKT with "SRID=n;" prefix.
     *
     * @return the EWKT string
     * @since 4.0.0
     */
    public final String toEWKT() {
        return "SRID=" + srid + ";" + toWKT();
    }

    /**
     * Returns ISO WKB in big-endian byte order.
     *
     * @return the WKB bytes
     * @since 4.0.0
     */
    public final byte[] toWKB() {
        return WkbWriter.write(this);
    }

    // ------------------------------------------------------------------------
    // Object methods
    // ------------------------------------------------------------------------

    /**
     * Exact equality: same type, same SRID, same coordinates (including z flag).
     * Coordinates are compared numerically, so -0.0 equals 0.0.
     */
    @Override
    public abstract boolean equals(Object o);

    /**
     * Hash code consistent with equals. Zero values are normalized so -0.0 and 0.0 hash alike.
     */
    @Override
    public abstract int hashCode();

    /**
     * Compares this geometry with another in a total order that is consistent with
     * {@link #equals(Object)}: the result is 0 exactly when the two values are equal. Each key below
     * is used only when every key before it ties:
     * <ol>
     *   <li>the SRID</li>
     *   <li>emptiness, so empty geometries sort first</li>
     *   <li>the level-16 Hilbert key of the center of the 2D bounding box, for non-empty geometries,
     *       so that geometries which lie near each other sort near each other. The key is a 32-bit
     *       value compared as an unsigned integer</li>
     *   <li>the coordinates in WKB order, including z, compared numerically, so that -0.0 and 0.0
     *       tie. The shorter sequence sorts first</li>
     *   <li>the kind, by WKB type code: Point 1, LineString 2, Polygon 3, MultiPoint 4,
     *       MultiLineString 5, MultiPolygon 6, GeometryCollection 7</li>
     *   <li>the structure: for each nested geometry, taken recursively, its kind, its number of
     *       parts, and whether it holds parts of its own, one with none first. This is not the
     *       emptiness of key 2: it only separates an empty point from a point with a coordinate.
     *       When the whole structure ties, a geometry without z sorts before one with z</li>
     * </ol>
     *
     * @param other the geometry to compare with
     * @return a negative value, zero or a positive value as this geometry sorts before, with or
     *         after the other
     * @throws NullPointerException if {@code other} is null
     * @since 4.0.0
     */
    @Override
    public final int compareTo(final Geometry other) {
        return GeometryOrder.compare(this, other);
    }

    /**
     * Returns the WKT representation (same as {@link #toWKT()}).
     */
    @Override
    public final String toString() {
        return toWKT();
    }

    // ------------------------------------------------------------------------
    // Package-private helpers for subclasses
    // ------------------------------------------------------------------------

    /**
     * Returns the WKB type code for this geometry kind (1=Point, 2=LineString, etc.).
     */
    abstract int wkbTypeCode();

    /**
     * Returns the parts this geometry is built from, in WKB order: the points of a line string,
     * the shell ring followed by the hole rings of a polygon, the elements of a collection, and an
     * empty list for a point. A polygon with an empty shell has no rings.
     * <p>
     * This is the structure the order's last key walks, and the path the coordinate walk takes to
     * reach the points of any geometry.
     */
    abstract List<? extends Geometry> structuralParts();

    /**
     * Returns the Hilbert key of this value, the third key of the order, computed on first use.
     * The key is a 32 bit value to be compared as an unsigned integer.
     */
    final int hilbertKey() {
        final long cached = hilbertKeyCache;
        if (cached != HILBERT_KEY_ABSENT) {
            return (int) cached;
        }
        final int computed = HilbertKey.of(this);
        hilbertKeyCache = Integer.toUnsignedLong(computed);
        return computed;
    }

    /**
     * Returns true if this geometry holds no parts of its own: no coordinate for a point,
     * no points for a line string, no rings for a polygon, no elements for a collection.
     * <p>
     * This is the structural test the writers use to choose between {@code X EMPTY} and a
     * parenthesized body, so that WKT and WKB of one value describe the same structure.
     * It differs from the public {@link #isEmpty()}, which is deep: a collection whose parts
     * are all empty is empty, but it still has parts.
     */
    abstract boolean hasNoParts();

    /**
     * Writes the WKT body (coordinates or parts) to the builder.
     */
    abstract void writeWktBody(StringBuilder sb);

    /**
     * Returns the WKT type name (POINT, LINESTRING, etc.).
     */
    abstract String wktTypeName();
}

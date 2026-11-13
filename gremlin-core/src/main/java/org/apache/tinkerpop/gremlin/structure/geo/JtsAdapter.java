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

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds the JTS value that the geometry predicates are evaluated with, from the coordinates of a
 * TinkerPop geometry. The two models hold the same coordinates, so a geographic value is built in
 * longitude and latitude and the model is applied by the caller.
 * <p>
 * Only x and y are carried over: all geometry math is 2D, so z is dropped.
 * <p>
 * An empty component of a multi kind or of a collection is skipped, so it contributes nothing to
 * the value the predicates are evaluated with. A geometry that is empty throughout never reaches
 * here, because the predicates answer false for it before converting.
 */
final class JtsAdapter {

    /**
     * The factory is immutable and carries the default floating precision model, so one instance
     * serves every thread.
     */
    private static final GeometryFactory FACTORY = new GeometryFactory();

    private JtsAdapter() {
        // static methods only, do not instantiate class
    }

    /**
     * Converts a geometry of any kind, recursing into the parts of a collection.
     */
    static org.locationtech.jts.geom.Geometry toJts(final Geometry geometry) {
        if (geometry instanceof Point) {
            return toJts((Point) geometry);
        }
        if (geometry instanceof LineString) {
            return FACTORY.createLineString(coordinatesOf(((LineString) geometry).points()));
        }
        if (geometry instanceof Polygon) {
            return toJts((Polygon) geometry);
        }
        if (geometry instanceof MultiPoint) {
            final List<Point> points = nonEmpty(((MultiPoint) geometry).geometries());
            final org.locationtech.jts.geom.Point[] parts = new org.locationtech.jts.geom.Point[points.size()];
            for (int ix = 0; ix < parts.length; ix++) {
                parts[ix] = toJts(points.get(ix));
            }
            return FACTORY.createMultiPoint(parts);
        }
        if (geometry instanceof MultiLineString) {
            final List<LineString> lineStrings = nonEmpty(((MultiLineString) geometry).geometries());
            final org.locationtech.jts.geom.LineString[] parts = new org.locationtech.jts.geom.LineString[lineStrings.size()];
            for (int ix = 0; ix < parts.length; ix++) {
                parts[ix] = FACTORY.createLineString(coordinatesOf(lineStrings.get(ix).points()));
            }
            return FACTORY.createMultiLineString(parts);
        }
        if (geometry instanceof MultiPolygon) {
            final List<Polygon> polygons = nonEmpty(((MultiPolygon) geometry).geometries());
            final org.locationtech.jts.geom.Polygon[] parts = new org.locationtech.jts.geom.Polygon[polygons.size()];
            for (int ix = 0; ix < parts.length; ix++) {
                parts[ix] = toJts(polygons.get(ix));
            }
            return FACTORY.createMultiPolygon(parts);
        }
        if (geometry instanceof GeometryCollection) {
            final List<? extends Geometry> parts = nonEmpty(((GeometryCollection) geometry).geometries());
            final org.locationtech.jts.geom.Geometry[] elements = new org.locationtech.jts.geom.Geometry[parts.size()];
            for (int ix = 0; ix < elements.length; ix++) {
                elements[ix] = toJts(parts.get(ix));
            }
            return FACTORY.createGeometryCollection(elements);
        }
        throw new IllegalArgumentException("Unknown geometry kind " + geometry.getClass().getSimpleName());
    }

    /**
     * Returns the components that hold a coordinate, so that an empty one is left out of the value
     * it is a part of.
     */
    private static <G extends Geometry> List<G> nonEmpty(final List<G> components) {
        final List<G> kept = new ArrayList<>(components.size());
        for (final G component : components) {
            if (!component.isEmpty()) {
                kept.add(component);
            }
        }
        return kept;
    }

    private static org.locationtech.jts.geom.Point toJts(final Point point) {
        return point.isEmpty() ? FACTORY.createPoint() : FACTORY.createPoint(coordinateOf(point));
    }

    private static org.locationtech.jts.geom.Polygon toJts(final Polygon polygon) {
        final LineString shell = polygon.exteriorRing();
        if (shell.points().isEmpty()) {
            return FACTORY.createPolygon();
        }

        final List<LineString> interiorRings = polygon.interiorRings();
        final LinearRing[] holes = new LinearRing[interiorRings.size()];
        for (int ix = 0; ix < holes.length; ix++) {
            holes[ix] = FACTORY.createLinearRing(coordinatesOf(interiorRings.get(ix).points()));
        }
        return FACTORY.createPolygon(FACTORY.createLinearRing(coordinatesOf(shell.points())), holes);
    }

    private static Coordinate[] coordinatesOf(final List<Point> points) {
        final Coordinate[] coordinates = new Coordinate[points.size()];
        for (int ix = 0; ix < coordinates.length; ix++) {
            coordinates[ix] = coordinateOf(points.get(ix));
        }
        return coordinates;
    }

    private static Coordinate coordinateOf(final Point point) {
        return new Coordinate(point.x(), point.y());
    }
}

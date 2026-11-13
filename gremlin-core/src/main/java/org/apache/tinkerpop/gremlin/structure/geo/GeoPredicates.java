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
import org.locationtech.jts.operation.distance.DistanceOp;
import org.locationtech.jts.operation.relateng.RelateNG;
import org.locationtech.jts.operation.relateng.RelatePredicate;
import org.locationtech.spatial4j.distance.DistanceUtils;

/**
 * Evaluates the geometry predicates with JTS and Spatial4j.
 * <p>
 * A value that cannot be compared gives {@code false}: a non-geometry, null, an empty geometry, a
 * different SRID, or a model other than {@link Model#CARTESIAN} or {@link Model#GEOGRAPHIC}.
 * <p>
 * All math is 2D. {@link Model#GEOGRAPHIC} relates shapes in longitude and latitude and measures
 * meters on a sphere of the WGS84 mean radius, within about 1% of WGS84. Between shapes other than
 * two points, this holds for edges under about 100 km that cross neither the antimeridian nor a pole.
 * <p>
 * Operands are converted to JTS on every call and nothing is cached, because JTS geometries are not
 * thread safe and one predicate may be shared by many traversers.
 *
 * @since 4.0.0
 */
public final class GeoPredicates {

    /**
     * The WGS84 mean radius in meters, which is the sphere the geographic model measures on.
     */
    private static final double EARTH_RADIUS_METERS = 6371008.8;

    private GeoPredicates() {
        // static methods only, do not instantiate class
    }

    /**
     * Returns true if {@code value} lies entirely inside {@code argument}. A value that lies only on
     * the argument's boundary is not within it.
     *
     * @since 4.0.0
     */
    public static boolean within(final Object value, final Object argument) {
        if (!comparable(value, argument)) return false;
        return RelateNG.relate(JtsAdapter.toJts((Geometry) value), JtsAdapter.toJts((Geometry) argument),
                RelatePredicate.within());
    }

    /**
     * Returns true if {@code value} entirely contains {@code argument}. A geometry does not contain a
     * value that lies only on its boundary.
     *
     * @since 4.0.0
     */
    public static boolean contains(final Object value, final Object argument) {
        if (!comparable(value, argument)) return false;
        return RelateNG.relate(JtsAdapter.toJts((Geometry) value), JtsAdapter.toJts((Geometry) argument),
                RelatePredicate.contains());
    }

    /**
     * Returns true if the distance between {@code value} and {@code argument} is at most
     * {@code distance}. The distance is in coordinate units for
     * {@link Model#CARTESIAN} and in meters for {@link Model#GEOGRAPHIC}.
     * <p>
     * A distance of 0 matches geometries that touch or overlap. A negative or {@code NaN} distance
     * matches nothing, and an infinite distance matches every value this predicate can compare.
     *
     * @since 4.0.0
     */
    public static boolean withinDistance(final Object value, final Object argument, final Object distance) {
        if (!comparable(value, argument)) return false;
        if (!(distance instanceof Number)) return false;

        final double limit = ((Number) distance).doubleValue();
        if (Double.isNaN(limit) || limit < 0.0) return false;

        return distanceOf((Geometry) value, (Geometry) argument) <= limit;
    }

    /**
     * Returns the distance between two geometries that are known to be comparable, in the units of
     * their shared model.
     */
    private static double distanceOf(final Geometry value, final Geometry argument) {
        final org.locationtech.jts.geom.Geometry a = JtsAdapter.toJts(value);
        final org.locationtech.jts.geom.Geometry b = JtsAdapter.toJts(argument);

        if (value.model() != Model.GEOGRAPHIC) {
            return DistanceOp.distance(a, b);
        }

        if (a instanceof org.locationtech.jts.geom.Point && b instanceof org.locationtech.jts.geom.Point) {
            final Coordinate first = a.getCoordinate();
            final Coordinate second = b.getCoordinate();
            return greatCircleMeters(first.x, first.y, second.x, second.y);
        }

        // between geometries that are not both points, the nearest points are the pair JTS finds
        // in longitude and latitude, and the distance between them is then measured on the sphere
        final Coordinate[] nearest = DistanceOp.nearestPoints(a, b);
        return greatCircleMeters(nearest[0].x, nearest[0].y, nearest[1].x, nearest[1].y);
    }

    /**
     * Returns the great-circle distance in meters between two points given in degrees of longitude
     * and latitude.
     */
    private static double greatCircleMeters(final double lon1, final double lat1,
                                            final double lon2, final double lat2) {
        // Spatial4j takes latitude before longitude, and works in radians
        final double radians = DistanceUtils.distHaversineRAD(
                Math.toRadians(lat1), Math.toRadians(lon1), Math.toRadians(lat2), Math.toRadians(lon2));
        return DistanceUtils.radians2Dist(radians, EARTH_RADIUS_METERS);
    }

    /**
     * Returns true if a stored value and an argument can be compared at all: both are non-empty
     * geometries in the same spatial reference system, which is one TinkerPop defines the math for.
     * <p>
     * Emptiness is checked here because the distance of an empty geometry is 0 in JTS, which would
     * otherwise make every empty value lie within any distance.
     */
    private static boolean comparable(final Object value, final Object argument) {
        if (!(value instanceof Geometry) || !(argument instanceof Geometry)) return false;

        final Geometry first = (Geometry) value;
        final Geometry second = (Geometry) argument;
        if (first.srid() != second.srid()) return false;
        if (first.model() == Model.OTHER) return false;
        return !first.isEmpty() && !second.isEmpty();
    }
}

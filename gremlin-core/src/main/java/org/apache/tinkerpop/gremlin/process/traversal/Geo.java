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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.structure.geo.GeoPredicates;

import java.util.List;

/**
 * {@link Geo} is a {@code BiPredicate} that evaluates a spatial relationship between a geometry
 * value and the argument of a {@link GeoP}. A provider strategy that recognizes one of these
 * values on a {@code HasContainer} can answer the filter from a spatial index instead.
 * <p>
 * A value that cannot be compared is {@code false} rather than an error: a value that is not a
 * geometry, a null, an empty geometry, a value whose SRID differs from the argument's, and a value
 * in a coordinate system other than {@code CARTESIAN} or {@code GEOGRAPHIC}. All math is 2D, so z
 * is ignored.
 * <p>
 * In the geographic model an edge is treated as straight in longitude and latitude rather than as
 * a geodesic. The relate predicates and the distance of a geometry that is not a point are
 * therefore accurate for an edge shorter than about 100 km that crosses neither the antimeridian
 * nor a pole, where they stay within about 1% of the WGS84 measure. The distance between two
 * points is measured on the sphere directly, so it is accurate at any separation and is unaffected
 * by the antimeridian.
 *
 * @since 4.0.0
 */
public enum Geo implements PBiPredicate<Object, Object> {

    /**
     * Evaluates if the value lies entirely inside the argument.
     *
     * @since 4.0.0
     */
    geoWithin {
        @Override
        public boolean test(final Object value, final Object argument) {
            return GeoPredicates.within(value, argument);
        }
    },

    /**
     * Evaluates if the value entirely contains the argument.
     *
     * @since 4.0.0
     */
    geoContains {
        @Override
        public boolean test(final Object value, final Object argument) {
            return GeoPredicates.contains(value, argument);
        }
    },

    /**
     * Evaluates if the distance between the value and the argument's geometry is at most the
     * argument's distance. The argument is the geometry and the distance as a two element
     * list, in that order, so an argument of any other shape is {@code false}.
     *
     * @since 4.0.0
     */
    geoWithinDistance {
        @Override
        public boolean test(final Object value, final Object argument) {
            if (!(argument instanceof List)) return false;
            final List<?> arguments = (List<?>) argument;
            if (arguments.size() != ARGUMENT_COUNT) return false;
            return GeoPredicates.withinDistance(value, arguments.get(GEOMETRY_INDEX), arguments.get(DISTANCE_INDEX));
        }
    };

    /**
     * The position of the geometry in {@link #geoWithinDistance}'s argument list.
     */
    public static final int GEOMETRY_INDEX = 0;

    /**
     * The position of the distance in {@link #geoWithinDistance}'s argument list.
     */
    public static final int DISTANCE_INDEX = 1;

    /**
     * The size of {@link #geoWithinDistance}'s argument list.
     */
    public static final int ARGUMENT_COUNT = 2;

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract boolean test(final Object value, final Object argument);
}

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

import org.locationtech.jts.operation.relateng.RelateNG;
import org.locationtech.jts.operation.relateng.RelatePredicate;

/**
 * Evaluates the geometry predicates with JTS.
 * <p>
 * A value that cannot be compared gives {@code false}: a non-geometry, null, an empty geometry, a
 * different SRID, or a model other than {@link Model#CARTESIAN} or {@link Model#GEOGRAPHIC}.
 * <p>
 * All math is 2D. {@link Model#GEOGRAPHIC} relates shapes in longitude and latitude, with edges
 * straight in both, so the predicates hold for edges under about 100 km that cross neither the
 * antimeridian nor a pole.
 * <p>
 * Operands are converted to JTS on every call and nothing is cached, because JTS geometries are not
 * thread safe and one predicate may be shared by many traversers.
 *
 * @since 4.0.0
 */
public final class GeoPredicates {

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
     * Returns true if a stored value and an argument can be compared at all: both are non-empty
     * geometries in the same spatial reference system, which is one TinkerPop defines the math for.
     * <p>
     * An empty geometry is neither within another nor contains one, so it is excluded here.
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

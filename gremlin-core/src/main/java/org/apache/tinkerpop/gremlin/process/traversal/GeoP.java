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

import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.structure.geo.Geometry;
import org.apache.tinkerpop.gremlin.structure.geo.Model;

import java.util.HashMap;

/**
 * Predefined {@code Predicate} values that filter {@link Geometry} values by a spatial
 * relationship, backed by the {@link Geo} predicates.
 * <p>
 * An argument may instead come from a child traversal, which is resolved against each traverser.
 * An argument may also be a {@link GValue}, whose value is resolved when the traversal is built.
 * <p>
 * <b>Accuracy in the geographic model.</b> Shapes are related in longitude and latitude, with
 * edges straight in both, so the predicates hold for edges under about 100 km that cross neither
 * the antimeridian nor a pole.
 *
 * @since 4.0.0
 */
public class GeoP extends P<Geometry> {

    /**
     * Constructs a {@code GeoP} from the given geometry, which is validated as a constant. A
     * geometry resolved at runtime is given as a child traversal instead.
     *
     * @since 4.0.0
     */
    public GeoP(final PBiPredicate<Object, Object> biPredicate, final Object value) {
        super(asGeometryPredicate(biPredicate), GeoP.<Geometry>validatedValue(biPredicate, value));
    }

    /**
     * Constructs a {@code GeoP} with a child traversal whose result is resolved at runtime against
     * the current traverser.
     *
     * @since 4.0.0
     */
    public GeoP(final PBiPredicate<Object, Object> biPredicate, final Traversal.Admin<?, ?> traversalValue) {
        super(asGeometryPredicate(biPredicate), traversalValue);
    }

    /**
     * Constructs a {@code GeoP} from a variable, whose value is validated once it is resolved.
     *
     * @since 4.0.0
     */
    public GeoP(final PBiPredicate<Object, Object> biPredicate, final GValue<?> value) {
        super(asGeometryPredicate(biPredicate), asGeometryVariable(value));
        validateConstants(biPredicate, null == value ? null : value.get());
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof GeoP && super.equals(other);
    }

    @Override
    public P<Geometry> negate() {
        return new NotP<>(this);
    }

    public GeoP clone() {
        final GeoP clone = (GeoP) super.clone();
        clone.variables = new HashMap<>(variables);
        return clone;
    }

    /**
     * {@inheritDoc}
     * <p>
     * A new value for one of this predicate's variables is validated as the same value given to a
     * constructor would be, before it is kept.
     */
    @Override
    public void updateVariable(final String name, final Object value) {
        if (!variables.containsKey(name)) return;

        validateConstants(biPredicate, value);

        super.updateVariable(name, value);
    }

    //////////////// statics

    /**
     * Determines if a geometry lies entirely inside the given geometry. A value that lies only on
     * the given geometry's boundary is not within it.
     *
     * @since 4.0.0
     */
    public static GeoP geoWithin(final Geometry value) {
        return new GeoP(Geo.geoWithin, value);
    }

    /**
     * Determines if a geometry lies entirely inside the geometry a variable holds.
     *
     * @since 4.0.0
     */
    public static GeoP geoWithin(final GValue<Geometry> value) {
        return ofGeometry(Geo.geoWithin, value);
    }

    /**
     * Determines if a geometry lies entirely inside the geometry resolved from a child traversal
     * at runtime.
     *
     * @since 4.0.0
     */
    public static GeoP geoWithin(final Traversal<?, ?> traversalValue) {
        return new GeoP(Geo.geoWithin, traversalValue.asAdmin());
    }

    /**
     * Determines if a geometry entirely contains the given geometry. A geometry does not contain a
     * value that lies only on its boundary.
     *
     * @since 4.0.0
     */
    public static GeoP geoContains(final Geometry value) {
        return new GeoP(Geo.geoContains, value);
    }

    /**
     * Determines if a geometry entirely contains the geometry a variable holds.
     *
     * @since 4.0.0
     */
    public static GeoP geoContains(final GValue<Geometry> value) {
        return ofGeometry(Geo.geoContains, value);
    }

    /**
     * Determines if a geometry entirely contains the geometry resolved from a child traversal at
     * runtime.
     *
     * @since 4.0.0
     */
    public static GeoP geoContains(final Traversal<?, ?> traversalValue) {
        return new GeoP(Geo.geoContains, traversalValue.asAdmin());
    }

    //////////////// argument assembly

    /**
     * Builds a single argument predicate, using the plain form when the argument is not a variable
     * so that a literal reads the same however it was given.
     */
    private static GeoP ofGeometry(final PBiPredicate<Object, Object> biPredicate, final GValue<Geometry> value) {
        if (null == value) return new GeoP(biPredicate, (Object) null);
        if (!value.isVariable()) return new GeoP(biPredicate, (Object) value.get());
        return new GeoP(biPredicate, value);
    }

    /**
     * Carries the {@link Geo} predicates, which test a value of any type, as the predicate of a
     * geometry. A value that is not a geometry is {@code false} rather than an error, so a type
     * other than {@link Geometry} reaching {@code test} is answered and not cast.
     */
    @SuppressWarnings("unchecked")
    private static PBiPredicate<Geometry, Geometry> asGeometryPredicate(final PBiPredicate<Object, Object> biPredicate) {
        return (PBiPredicate<Geometry, Geometry>) (PBiPredicate<?, ?>) biPredicate;
    }

    /**
     * Carries a variable as a variable of a geometry, whose value is validated by the constructor.
     */
    @SuppressWarnings("unchecked")
    private static GValue<Geometry> asGeometryVariable(final GValue<?> value) {
        return (GValue<Geometry>) value;
    }

    /**
     * Returns the argument as this predicate's value, after validating it as a constant. A
     * {@code GeoP} filters by a geometry, so its value is one once it is accepted.
     */
    @SuppressWarnings("unchecked")
    private static <V> V validatedValue(final PBiPredicate<Object, Object> biPredicate, final Object value) {
        validateConstants(biPredicate, value);
        return (V) value;
    }

    //////////////// argument validation

    /**
     * Rejects a constant argument that can never match any value, which is a programming error
     * rather than a filter. The same value from a child traversal is false per traverser instead,
     * because it is data.
     */
    private static void validateConstants(final PBiPredicate<?, ?> biPredicate, final Object value) {
        final String name = biPredicate.getPredicateName();
        if (!(value instanceof Geometry)) {
            throw new IllegalArgumentException(String.format(
                    "The geometry of %s must be a Geometry, but was %s", name, describe(value)));
        }
        final Geometry geometry = (Geometry) value;
        if (geometry.model() == Model.OTHER) {
            throw new IllegalArgumentException(String.format(
                    "The geometry of %s must be in the CARTESIAN or GEOGRAPHIC model, but SRID %d is %s",
                    name, geometry.srid(), Model.OTHER));
        }
    }

    private static String describe(final Object value) {
        return null == value ? "null" : value.getClass().getSimpleName();
    }
}

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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.structure.geo.Geometry;
import org.apache.tinkerpop.gremlin.structure.geo.Model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Predefined {@code Predicate} values that filter {@link Geometry} values by a spatial
 * relationship, backed by the {@link Geo} predicates.
 * <p>
 * <b>What the predicate holds.</b> For {@code geoWithin} and {@code geoContains},
 * {@link #getValue()} is the geometry to compare against. For {@code geoWithinDistance} it is a
 * two element list of the geometry and the distance, in that order, so that both arguments stay
 * readable to a provider strategy that folds the filter into a spatial index.
 * <p>
 * An argument may instead come from a child traversal, which is resolved against each traverser.
 * {@code geoWithinDistance} then holds a child traversal for each of its two arguments, and a
 * constant given beside a traversal is carried as a constant traversal. An argument may also be a
 * {@link GValue}, whose value is resolved when the traversal is built.
 * <p>
 * <b>Accuracy in the geographic model.</b> {@link Model#GEOGRAPHIC} edges are treated as straight
 * in longitude and latitude rather than as geodesics. The relate predicates and the distance of a
 * geometry that is not a point are therefore accurate for an edge shorter than about 100 km that
 * crosses neither the antimeridian nor a pole, where they stay within about 1% of the WGS84
 * measure. The distance between two points is measured on the sphere directly, so it is accurate
 * at any separation and is unaffected by the antimeridian.
 *
 * @since 4.0.0
 */
public class GeoP extends P<Object> {

    /**
     * {@code geoWithinDistance}'s arguments as they were given, each either a value or a
     * {@link GValue}, so that the pair can be rebuilt when a variable takes a new value. Null
     * unless one of the arguments is a variable.
     */
    private final List<Object> arguments;

    public GeoP(final PBiPredicate<Object, Object> biPredicate, final Object value) {
        super(biPredicate, value);
        this.arguments = null;
        if (!hasTraversal()) {
            validateConstants(biPredicate, value);
        }
    }

    /**
     * Constructs a {@code GeoP} with a child traversal whose result is resolved at runtime against
     * the current traverser.
     *
     * @since 4.0.0
     */
    public GeoP(final PBiPredicate<Object, Object> biPredicate, final Traversal.Admin<?, ?> traversalValue) {
        super(biPredicate, traversalValue);
        this.arguments = null;
    }

    /**
     * Constructs a {@code GeoP} from a variable, whose value is validated once it is resolved.
     *
     * @since 4.0.0
     */
    @SuppressWarnings("unchecked")
    public GeoP(final PBiPredicate<Object, Object> biPredicate, final GValue<?> value) {
        super(biPredicate, (GValue<Object>) value);
        this.arguments = null;
        validateConstants(biPredicate, getValue());
    }

    /**
     * Constructs a {@code GeoP} from arguments kept in the order they were written, so that a
     * variable among them stays bound to the argument it was given for.
     */
    private GeoP(final PBiPredicate<Object, Object> biPredicate, final List<Object> arguments,
                 final Map<String, Object> variables) {
        super(biPredicate, Collections.<Object>singletonList(resolve(arguments, variables)), variables, false);
        this.arguments = Collections.unmodifiableList(new ArrayList<>(arguments));
        validateConstants(biPredicate, getValue());
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof GeoP && super.equals(other);
    }

    @Override
    public P<Object> negate() {
        return new NotP<>(this);
    }

    public GeoP clone() {
        return (GeoP) super.clone();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateVariable(final String name, final Object value) {
        super.updateVariable(name, value);
        if (null != arguments && variables.containsKey(name)) {
            literals = Collections.singletonList((Object) resolve(arguments, variables));
        }
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

    /**
     * Determines if the distance between a geometry and the given geometry is at most
     * {@code distance}, in coordinate units for {@link Model#CARTESIAN} and in meters for
     * {@link Model#GEOGRAPHIC}.
     *
     * @since 4.0.0
     */
    public static GeoP geoWithinDistance(final Geometry value, final Number distance) {
        return ofPair(value, distance);
    }

    /**
     * Determines if the distance between a geometry and the geometry a variable holds is at most
     * {@code distance}.
     *
     * @since 4.0.0
     */
    public static GeoP geoWithinDistance(final GValue<Geometry> value, final Number distance) {
        return ofPair(value, distance);
    }

    /**
     * Determines if the distance between a geometry and the given geometry is at most the distance
     * a variable holds.
     *
     * @since 4.0.0
     */
    public static GeoP geoWithinDistance(final Geometry value, final GValue<Number> distance) {
        return ofPair(value, distance);
    }

    /**
     * Determines if the distance between a geometry and the geometry a variable holds is at most
     * the distance a variable holds.
     *
     * @since 4.0.0
     */
    public static GeoP geoWithinDistance(final GValue<Geometry> value, final GValue<Number> distance) {
        return ofPair(value, distance);
    }

    /**
     * Determines if the distance between a geometry and the geometry resolved from a child
     * traversal at runtime is at most {@code distance}.
     *
     * @since 4.0.0
     */
    public static GeoP geoWithinDistance(final Traversal<?, ?> traversalValue, final Number distance) {
        validateDistance(Geo.geoWithinDistance.getPredicateName(), distance);
        return new GeoP(Geo.geoWithinDistance, Arrays.asList(traversalValue.asAdmin(), __.constant(distance)));
    }

    /**
     * Determines if the distance between a geometry and the geometry resolved from a child
     * traversal at runtime is at most the distance a variable holds.
     *
     * @since 4.0.0
     */
    public static GeoP geoWithinDistance(final Traversal<?, ?> traversalValue, final GValue<Number> distance) {
        if (null == distance || !distance.isVariable()) {
            return geoWithinDistance(traversalValue, null == distance ? null : distance.get());
        }
        validateDistance(Geo.geoWithinDistance.getPredicateName(), distance.get());
        return new GeoP(Geo.geoWithinDistance, Arrays.asList(traversalValue.asAdmin(), __.constant(distance)));
    }

    /**
     * Determines if the distance between a geometry and the given geometry is at most the distance
     * resolved from a child traversal at runtime.
     *
     * @since 4.0.0
     */
    public static GeoP geoWithinDistance(final Geometry value, final Traversal<?, ?> distanceTraversal) {
        validateGeometry(Geo.geoWithinDistance.getPredicateName(), value);
        return new GeoP(Geo.geoWithinDistance, Arrays.asList(__.constant(value), distanceTraversal.asAdmin()));
    }

    /**
     * Determines if the distance between a geometry and the geometry a variable holds is at most
     * the distance resolved from a child traversal at runtime.
     *
     * @since 4.0.0
     */
    public static GeoP geoWithinDistance(final GValue<Geometry> value, final Traversal<?, ?> distanceTraversal) {
        if (null == value || !value.isVariable()) {
            return geoWithinDistance(null == value ? null : value.get(), distanceTraversal);
        }
        validateGeometry(Geo.geoWithinDistance.getPredicateName(), value.get());
        return new GeoP(Geo.geoWithinDistance, Arrays.asList(__.constant(value), distanceTraversal.asAdmin()));
    }

    /**
     * Determines if the distance between a geometry and the geometry resolved from a child
     * traversal at runtime is at most the distance resolved from a child traversal at runtime.
     *
     * @since 4.0.0
     */
    public static GeoP geoWithinDistance(final Traversal<?, ?> traversalValue, final Traversal<?, ?> distanceTraversal) {
        return new GeoP(Geo.geoWithinDistance, Arrays.asList(traversalValue.asAdmin(), distanceTraversal.asAdmin()));
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
     * Builds {@code geoWithinDistance} from arguments that may each be a {@link GValue}, using the
     * plain form when neither is a variable.
     */
    private static GeoP ofPair(final Object value, final Object distance) {
        final List<Object> arguments = Arrays.asList(value, distance);
        final Map<String, Object> variables = variablesOf(arguments);
        if (variables.isEmpty()) {
            return new GeoP(Geo.geoWithinDistance, (Object) resolve(arguments, variables));
        }
        return new GeoP(Geo.geoWithinDistance, arguments, variables);
    }

    /**
     * Returns the value of every argument that is a variable, by name.
     */
    private static Map<String, Object> variablesOf(final List<Object> arguments) {
        final Map<String, Object> variables = new HashMap<>(arguments.size());
        for (final Object argument : arguments) {
            if (argument instanceof GValue && ((GValue<?>) argument).isVariable()) {
                variables.put(((GValue<?>) argument).getName(), ((GValue<?>) argument).get());
            }
        }
        return variables;
    }

    /**
     * Returns the arguments with each variable replaced by its current value.
     */
    private static List<Object> resolve(final List<Object> arguments, final Map<String, Object> variables) {
        final List<Object> values = new ArrayList<>(arguments.size());
        for (final Object argument : arguments) {
            if (argument instanceof GValue) {
                final GValue<?> gValue = (GValue<?>) argument;
                values.add(gValue.isVariable() ? variables.get(gValue.getName()) : gValue.get());
            } else {
                values.add(argument);
            }
        }
        return Collections.unmodifiableList(values);
    }

    //////////////// argument validation

    /**
     * Rejects a constant argument that can never match any value, which is a programming error
     * rather than a filter. The same value from a child traversal is false per traverser instead,
     * because it is data.
     */
    private static void validateConstants(final PBiPredicate<Object, Object> biPredicate, final Object value) {
        if (!(biPredicate instanceof Geo)) return;

        final String name = biPredicate.getPredicateName();
        if (biPredicate != Geo.geoWithinDistance) {
            validateGeometry(name, value);
            return;
        }

        if (!(value instanceof List) || ((List<?>) value).size() != Geo.ARGUMENT_COUNT) {
            throw new IllegalArgumentException(String.format(
                    "%s takes a geometry and a distance, but was given %s", name, describe(value)));
        }

        final List<?> arguments = (List<?>) value;
        validateGeometry(name, arguments.get(Geo.GEOMETRY_INDEX));
        validateDistance(name, arguments.get(Geo.DISTANCE_INDEX));
    }

    private static void validateGeometry(final String name, final Object value) {
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

    private static void validateDistance(final String name, final Object distance) {
        if (!(distance instanceof Number)) {
            throw new IllegalArgumentException(String.format(
                    "The distance of %s must be a number, but was %s", name, describe(distance)));
        }
    }

    private static String describe(final Object value) {
        return null == value ? "null" : value.getClass().getSimpleName();
    }
}

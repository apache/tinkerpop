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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_Traverser;
import org.apache.tinkerpop.gremlin.structure.geo.Geometry;
import org.apache.tinkerpop.gremlin.structure.geo.Model;
import org.apache.tinkerpop.gremlin.structure.geo.Point;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(Enclosed.class)
public class GeoPTest {

    private static final String SQUARE = "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))";
    private static final String HOLED_SQUARE = "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))";

    private static Traverser.Admin<?> traverser() {
        return traverser("start");
    }

    /**
     * A traverser whose object a child traversal reads, so that a map object gives
     * {@code select(key)} a value of its own for each traverser.
     */
    private static Traverser.Admin<?> traverser(final Object object) {
        return new B_O_Traverser<>(object, 1L);
    }

    /**
     * {@code geoWithin} and {@code geoContains} are converses, so each row is read in both
     * directions: the value is within the argument exactly when the argument contains the value.
     */
    @RunWith(Parameterized.class)
    public static class WithinAndContainsTest {

        @Parameterized.Parameter(value = 0)
        public String value;

        @Parameterized.Parameter(value = 1)
        public String argument;

        @Parameterized.Parameter(value = 2)
        public boolean within;

        @Parameterized.Parameters(name = "{0} within {1} is {2}")
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {"POINT(2 2)", SQUARE, true},
                    // a value that lies only on the boundary is not within
                    {"POINT(0 2)", SQUARE, false},
                    // the hole is outside the polygon
                    {"POINT(1.5 1.5)", HOLED_SQUARE, false},
                    {"LINESTRING(1 1, 3 3)", SQUARE, true},
                    {"LINESTRING(1 1, 5 5)", SQUARE, false},
                    {"POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))", SQUARE, true},
                    {SQUARE, SQUARE, true},
                    {"MULTIPOINT((1 1), (2 2))", SQUARE, true},
                    {"MULTILINESTRING((1 1, 2 2), (3 3, 3.5 3.5))", SQUARE, true},
                    {"MULTIPOLYGON(((1 1, 3 1, 3 3, 1 1)))", SQUARE, true},
                    {"GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(2 2, 3 3))", SQUARE, true},
                    {"GEOMETRYCOLLECTION(POINT(1 1), POINT(9 9))", SQUARE, false},
                    // z is ignored, so a 3D value is tested against a 2D argument in 2D
                    {"POINT Z (2 2 9)", SQUARE, true},
            });
        }

        @Test
        public void shouldEvaluateWithin() {
            assertEquals(within, GeoP.geoWithin(Geometry.fromWkt(argument)).test(Geometry.fromWkt(value)));
        }

        @Test
        public void shouldEvaluateContainsAsTheConverseOfWithin() {
            assertEquals(within, GeoP.geoContains(Geometry.fromWkt(value)).test(Geometry.fromWkt(argument)));
        }

    }

    /**
     * An argument given as a child traversal is resolved against each traverser.
     */
    public static class TraversalArgumentTest {

        private static final Geometry SQUARE_VALUE = Geometry.fromWkt(SQUARE);
        private static final Geometry FAR_SQUARE_VALUE =
                Geometry.fromWkt("POLYGON((10 10, 14 10, 14 14, 10 14, 10 10))");

        @Test
        public void shouldEvaluateWithinFromATraversalArgument() {
            final P<Geometry> predicate = GeoP.geoWithin(__.constant(SQUARE_VALUE));
            predicate.resolve(traverser());
            assertTrue(predicate.test(Point.of(2, 2)));
            assertFalse(predicate.test(Point.of(5, 5)));
        }

        @Test
        public void shouldResolveATraversalArgumentForEachTraverser() {
            final P<Geometry> predicate = GeoP.geoWithin(__.select("zone"));

            // the same value is within the first traverser's zone and outside the second's
            predicate.resolve(traverser(Collections.singletonMap("zone", SQUARE_VALUE)));
            assertTrue(predicate.test(Point.of(2, 2)));

            predicate.resolve(traverser(Collections.singletonMap("zone", FAR_SQUARE_VALUE)));
            assertFalse(predicate.test(Point.of(2, 2)));
        }
    }

    /**
     * A component that holds no coordinates contributes nothing, so a geometry that has one is
     * compared by the rest of its components.
     */
    @RunWith(Parameterized.class)
    public static class EmptyComponentTest {

        @Parameterized.Parameter
        public String value;

        @Parameterized.Parameters(name = "{0}")
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {"MULTIPOINT(EMPTY, (1 1))"},
                    {"MULTILINESTRING(EMPTY, (1 1, 2 2))"},
                    {"GEOMETRYCOLLECTION(POINT EMPTY, POINT(1 1))"},
                    {"MULTIPOLYGON(EMPTY, ((1 1, 3 1, 3 3, 1 1)))"},
            });
        }

        @Test
        public void shouldEvaluateWithinTheSquare() {
            assertTrue(GeoP.geoWithin(Geometry.fromWkt(SQUARE)).test(Geometry.fromWkt(value)));
        }
    }

    /**
     * The geographic model evaluates the relate predicates in longitude and latitude.
     */
    public static class GeographicModelTest {

        @Test
        public void shouldEvaluateWithinInLongitudeAndLatitude() {
            final Geometry square = Geometry.fromWkt(SQUARE, Model.GEOGRAPHIC);
            final Geometry interior = Geometry.fromWkt("POINT(2 2)", Model.GEOGRAPHIC);
            final Geometry onTheBoundary = Geometry.fromWkt("POINT(0 2)", Model.GEOGRAPHIC);

            assertTrue(GeoP.geoWithin(square).test(interior));
            assertTrue(GeoP.geoContains(interior).test(square));
            assertFalse(GeoP.geoWithin(square).test(onTheBoundary));
            assertFalse(GeoP.geoContains(onTheBoundary).test(square));
        }
    }

    /**
     * A value that cannot be compared is false, and a constant argument that can never match any
     * value is rejected when the predicate is built.
     */
    public static class RulesTest {

        private static final Geometry POINT = Point.of(2, 2);
        private static final Geometry SQUARE_VALUE = Geometry.fromWkt(SQUARE);

        private static List<P<?>> predicatesAgainst(final Geometry argument) {
            return Arrays.asList(GeoP.geoWithin(argument), GeoP.geoContains(argument));
        }

        private static void assertFalseForEvery(final Geometry argument, final Object value) {
            for (final P<?> predicate : predicatesAgainst(argument)) {
                assertFalse(predicate.getPredicateName() + " of " + value, test(predicate, value));
            }
        }

        /**
         * Tests a value of any type, as a step does, since a value that is not a geometry is false
         * rather than an error.
         */
        @SuppressWarnings("unchecked")
        private static boolean test(final P<?> predicate, final Object value) {
            return ((P<Object>) predicate).test(value);
        }

        @Test
        public void shouldNotMatchAValueThatIsNotAGeometry() {
            assertFalseForEvery(SQUARE_VALUE, null);
            assertFalseForEvery(SQUARE_VALUE, "POINT(2 2)");
            assertFalseForEvery(SQUARE_VALUE, 5);
            assertFalseForEvery(SQUARE_VALUE, new DetachedVertex(1, "person", Collections.emptyMap()));
        }

        @Test
        public void shouldNotMatchAnEmptyValue() {
            assertFalseForEvery(SQUARE_VALUE, Geometry.fromWkt("POINT EMPTY"));
            assertFalseForEvery(SQUARE_VALUE, Geometry.fromWkt("POLYGON EMPTY"));
        }

        @Test
        public void shouldNotMatchAnyValueForAnEmptyArgument() {
            final Geometry empty = Geometry.fromWkt("POLYGON EMPTY");
            assertFalseForEvery(empty, POINT);
            assertFalseForEvery(empty, SQUARE_VALUE);
            assertFalseForEvery(empty, Geometry.fromWkt("POINT EMPTY"));
        }

        @Test
        public void shouldNotMatchAValueInAnotherSrid() {
            final Geometry geographicSquare = Geometry.fromWkt(SQUARE, Model.GEOGRAPHIC);
            assertFalseForEvery(geographicSquare, POINT);
            assertFalseForEvery(SQUARE_VALUE, Point.of(2, 2, Model.GEOGRAPHIC));
        }

        @Test
        public void shouldNotMatchWhenBothSidesAreInAnotherSpatialReferenceSystem() {
            final P<Geometry> predicate = GeoP.geoWithin(__.constant(Geometry.fromWkt(SQUARE, 32618)));
            predicate.resolve(traverser());
            assertFalse(predicate.test(Geometry.fromWkt("POINT(2 2)", 32618)));
        }

        @Test
        public void shouldNotMatchAnArgumentFromATraversalThatCannotBeCompared() {
            final Object[][] cases = {
                    {Geometry.fromWkt("POLYGON EMPTY"), POINT},
                    // both sides are SRID 32618, so the argument is rejected for its model
                    {Geometry.fromWkt(SQUARE, 32618), Geometry.fromWkt("POINT(2 2)", 32618)},
                    {"POLYGON EMPTY", POINT},
            };
            for (final Object[] argumentAndValue : cases) {
                final P<Geometry> predicate = GeoP.geoWithin(__.constant(argumentAndValue[0]));
                predicate.resolve(traverser());
                assertFalse(String.valueOf(argumentAndValue[0]), test(predicate, argumentAndValue[1]));
            }
        }

        @Test
        public void shouldNotMatchWhenATraversalArgumentYieldsNothing() {
            final P<Geometry> within = GeoP.geoWithin(__.limit(0));
            within.resolve(traverser());
            assertTrue(within.isResolvedEmpty());
            assertFalse(within.test(POINT));
        }

        @Test
        public void shouldRejectANullConstantArgument() {
            final IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> GeoP.geoWithin((Geometry) null));
            assertEquals("The geometry of geoWithin must be a Geometry, but was null", iae.getMessage());
        }

        @Test
        public void shouldRejectAConstantArgumentInAnotherSpatialReferenceSystem() {
            final Geometry otherSrid = Geometry.fromWkt(SQUARE, 32618);
            final IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> GeoP.geoContains(otherSrid));
            assertEquals("The geometry of geoContains must be in the CARTESIAN or GEOGRAPHIC model, " +
                    "but SRID 32618 is OTHER", iae.getMessage());
        }

        @Test
        public void shouldValidateASetValueAsTheConstructorDoes() {
            final GeoP within = GeoP.geoWithin(SQUARE_VALUE);
            final Geometry otherSrid = Geometry.fromWkt(SQUARE, 32618);
            final String message = "The geometry of geoWithin must be in the CARTESIAN or GEOGRAPHIC model, " +
                    "but SRID 32618 is OTHER";
            assertEquals(message, assertThrows(IllegalArgumentException.class,
                    () -> within.setValue(otherSrid)).getMessage());
            // a variable is validated by the value it holds
            assertEquals(message, assertThrows(IllegalArgumentException.class,
                    () -> ((P) within).setValue(GValue.of("zone", otherSrid))).getMessage());

            // the rejected values are not kept
            assertEquals(SQUARE_VALUE, within.getValue());
            assertTrue(within.test(POINT));

            // a geometry the constructor accepts replaces the value
            final Geometry farSquare = Geometry.fromWkt("POLYGON((10 10, 14 10, 14 14, 10 14, 10 10))");
            within.setValue(farSquare);
            assertEquals(farSquare, within.getValue());
            assertFalse(within.test(POINT));
        }

        @Test
        public void shouldExposeTheGeoPredicateAndItsArgumentsToAProvider() {
            final HasContainer within = new HasContainer("loc", GeoP.geoWithin(SQUARE_VALUE));
            assertSame(Geo.geoWithin, within.getBiPredicate());
            assertEquals(SQUARE_VALUE, within.getValue());
        }

        @Test
        public void shouldFilterAStepThatYieldsGeometries() {
            // a geo predicate is a predicate of a geometry, so a step that yields geometries takes it
            final P<Geometry> within = GeoP.geoWithin(SQUARE_VALUE);
            final GraphTraversal<?, Geometry> points = EmptyGraph.instance().traversal()
                    .inject(Geometry.fromWkt("POINT(2 2)"), Geometry.fromWkt("POINT(5 5)"));
            assertEquals(Collections.singletonList(Geometry.fromWkt("POINT(2 2)")),
                    points.is(within).toList());
        }

        @Test
        public void shouldBeEqualWithEqualArguments() {
            assertEquals(GeoP.geoWithin(SQUARE_VALUE), GeoP.geoWithin(Geometry.fromWkt(SQUARE)));
            assertEquals(GeoP.geoWithin(SQUARE_VALUE).hashCode(), GeoP.geoWithin(Geometry.fromWkt(SQUARE)).hashCode());

            assertNotEquals(GeoP.geoWithin(SQUARE_VALUE), GeoP.geoContains(SQUARE_VALUE));
            assertNotEquals(GeoP.geoWithin(SQUARE_VALUE), GeoP.geoWithin(POINT));
            assertNotEquals(GeoP.geoWithin(SQUARE_VALUE), P.eq(SQUARE_VALUE));
        }

        @Test
        public void shouldNegateWithoutLosingTheArgument() {
            final P<Geometry> negated = GeoP.geoWithin(SQUARE_VALUE).negate();
            assertEquals(SQUARE_VALUE, negated.getValue());
            assertFalse(negated.test(POINT));
            assertTrue(negated.test(Point.of(5, 5)));
        }

        @Test
        public void shouldCloneWithTheSameArguments() {
            final GeoP original = GeoP.geoWithin(GValue.of("zone", SQUARE_VALUE));
            final GeoP clone = original.clone();
            assertEquals(original, clone);
            assertTrue(clone.test(POINT));

            // giving the clone's variable a new value leaves the original's variables as they were
            clone.updateVariable("zone", Geometry.fromWkt("POLYGON((10 10, 14 10, 14 14, 10 14, 10 10))"));
            assertEquals(Collections.singleton(GValue.of("zone", SQUARE_VALUE)), original.getGValues());
            assertEquals(SQUARE_VALUE, original.getValue());
            assertTrue(original.test(POINT));
        }
    }

    /**
     * A variable argument is resolved when the predicate is built and stays visible as a
     * parameter, so that it can be given a new value.
     */
    public static class VariableArgumentTest {

        private static final Geometry SQUARE_VALUE = Geometry.fromWkt(SQUARE);
        private static final Geometry FAR_SQUARE = Geometry.fromWkt("POLYGON((10 10, 14 10, 14 14, 10 14, 10 10))");
        private static final Geometry POINT = Point.of(2, 2);

        @Test
        public void shouldHoldAGeometryVariableAsAParameter() {
            final GeoP within = GeoP.geoWithin(GValue.of("zone", SQUARE_VALUE));
            assertTrue(within.isParameterized());
            assertEquals(Collections.singleton(GValue.of("zone", SQUARE_VALUE)), within.getGValues());
            assertTrue(within.test(POINT));
        }

        @Test
        public void shouldMatchTheNewValueWhenAVariableTakesOne() {
            final GeoP predicate = GeoP.geoWithin(GValue.of("zone", SQUARE_VALUE));
            assertTrue(predicate.test(POINT));

            predicate.updateVariable("zone", FAR_SQUARE);
            assertEquals(FAR_SQUARE, predicate.getValue());
            assertFalse(predicate.test(POINT));
        }

        @Test
        public void shouldBeTheConstantPredicateWhenAnArgumentIsALiteral() {
            assertEquals(GeoP.geoWithin(SQUARE_VALUE), GeoP.geoWithin(GValue.of(null, SQUARE_VALUE)));
            assertEquals(GeoP.geoContains(SQUARE_VALUE), GeoP.geoContains(GValue.of(null, SQUARE_VALUE)));
            assertFalse(GeoP.geoWithin(GValue.of(null, SQUARE_VALUE)).isParameterized());
        }

        @Test
        public void shouldRejectAVariableHoldingAGeometryInAnotherSpatialReferenceSystem() {
            final IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> GeoP.geoWithin(GValue.of("zone", Geometry.fromWkt(SQUARE, 32618))));
            assertEquals("The geometry of geoWithin must be in the CARTESIAN or GEOGRAPHIC model, " +
                    "but SRID 32618 is OTHER", iae.getMessage());
        }

        @Test
        public void shouldRejectAGeometryVariableThatTakesAValueThatIsNotAGeometry() {
            // a variable's value is validated when the predicate is built
            final IllegalArgumentException built = assertThrows(IllegalArgumentException.class,
                    () -> GeoP.geoWithin((GValue) GValue.of("zone", "not a geometry")));
            assertEquals("The geometry of geoWithin must be a Geometry, but was String", built.getMessage());

            final GeoP within = GeoP.geoWithin(GValue.of("zone", SQUARE_VALUE));
            final IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> within.updateVariable("zone", "not a geometry"));
            assertEquals("The geometry of geoWithin must be a Geometry, but was String", iae.getMessage());

            // the rejected value is not kept
            assertEquals(SQUARE_VALUE, within.getValue());
            assertEquals(Collections.singleton(GValue.of("zone", SQUARE_VALUE)), within.getGValues());
            assertTrue(within.test(POINT));
        }
    }

    /**
     * One predicate holds no state that two traversers can race on, so threads that share it see
     * the results a single thread sees.
     */
    public static class ConcurrencyTest {

        private static final int THREADS = 8;
        private static final int TESTS_PER_THREAD = 1000;

        @Test
        public void shouldEvaluateConsistentlyAcrossThreads() throws Exception {
            final Geometry square = Geometry.fromWkt(SQUARE);
            final P<Geometry> predicate = GeoP.geoWithin(square);
            final Geometry inside = Point.of(2, 2);
            final Geometry outside = Point.of(5, 5);

            final CyclicBarrier start = new CyclicBarrier(THREADS);
            final ExecutorService executor = Executors.newFixedThreadPool(THREADS);
            try {
                final List<Callable<Boolean>> tasks = new ArrayList<>(THREADS);
                for (int thread = 0; thread < THREADS; thread++) {
                    tasks.add(() -> {
                        // every thread waits here, so they all test the one predicate at once
                        start.await(30, TimeUnit.SECONDS);
                        for (int ix = 0; ix < TESTS_PER_THREAD; ix++) {
                            if (!predicate.test(inside) || predicate.test(outside)) return false;
                        }
                        return true;
                    });
                }

                for (final Future<Boolean> result : executor.invokeAll(tasks)) {
                    assertTrue(result.get());
                }
            } finally {
                executor.shutdownNow();
            }
        }
    }
}

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
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_Traverser;
import org.apache.tinkerpop.gremlin.structure.geo.Geometry;
import org.apache.tinkerpop.gremlin.structure.geo.Model;
import org.apache.tinkerpop.gremlin.structure.geo.Point;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
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

    /**
     * One degree of latitude on the sphere the geographic model measures on, in meters.
     */
    private static final double ONE_DEGREE_OF_LATITUDE = 111195.08;

    private static Traverser.Admin<?> traverser() {
        return new B_O_Traverser<>("start", 1L);
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
     * The distance is in coordinate units for the cartesian model, and inclusive.
     */
    @RunWith(Parameterized.class)
    public static class WithinDistanceTest {

        @Parameterized.Parameter(value = 0)
        public String value;

        @Parameterized.Parameter(value = 1)
        public String argument;

        @Parameterized.Parameter(value = 2)
        public Number distance;

        @Parameterized.Parameter(value = 3)
        public boolean expected;

        @Parameterized.Parameters(name = "{0} is within {2} of {1} is {3}")
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // exactly the distance, which is inclusive
                    {"POINT(3 4)", "POINT(0 0)", 5, true},
                    {"POINT(3 4.1)", "POINT(0 0)", 5, false},
                    // 1 from the nearest edge of the square
                    {"POINT(5 2)", SQUARE, 1, true},
                    {"POINT(5.1 2)", SQUARE, 1, false},    // just past 1
                    // a value inside the argument, and one on its boundary, are both 0 away
                    {"POINT(2 2)", SQUARE, 0, true},
                    {"POINT(0 2)", SQUARE, 0, true},
                    {"POINT(5 2)", SQUARE, 0, false},      // outside, so not 0 away
                    {"POINT(1.5 1.5)", HOLED_SQUARE, 0.5, true},   // in the hole, 0.5 from the hole's ring
                    {"POINT(1.5 1.5)", HOLED_SQUARE, 0.4, false},
                    // a negative distance matches nothing, even where its magnitude would reach
                    {"POINT(1 1)", "POINT(0 0)", -2, false},
                    {"POINT(1 1)", "POINT(0 0)", Double.NaN, false},
                    {"POINT(1000000 1000000)", "POINT(0 0)", Double.POSITIVE_INFINITY, true},
            });
        }

        @Test
        public void shouldEvaluateWithinDistance() {
            assertEquals(expected, GeoP.geoWithinDistance(Geometry.fromWkt(argument), distance)
                    .test(Geometry.fromWkt(value)));
        }

    }

    /**
     * An argument given as a child traversal is resolved against each traverser, and the two
     * arguments of {@code geoWithinDistance} keep the order they were written through resolution.
     */
    public static class TraversalArgumentTest {

        private static final Geometry SQUARE_VALUE = Geometry.fromWkt(SQUARE);

        @Test
        public void shouldEvaluateWithinFromATraversalArgument() {
            final P<Object> predicate = GeoP.geoWithin(__.constant(SQUARE_VALUE));
            predicate.resolve(traverser());
            assertTrue(predicate.test(Point.of(2, 2)));
            assertFalse(predicate.test(Point.of(5, 5)));
        }

        @Test
        public void shouldEvaluateWithinDistanceFromTraversalArguments() {
            final P<Object> predicate = GeoP.geoWithinDistance(__.constant(SQUARE_VALUE), __.constant(1));
            predicate.resolve(traverser());
            // 1 from the nearest edge of the square, and 2 from it
            assertTrue(predicate.test(Point.of(5, 2)));
            assertFalse(predicate.test(Point.of(6, 2)));
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

        @Test
        public void shouldEvaluateWithinDistanceOfTheSquare() {
            assertTrue(GeoP.geoWithinDistance(Geometry.fromWkt(SQUARE), 0).test(Geometry.fromWkt(value)));
        }
    }

    /**
     * The geographic model measures in meters on a sphere, and evaluates the relate predicates in
     * longitude and latitude.
     */
    public static class GeographicDistanceTest {

        @Test
        public void shouldMeasureOneDegreeOfLatitudeBetweenPoints() {
            final Geometry origin = Geometry.fromWkt("POINT(0 0)", Model.GEOGRAPHIC);
            final Geometry oneDegreeNorth = Geometry.fromWkt("POINT(0 1)", Model.GEOGRAPHIC);

            assertTrue(GeoP.geoWithinDistance(origin, ONE_DEGREE_OF_LATITUDE * (1.0 + 1e-6)).test(oneDegreeNorth));
            assertFalse(GeoP.geoWithinDistance(origin, ONE_DEGREE_OF_LATITUDE * (1.0 - 1e-6)).test(oneDegreeNorth));
        }

        @Test
        public void shouldMeasureAPointFiveKilometersAway() {
            final Geometry manhattan = Geometry.fromWkt("POINT(-73.98 40.75)", Model.GEOGRAPHIC);
            // offset in longitude, where the measure narrows by the cosine of the latitude: without
            // that narrowing these 0.0593 degrees would measure 6593.9 meters rather than 4995.3
            final Geometry fiveKilometersEast = Geometry.fromWkt("POINT(-73.9207 40.75)", Model.GEOGRAPHIC);

            assertTrue(GeoP.geoWithinDistance(manhattan, 5000).test(fiveKilometersEast));
            assertFalse(GeoP.geoWithinDistance(manhattan, 4990).test(fiveKilometersEast));
        }

        @Test
        public void shouldMeasureToTheNearestPointOfALineString() {
            final Geometry equatorSegment = Geometry.fromWkt("LINESTRING(0 0, 1 0)", Model.GEOGRAPHIC);
            final Geometry oneDegreeNorth = Geometry.fromWkt("POINT(0.5 1)", Model.GEOGRAPHIC);

            assertTrue(GeoP.geoWithinDistance(equatorSegment, 111200).test(oneDegreeNorth));
            assertFalse(GeoP.geoWithinDistance(equatorSegment, 111190).test(oneDegreeNorth));
        }

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

        private static List<P<Object>> predicatesAgainst(final Geometry argument) {
            return Arrays.asList(GeoP.geoWithin(argument), GeoP.geoContains(argument),
                    GeoP.geoWithinDistance(argument, 1000));
        }

        private static void assertFalseForEvery(final Geometry argument, final Object value) {
            for (final P<Object> predicate : predicatesAgainst(argument)) {
                assertFalse(predicate.getPredicateName() + " of " + value, predicate.test(value));
            }
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
            final P<Object> predicate = GeoP.geoWithin(__.constant(Geometry.fromWkt(SQUARE, 32618)));
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
                final P<Object> predicate = GeoP.geoWithin(__.constant(argumentAndValue[0]));
                predicate.resolve(traverser());
                assertFalse(String.valueOf(argumentAndValue[0]), predicate.test(argumentAndValue[1]));
            }
        }

        @Test
        public void shouldNotMatchWhenATraversalArgumentYieldsNothing() {
            final P<Object> within = GeoP.geoWithin(__.limit(0));
            within.resolve(traverser());
            assertTrue(within.isResolvedEmpty());
            assertFalse(within.test(POINT));

            // when both arguments are traversals an unresolved one drops out, leaving a pair the
            // predicate cannot read
            final P<Object> withinDistance = GeoP.geoWithinDistance(__.limit(0), 1000);
            withinDistance.resolve(traverser());
            assertEquals(Collections.singletonList(1000), withinDistance.getValue());
            assertFalse(withinDistance.test(POINT));
        }

        @Test
        public void shouldNotMatchWhenADistanceFromATraversalIsNotANumber() {
            final P<Object> predicate = GeoP.geoWithinDistance(SQUARE_VALUE, __.constant("1000"));
            predicate.resolve(traverser());
            assertFalse(predicate.test(POINT));
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
        public void shouldRejectAConstantDistanceThatIsNotANumber() {
            final IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> new GeoP(Geo.geoWithinDistance, Arrays.asList(SQUARE_VALUE, "10")));
            assertEquals("The distance of geoWithinDistance must be a number, but was String", iae.getMessage());
        }

        @Test
        public void shouldRejectAConstantDistanceArgumentThatIsNotAGeometryAndADistance() {
            final IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> new GeoP(Geo.geoWithinDistance, SQUARE_VALUE));
            assertEquals("geoWithinDistance takes a geometry and a distance, but was given Polygon",
                    iae.getMessage());
        }

        @Test
        public void shouldRejectAConstantGeometryGivenBesideADistanceTraversal() {
            final IllegalArgumentException missing = assertThrows(IllegalArgumentException.class,
                    () -> GeoP.geoWithinDistance((Geometry) null, __.select("radius")));
            assertEquals("The geometry of geoWithinDistance must be a Geometry, but was null",
                    missing.getMessage());

            final IllegalArgumentException otherSrid = assertThrows(IllegalArgumentException.class,
                    () -> GeoP.geoWithinDistance(Geometry.fromWkt(SQUARE, 32618), __.select("radius")));
            assertEquals("The geometry of geoWithinDistance must be in the CARTESIAN or GEOGRAPHIC model, " +
                    "but SRID 32618 is OTHER", otherSrid.getMessage());
        }

        @Test
        public void shouldRejectAConstantDistanceGivenBesideAGeometryTraversal() {
            final IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> GeoP.geoWithinDistance(__.select("zone"), (Number) null));
            assertEquals("The distance of geoWithinDistance must be a number, but was null", iae.getMessage());
        }

        @Test
        public void shouldExposeTheValueOfGeoWithinDistanceAsAnUnmodifiableList() {
            final List<?> value = (List<?>) GeoP.geoWithinDistance(SQUARE_VALUE, 10).getValue();
            assertThrows(UnsupportedOperationException.class, () -> ((List<Object>) value).set(0, POINT));
        }

        @Test
        public void shouldExposeTheGeoPredicateAndItsArgumentsToAProvider() {
            final HasContainer within = new HasContainer("loc", GeoP.geoWithin(SQUARE_VALUE));
            assertSame(Geo.geoWithin, within.getBiPredicate());
            assertEquals(SQUARE_VALUE, within.getValue());

            final HasContainer withinDistance = new HasContainer("loc", GeoP.geoWithinDistance(SQUARE_VALUE, 10));
            assertSame(Geo.geoWithinDistance, withinDistance.getBiPredicate());
            assertEquals(Arrays.asList(SQUARE_VALUE, 10), withinDistance.getValue());
        }

        @Test
        public void shouldNotMatchWhenGeoWithinDistanceIsGivenSomethingOtherThanAGeometryAndADistance() {
            assertFalse(Geo.geoWithinDistance.test(POINT, SQUARE_VALUE));
            assertFalse(Geo.geoWithinDistance.test(POINT, Collections.singletonList(SQUARE_VALUE)));
        }

        @Test
        public void shouldBeEqualWithEqualArguments() {
            assertEquals(GeoP.geoWithin(SQUARE_VALUE), GeoP.geoWithin(Geometry.fromWkt(SQUARE)));
            assertEquals(GeoP.geoWithin(SQUARE_VALUE).hashCode(), GeoP.geoWithin(Geometry.fromWkt(SQUARE)).hashCode());
            assertEquals(GeoP.geoWithinDistance(SQUARE_VALUE, 10), GeoP.geoWithinDistance(Geometry.fromWkt(SQUARE), 10));
            assertEquals(GeoP.geoWithinDistance(SQUARE_VALUE, 10).hashCode(),
                    GeoP.geoWithinDistance(Geometry.fromWkt(SQUARE), 10).hashCode());

            assertNotEquals(GeoP.geoWithin(SQUARE_VALUE), GeoP.geoContains(SQUARE_VALUE));
            assertNotEquals(GeoP.geoWithin(SQUARE_VALUE), GeoP.geoWithin(POINT));
            assertNotEquals(GeoP.geoWithinDistance(SQUARE_VALUE, 10), GeoP.geoWithinDistance(SQUARE_VALUE, 11));
            assertNotEquals(GeoP.geoWithin(SQUARE_VALUE), P.eq(SQUARE_VALUE));
        }

        @Test
        public void shouldNegateWithoutLosingTheGeoWithinDistanceArguments() {
            final P<Object> negated = GeoP.geoWithinDistance(SQUARE_VALUE, 10).negate();
            assertEquals(Arrays.asList(SQUARE_VALUE, 10), negated.getValue());
            assertFalse(negated.test(POINT));
        }

        @Test
        public void shouldCloneWithTheSameArguments() {
            final GeoP original = GeoP.geoWithinDistance(GValue.of("zone", SQUARE_VALUE), 10);
            final GeoP clone = original.clone();
            assertEquals(original, clone);
            assertTrue(clone.test(POINT));

            // giving the clone's variable a new value leaves the original reading as it was
            clone.updateVariable("zone", Geometry.fromWkt("POLYGON((10 10, 14 10, 14 14, 10 14, 10 10))"));
            assertEquals(Arrays.asList(SQUARE_VALUE, 10), original.getValue());
            assertTrue(original.test(POINT));
        }
    }

    /**
     * A variable argument is resolved when the predicate is built and stays visible as a
     * parameter, so that it can be given a new value without losing which argument it was for.
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
        public void shouldHoldBothGeoWithinDistanceVariablesAsParameters() {
            final GeoP predicate = GeoP.geoWithinDistance(GValue.of("zone", SQUARE_VALUE),
                    GValue.of("radius", (Number) 1));
            assertTrue(predicate.isParameterized());
            assertEquals(Arrays.asList(SQUARE_VALUE, 1), predicate.getValue());
            assertEquals(2, predicate.getGValues().size());
            assertTrue(predicate.test(POINT));
        }

        @Test
        public void shouldKeepTheArgumentOrderWhenAVariableTakesANewValue() {
            final GeoP predicate = GeoP.geoWithinDistance(GValue.of("zone", SQUARE_VALUE), 1);
            predicate.updateVariable("zone", FAR_SQUARE);
            assertEquals(Arrays.asList(FAR_SQUARE, 1), predicate.getValue());
            assertFalse(predicate.test(POINT));
        }

        @Test
        public void shouldBeTheConstantPredicateWhenAnArgumentIsALiteral() {
            assertEquals(GeoP.geoWithin(SQUARE_VALUE), GeoP.geoWithin(GValue.of(null, SQUARE_VALUE)));
            assertEquals(GeoP.geoContains(SQUARE_VALUE), GeoP.geoContains(GValue.of(null, SQUARE_VALUE)));
            assertEquals(GeoP.geoWithinDistance(SQUARE_VALUE, 10),
                    GeoP.geoWithinDistance(GValue.of(null, SQUARE_VALUE), GValue.of(null, (Number) 10)));
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
        public void shouldRejectAVariableHoldingNoDistance() {
            final IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> GeoP.geoWithinDistance(GValue.of("zone", SQUARE_VALUE), GValue.of("radius", (Number) null)));
            assertEquals("The distance of geoWithinDistance must be a number, but was null", iae.getMessage());
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
            final P<Object> predicate = GeoP.geoWithin(square);
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

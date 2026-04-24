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
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_Traverser;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for traversal-aware behavior in {@link P}, covering traversal detection accuracy
 * and single-value predicate rejection of multiple traversal results.
 */
@RunWith(Enclosed.class)
public class PTraversalTest {

    /**
     * Property 10: Traversal detection in predicates is accurate.
     * <p>
     * For any P instance containing a child traversal, {@code P.hasTraversal()} SHALL return true.
     * For any P instance containing only literal values or GValue variables,
     * {@code P.hasTraversal()} SHALL return false.
     * <p>
     * <b>Validates: Requirements 9.4</b>
     */
    public static class TraversalDetectionTest {

        @Test
        public void shouldDetectTraversalInComparisonPredicate() {
            final P<Object> p = P.eq(__.identity().asAdmin());
            assertThat(p.hasTraversal(), is(true));
        }

        @Test
        public void shouldDetectTraversalInCollectionPredicate() {
            final P<Object> p = P.within(__.inject(1, 2, 3).asAdmin());
            assertThat(p.hasTraversal(), is(true));
        }

        @Test
        public void shouldNotDetectTraversalInLiteralPredicate() {
            final P<String> p = P.eq("value");
            assertThat(p.hasTraversal(), is(false));
        }

        @Test
        public void shouldNotDetectTraversalInGValuePredicate() {
            final P<String> p = P.eq(GValue.of("x", "value"));
            assertThat(p.hasTraversal(), is(false));
        }

        @Test
        public void shouldReturnTraversalValueWhenPresent() {
            final Traversal.Admin<?, ?> traversal = __.inject(42).asAdmin();
            final P<Object> p = P.eq(traversal);
            assertThat(p.getTraversalValue() == traversal, is(true));
        }

        @Test
        public void shouldReturnNullTraversalValueForLiteral() {
            final P<String> p = P.eq("value");
            assertThat(p.getTraversalValue() == null, is(true));
        }
    }

    /**
     * Property 3: Single-value predicate rejects multiple traversal results.
     * <p>
     * For any single-value predicate (eq, neq, gt, lt, gte, lte) and any child traversal that produces
     * more than one result, the predicate SHALL throw an IllegalArgumentException.
     * For any collection predicate (within, without) and any child traversal producing multiple results,
     * the predicate SHALL accept all results as the collection value.
     * <p>
     * <b>Validates: Requirements 2.5</b>
     */
    public static class SingleValuePredicateRejectionTest {

        private Traverser.Admin<?> createTraverser(final Object value) {
            return new B_O_Traverser<>(value, 1L);
        }

        // --- Single-value predicates should throw on multiple results ---

        @Test(expected = IllegalArgumentException.class)
        public void shouldRejectMultipleResultsForEq() {
            final P<Object> p = P.eq(__.inject(1, 2).asAdmin());
            p.resolve(createTraverser("start"));
        }

        @Test(expected = IllegalArgumentException.class)
        public void shouldRejectMultipleResultsForNeq() {
            final P<Object> p = P.neq(__.inject(1, 2).asAdmin());
            p.resolve(createTraverser("start"));
        }

        @Test(expected = IllegalArgumentException.class)
        public void shouldRejectMultipleResultsForGt() {
            final P<Object> p = P.gt(__.inject(10, 20).asAdmin());
            p.resolve(createTraverser("start"));
        }

        @Test(expected = IllegalArgumentException.class)
        public void shouldRejectMultipleResultsForLt() {
            final P<Object> p = P.lt(__.inject(10, 20).asAdmin());
            p.resolve(createTraverser("start"));
        }

        @Test(expected = IllegalArgumentException.class)
        public void shouldRejectMultipleResultsForGte() {
            final P<Object> p = P.gte(__.inject(10, 20).asAdmin());
            p.resolve(createTraverser("start"));
        }

        @Test(expected = IllegalArgumentException.class)
        public void shouldRejectMultipleResultsForLte() {
            final P<Object> p = P.lte(__.inject(10, 20).asAdmin());
            p.resolve(createTraverser("start"));
        }

        // --- Collection predicates should accept multiple results ---

        @SuppressWarnings("unchecked")
        @Test
        public void shouldAcceptMultipleResultsForWithin() {
            final P<Object> p = P.within(__.inject(1, 2, 3).asAdmin());
            p.resolve(createTraverser("start"));
            // After resolve, the predicate should have the collection value and be testable
            assertThat(p.test(1), is(true));
            assertThat(p.test(2), is(true));
            assertThat(p.test(3), is(true));
            assertThat(p.test(4), is(false));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldAcceptMultipleResultsForWithout() {
            final P<Object> p = P.without(__.inject(1, 2, 3).asAdmin());
            p.resolve(createTraverser("start"));
            // After resolve, without should exclude the resolved values
            assertThat(p.test(1), is(false));
            assertThat(p.test(2), is(false));
            assertThat(p.test(3), is(false));
            assertThat(p.test(4), is(true));
        }

        // --- Single-value predicates should succeed with exactly one result ---

        @SuppressWarnings("unchecked")
        @Test
        public void shouldAcceptSingleResultForEq() {
            final P<Object> p = P.eq(__.constant(42).asAdmin());
            p.resolve(createTraverser("start"));
            assertThat(p.test(42), is(true));
            assertThat(p.test(99), is(false));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldAcceptSingleResultForNeq() {
            final P<Object> p = P.neq(__.constant(42).asAdmin());
            p.resolve(createTraverser("start"));
            assertThat(p.test(42), is(false));
            assertThat(p.test(99), is(true));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldAcceptSingleResultForGt() {
            final P<Object> p = P.gt(__.constant(10).asAdmin());
            p.resolve(createTraverser("start"));
            assertThat(p.test(11), is(true));
            assertThat(p.test(10), is(false));
            assertThat(p.test(9), is(false));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldAcceptSingleResultForLt() {
            final P<Object> p = P.lt(__.constant(10).asAdmin());
            p.resolve(createTraverser("start"));
            assertThat(p.test(9), is(true));
            assertThat(p.test(10), is(false));
            assertThat(p.test(11), is(false));
        }

        // --- Collection predicates should also work with single result ---

        @SuppressWarnings("unchecked")
        @Test
        public void shouldAcceptSingleResultForWithin() {
            final P<Object> p = P.within(__.constant(42).asAdmin());
            p.resolve(createTraverser("start"));
            assertThat(p.test(42), is(true));
            assertThat(p.test(99), is(false));
        }
    }
}

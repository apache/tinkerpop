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
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for traversal-aware behavior in {@link P}, covering traversal detection accuracy
 * and single-value predicate rejection of multiple traversal results.
 */
@RunWith(Enclosed.class)
public class PTraversalTest {

    /**
     * Tests that traversal detection in predicates is accurate: {@code P.hasTraversal()} returns
     * true for traversal-bearing predicates and false for literal/GValue predicates.
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
            assertThat(p.getChildTraversals(), is(notNullValue()));
            assertThat(p.getChildTraversals().get(0), is(sameInstance(traversal)));
        }

        @Test
        public void shouldReturnNullTraversalValueForLiteral() {
            final P<String> p = P.eq("value");
            assertThat(p.getChildTraversals(), is(nullValue()));
        }
    }

    /**
     * Tests first-result semantics: single-value predicates (eq, neq, gt, lt, gte, lte) take the
     * first result from a multi-result traversal. Collection predicates (within, without) accept
     * all results as the collection value.
     */
    public static class SingleValuePredicateRejectionTest {

        private Traverser.Admin<?> createTraverser(final Object value) {
            return new B_O_Traverser<>(value, 1L);
        }

        // --- Single-value predicates take first result, ignore extras (consistent with by()) ---

        @SuppressWarnings("unchecked")
        @Test
        public void shouldTakeFirstResultForEq() {
            final P<Object> p = P.eq(__.union(__.constant(1), __.constant(2)).asAdmin());
            p.resolve(createTraverser("start"));
            assertThat(p.test(1), is(true));
            assertThat(p.test(2), is(false));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldTakeFirstResultForNeq() {
            final P<Object> p = P.neq(__.union(__.constant(1), __.constant(2)).asAdmin());
            p.resolve(createTraverser("start"));
            assertThat(p.test(1), is(false));
            assertThat(p.test(2), is(true));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldTakeFirstResultForGt() {
            final P<Object> p = P.gt(__.union(__.constant(10), __.constant(20)).asAdmin());
            p.resolve(createTraverser("start"));
            assertThat(p.test(11), is(true));
            assertThat(p.test(10), is(false));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldTakeFirstResultForLt() {
            final P<Object> p = P.lt(__.union(__.constant(10), __.constant(20)).asAdmin());
            p.resolve(createTraverser("start"));
            assertThat(p.test(9), is(true));
            assertThat(p.test(10), is(false));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldTakeFirstResultForGte() {
            final P<Object> p = P.gte(__.union(__.constant(10), __.constant(20)).asAdmin());
            p.resolve(createTraverser("start"));
            assertThat(p.test(10), is(true));
            assertThat(p.test(9), is(false));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldTakeFirstResultForLte() {
            final P<Object> p = P.lte(__.union(__.constant(10), __.constant(20)).asAdmin());
            p.resolve(createTraverser("start"));
            assertThat(p.test(10), is(true));
            assertThat(p.test(11), is(false));
        }

        // --- Collection predicates should accept multiple results ---

        @SuppressWarnings("unchecked")
        @Test
        public void shouldAcceptMultipleResultsForWithin() {
            // within(traversal) takes first result only. Use fold() to get a collection.
            final P<Object> p = P.within(__.inject(1, 2, 3).fold().asAdmin());
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
            // without(traversal) takes first result only. Use fold() to get a collection.
            final P<Object> p = P.without(__.inject(1, 2, 3).fold().asAdmin());
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

    /**
     * Tests for multi-traversal support in within() and without().
     * <p>
     * When multiple traversals are passed to within(trav1, trav2, ...), each traversal is evaluated
     * independently and results are unioned into a single collection for the Contains test.
     */
    public static class MultiTraversalTest {

        private Traverser.Admin<?> createTraverser(final Object value) {
            return new B_O_Traverser<>(value, 1L);
        }

        // --- Detection ---

        @Test
        public void shouldDetectMultipleTraversalsInWithin() {
            final P<Object> p = P.within(__.constant(1).asAdmin(), __.constant(2).asAdmin());
            assertThat(p.hasTraversal(), is(true));
        }

        @Test
        public void shouldDetectMultipleTraversalsInWithout() {
            final P<Object> p = P.without(__.constant(1).asAdmin(), __.constant(2).asAdmin());
            assertThat(p.hasTraversal(), is(true));
        }

        @Test
        public void shouldReturnTraversalValuesListForMultiTraversal() {
            final P<Object> p = P.within(__.constant(1).asAdmin(), __.constant(2).asAdmin());
            assertThat(p.getChildTraversals(), is(notNullValue()));
            assertThat(p.getChildTraversals().size(), is(2));
        }

        @Test
        public void shouldReturnNullTraversalValueForMultiTraversal() {
            // childTraversals should be non-null when using multi-traversal form
            final P<Object> p = P.within(__.constant(1).asAdmin(), __.constant(2).asAdmin());
            assertThat(p.getChildTraversals(), is(notNullValue()));
            assertThat(p.getChildTraversals().size(), is(2));
        }

        // --- Resolution and testing ---

        @SuppressWarnings("unchecked")
        @Test
        public void shouldResolveMultipleTraversalsForWithin() {
            // within(__.constant(1), __.constant(2)) should union results: [1, 2]
            final P<Object> p = P.within(__.constant(1).asAdmin(), __.constant(2).asAdmin());
            p.resolve(createTraverser("start"));
            assertThat(p.test(1), is(true));
            assertThat(p.test(2), is(true));
            assertThat(p.test(3), is(false));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldResolveMultipleTraversalsForWithout() {
            // without(__.constant(1), __.constant(2)) should union results: [1, 2]
            final P<Object> p = P.without(__.constant(1).asAdmin(), __.constant(2).asAdmin());
            p.resolve(createTraverser("start"));
            assertThat(p.test(1), is(false));
            assertThat(p.test(2), is(false));
            assertThat(p.test(3), is(true));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldResolveMultipleTraversalsWithMultipleResultsEach() {
            // within(__.inject(1,2).fold(), __.inject(3,4).fold()) - each fold() produces a list, unpacked into union
            final P<Object> p = P.within(__.inject(1, 2).fold().asAdmin(), __.inject(3, 4).fold().asAdmin());
            p.resolve(createTraverser("start"));
            assertThat(p.test(1), is(true));
            assertThat(p.test(2), is(true));
            assertThat(p.test(3), is(true));
            assertThat(p.test(4), is(true));
            assertThat(p.test(5), is(false));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldHandleEmptyResultFromOneTraversal() {
            // within(__.inject(1,2).fold(), __.limit(0)) where second produces nothing
            // Should still match on results from first traversal
            final P<Object> p = P.within(__.inject(1, 2).fold().asAdmin(), __.limit(0).asAdmin());
            p.resolve(createTraverser("start"));
            assertThat(p.isResolvedEmpty(), is(false));
            assertThat(p.test(1), is(true));
            assertThat(p.test(2), is(true));
            assertThat(p.test(3), is(false));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldHandleAllEmptyResults() {
            // within(__.limit(0), __.limit(0)) where both produce nothing. A collection predicate resolves to
            // an empty set rather than flagging resolved-empty, so within() simply matches nothing.
            final P<Object> p = P.within(__.limit(0).asAdmin(), __.limit(0).asAdmin());
            p.resolve(createTraverser("start"));
            assertThat(p.isResolvedEmpty(), is(false));
            assertThat(p.test(1), is(false));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldResolveThreeTraversals() {
            // within(__.constant("a"), __.constant("b"), __.constant("c"))
            final P<Object> p = P.within(__.constant("a").asAdmin(), __.constant("b").asAdmin(), __.constant("c").asAdmin());
            p.resolve(createTraverser("start"));
            assertThat(p.test("a"), is(true));
            assertThat(p.test("b"), is(true));
            assertThat(p.test("c"), is(true));
            assertThat(p.test("d"), is(false));
        }

        // --- Clone independence ---

        @SuppressWarnings("unchecked")
        @Test
        public void shouldCloneMultiTraversalPredicate() {
            final P<Object> original = P.within(__.constant(1).asAdmin(), __.constant(2).asAdmin());
            final P<Object> clone = original.clone();

            // Clone should have independent traversal values
            assertThat(clone.hasTraversal(), is(true));
            assertThat(clone.getChildTraversals(), is(notNullValue()));
            assertThat(clone.getChildTraversals().size(), is(2));

            // Resolve the clone - this mutates the clone's internal state
            clone.resolve(createTraverser("start"));
            assertThat(clone.test(1), is(true));

            // Original should be unaffected: resolving it independently should still work correctly
            original.resolve(createTraverser("other"));
            assertThat(original.test(1), is(true));
            assertThat(original.test(2), is(true));
        }

        // --- Varargs detection ---

        @SuppressWarnings("unchecked")
        @Test
        public void shouldDetectMultipleTraversalsInVarargs() {
            // This tests the varargs path: P.within(trav1, trav2) going through within(V... values)
            final Traversal<?, ?> trav1 = __.constant(10);
            final Traversal<?, ?> trav2 = __.constant(20);
            final P<Object> p = (P<Object>) P.within(trav1, trav2);
            assertThat(p.hasTraversal(), is(true));
            assertThat(p.getChildTraversals() != null, is(true));
            assertThat(p.getChildTraversals().size(), is(2));
            p.resolve(createTraverser("start"));
            assertThat(p.test(10), is(true));
            assertThat(p.test(20), is(true));
            assertThat(p.test(30), is(false));
        }

        // --- collectTraversals and integrateTraversals ---

        @Test
        public void shouldCollectTraversalsFromMultiTraversalPredicate() {
            final P<Object> p = P.within(__.constant(1).asAdmin(), __.constant(2).asAdmin(), __.constant(3).asAdmin());
            final java.util.List<Traversal.Admin<?, ?>> collected = new java.util.ArrayList<>();
            P.collectTraversals(p, collected);
            assertThat(collected.size(), is(3));
        }
    }

    /**
     * Covers the empty-result semantics of collection (within/without) versus scalar predicates and the
     * behavior of connective predicates (AndP/OrP) and NotP when their operands carry child traversals.
     * <p>
     * Regression coverage for: within(empty) -> false, without(empty) -> true (collection predicates resolve
     * to an empty collection rather than short-circuiting), scalar predicates remain resolved-empty, and the
     * shared-state safety of resolving the same predicate across many traversers sequentially.
     */
    public static class EmptyResolutionAndConnectiveTest {

        private Traverser.Admin<?> createTraverser(final Object value) {
            return new B_O_Traverser<>(value, 1L);
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldPassWithoutWhenTraversalResolvesEmpty() {
            // without(empty set) -> nothing to exclude -> everything passes
            final P<Object> p = P.without(__.<Object>limit(0).asAdmin());
            p.resolve(createTraverser("anything"));
            assertThat(p.isResolvedEmpty(), is(false));
            assertThat(p.test("anything"), is(true));
            assertThat(p.test(42), is(true));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldFailWithinWhenTraversalResolvesEmpty() {
            // within(empty set) -> nothing matches -> everything fails
            final P<Object> p = P.within(__.<Object>limit(0).asAdmin());
            p.resolve(createTraverser("anything"));
            assertThat(p.isResolvedEmpty(), is(false));
            assertThat(p.test("anything"), is(false));
            assertThat(p.test(42), is(false));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldPassWithoutWhenFoldedTraversalIsEmpty() {
            // explicit empty collection via fold() of nothing
            final P<Object> p = P.without(__.<Object>limit(0).fold().asAdmin());
            p.resolve(createTraverser("anything"));
            assertThat(p.test("anything"), is(true));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldFailWithinWhenMultiTraversalResolvesAllEmpty() {
            final P<Object> p = P.within(__.<Object>limit(0).asAdmin(), __.<Object>limit(0).asAdmin());
            p.resolve(createTraverser("anything"));
            assertThat(p.isResolvedEmpty(), is(false));
            assertThat(p.test(1), is(false));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldRemainResolvedEmptyForScalarPredicateWithEmptyTraversal() {
            // eq(empty) has no comparison value -> resolved empty so the step can short-circuit
            final P<Object> p = P.eq(__.<Object>limit(0).asAdmin());
            p.resolve(createTraverser("anything"));
            assertThat(p.isResolvedEmpty(), is(true));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldResolveAndPWithTraversalOperands() {
            final P<Object> p = (P<Object>) (P) P.gt(__.constant(10).asAdmin()).and(P.lt(__.constant(20).asAdmin()));
            p.resolve(createTraverser("start"));
            assertThat(p.test(15), is(true));
            assertThat(p.test(10), is(false));
            assertThat(p.test(25), is(false));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldShortCircuitAndPResolveWhenScalarChildEmpty() {
            // first child resolves empty (no comparison value); AndP cannot be satisfied
            final P<Object> p = (P<Object>) (P) P.eq(__.<Object>limit(0).asAdmin()).and(P.gt(__.constant(5).asAdmin()));
            p.resolve(createTraverser("start"));
            assertThat(p.isResolvedEmpty(), is(true));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldResolveOrPWithTraversalOperands() {
            final P<Object> p = (P<Object>) (P) P.eq(__.constant(1).asAdmin()).or(P.eq(__.constant(2).asAdmin()));
            p.resolve(createTraverser("start"));
            assertThat(p.test(1), is(true));
            assertThat(p.test(2), is(true));
            assertThat(p.test(3), is(false));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldReturnFalseForOrPWhenResolvedNonEmptyButNonMatching() {
            // a non-empty within() resolution that simply does not contain the test value must still return false
            final P<Object> p = (P<Object>) (P) P.within(__.inject(1, 2, 3).fold().asAdmin())
                    .or(P.within(__.inject(4, 5, 6).fold().asAdmin()));
            p.resolve(createTraverser("start"));
            assertThat(p.test(2), is(true));
            assertThat(p.test(5), is(true));
            assertThat(p.test(9), is(false));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldResolveNotPWrappingTraversalPredicate() {
            final P<Object> p = P.eq(__.constant(42).asAdmin()).negate();
            p.resolve(createTraverser("start"));
            assertThat(p.test(42), is(false));
            assertThat(p.test(99), is(true));
        }

        @Test
        public void shouldExposeWrappedPredicateFromNotP() {
            final P<Object> inner = P.eq(__.constant(42).asAdmin());
            final NotP<Object> notP = new NotP<>(inner);
            assertThat(notP.getWrapped() == inner, is(true));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void shouldProduceConsistentResultsAcrossManySequentialResolves() {
            // resolve() mutates the predicate per traverser; verify repeated resolve+test cycles are stable
            final P<Object> p = P.gt(__.constant(10).asAdmin());
            for (int i = 0; i < 1000; i++) {
                p.resolve(createTraverser("start" + i));
                assertThat(p.test(11), is(true));
                assertThat(p.test(5), is(false));
            }
        }
    }
}

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
package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import static org.junit.Assert.assertThrows;

/**
 * Tests for construction-time child traversal validation.
 */
public class ChildTraversalValidatorTest {

    private final GraphTraversalSource g = EmptyGraph.instance().traversal();

    // ===== FILTER CONTEXT: should reject mutating steps =====

    @Test
    public void shouldRejectAddVInHasTraversal() {
        assertThrows(IllegalArgumentException.class, () ->
                g.V().has("name", __.addV("x").values("name")));
    }

    @Test
    public void shouldRejectDropInHasTraversal() {
        assertThrows(IllegalArgumentException.class, () ->
                g.V().has("name", __.V().drop().constant("x")));
    }

    @Test
    public void shouldRejectNestedMutatingInHasTraversal() {
        assertThrows(IllegalArgumentException.class, () ->
                g.V().has("name", __.V().map(__.addV("x")).values("name")));
    }

    @Test
    public void shouldRejectMutatingInHasWithTAccessor() {
        assertThrows(IllegalArgumentException.class, () ->
                g.V().has(org.apache.tinkerpop.gremlin.structure.T.label, __.addV("x").label()));
    }

    @Test
    public void shouldRejectMutatingInHasWithLabel() {
        assertThrows(IllegalArgumentException.class, () ->
                g.V().has("person", "name", __.addV("x").values("name")));
    }

    // ===== LOOKUP CONTEXT: should reject mutating steps =====

    @Test
    public void shouldRejectAddVInMidTraversalV() {
        assertThrows(IllegalArgumentException.class, () ->
                g.V().V(__.addV("x").id()));
    }

    @Test
    public void shouldRejectAddVInMidTraversalE() {
        assertThrows(IllegalArgumentException.class, () ->
                g.V().E(__.addV("x").id()));
    }

    @Test
    public void shouldRejectAddVInStartStepV() {
        assertThrows(IllegalArgumentException.class, () ->
                g.V(__.addV("x").id()));
    }

    @Test
    public void shouldRejectAddVInStartStepE() {
        assertThrows(IllegalArgumentException.class, () ->
                g.E(__.addV("x").id()));
    }

    // ===== P FACTORY METHODS: should reject mutating steps =====

    @Test
    public void shouldRejectMutatingInPEq() {
        assertThrows(IllegalArgumentException.class, () ->
                P.eq(__.addV("x").values("name")));
    }

    @Test
    public void shouldRejectMutatingInPNeq() {
        assertThrows(IllegalArgumentException.class, () ->
                P.neq(__.addV("x").values("name")));
    }

    @Test
    public void shouldRejectMutatingInPGt() {
        assertThrows(IllegalArgumentException.class, () ->
                P.gt(__.addV("x").values("age")));
    }

    @Test
    public void shouldRejectMutatingInPLt() {
        assertThrows(IllegalArgumentException.class, () ->
                P.lt(__.addV("x").values("age")));
    }

    @Test
    public void shouldRejectMutatingInPGte() {
        assertThrows(IllegalArgumentException.class, () ->
                P.gte(__.addV("x").values("age")));
    }

    @Test
    public void shouldRejectMutatingInPLte() {
        assertThrows(IllegalArgumentException.class, () ->
                P.lte(__.addV("x").values("age")));
    }

    @Test
    public void shouldRejectMutatingInPWithin() {
        assertThrows(IllegalArgumentException.class, () ->
                P.within(__.addV("x").values("name")));
    }

    @Test
    public void shouldRejectMutatingInPWithout() {
        assertThrows(IllegalArgumentException.class, () ->
                P.without(__.addV("x").values("name")));
    }

    @Test
    public void shouldRejectMutatingInMultiTraversalWithin() {
        assertThrows(IllegalArgumentException.class, () ->
                P.within(__.V().values("name"), __.addV("x").values("name")));
    }

    @Test
    public void shouldRejectMutatingInMultiTraversalWithout() {
        assertThrows(IllegalArgumentException.class, () ->
                P.without(__.V().values("name"), __.addV("x").values("name")));
    }

    // ===== TextP FACTORY METHODS: should reject mutating steps =====

    @Test
    public void shouldRejectMutatingInTextPStartingWith() {
        assertThrows(IllegalArgumentException.class, () ->
                TextP.startingWith(__.addV("x").values("name")));
    }

    @Test
    public void shouldRejectMutatingInTextPContaining() {
        assertThrows(IllegalArgumentException.class, () ->
                TextP.containing(__.addV("x").values("name")));
    }

    // ===== MUTATION CONTEXT: should reject DropStep but allow other mutations =====

    @Test
    public void shouldRejectDropInPropertyTraversal() {
        assertThrows(IllegalArgumentException.class, () ->
                g.V().property(__.V().map(__.drop()).project("x").by("name")));
    }

    @Test
    public void shouldRejectNestedDropInPropertyTraversal() {
        assertThrows(IllegalArgumentException.class, () ->
                g.V().property(__.V().union(__.drop(), __.constant("x")).project("k").by()));
    }

    // ===== VALID TRAVERSALS: should pass without error =====

    @Test
    public void shouldAllowReadOnlyHasTraversal() {
        // Should not throw
        g.V().has("name", __.V().values("name"));
    }

    @Test
    public void shouldAllowNavigationInHasTraversal() {
        // Should not throw
        g.V().has("name", __.V().out("knows").values("name"));
    }

    @Test
    public void shouldAllowReadOnlyLookupTraversal() {
        // Should not throw
        g.V().V(__.out("knows").id());
    }

    @Test
    public void shouldAllowReadOnlyStartStepV() {
        // Should not throw
        g.V(__.V().id());
    }

    @Test
    public void shouldAllowReadOnlyPredicate() {
        // Should not throw
        P.eq(__.V().values("age"));
    }

    @Test
    public void shouldAllowAddVInMutationContext() {
        // addV is allowed in property(traversal) — only DropStep is blocked
        g.V().property(__.V().addV("temp").project("k").by("name"));
    }

    @Test
    public void shouldAllowReadOnlyPropertyTraversal() {
        // Should not throw
        g.V().property(__.V().project("name").by("name"));
    }
}

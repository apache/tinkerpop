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
package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link HasContainer}, including traversal-bearing constructor and lifecycle methods.
 */
public class HasContainerTest {

    @Test
    public void shouldReturnFalseForHasTraversalWithLiteralPredicate() {
        final HasContainer hc = new HasContainer("name", P.eq("marko"));
        assertThat(hc.hasTraversal(), is(false));
    }

    @Test
    public void shouldReturnTrueForHasTraversalWithTraversalValue() {
        final Traversal.Admin<?, ?> traversal = __.identity().asAdmin();
        final HasContainer hc = new HasContainer("name", traversal);
        assertThat(hc.hasTraversal(), is(true));
    }

    @Test
    public void shouldSetFieldsCorrectlyWithTraversalConstructor() {
        final Traversal.Admin<?, ?> traversal = __.identity().asAdmin();
        final HasContainer hc = new HasContainer("age", traversal);

        assertEquals("age", hc.getKey());
        assertThat(hc.getPredicate(), is(nullValue()));
        assertThat(hc.getTraversalValue(), is(notNullValue()));
        assertThat(hc.getTraversalValue(), is(sameInstance(traversal)));
    }

    @Test
    public void shouldCloneProduceIndependentDeepCopyOfTraversalValue() {
        final Traversal.Admin<?, ?> traversal = __.identity().asAdmin();
        final HasContainer original = new HasContainer("name", traversal);
        final HasContainer clone = original.clone();

        // clone should have a traversalValue that is not the same instance
        assertThat(clone.getTraversalValue(), is(notNullValue()));
        assertThat(clone.getTraversalValue(), is(not(sameInstance(original.getTraversalValue()))));

        // key should be equal
        assertEquals(original.getKey(), clone.getKey());

        // both should still report hasTraversal
        assertThat(clone.hasTraversal(), is(true));
    }

    @Test
    public void shouldCloneLiteralHasContainerWithoutTraversal() {
        final HasContainer original = new HasContainer("name", P.eq("marko"));
        final HasContainer clone = original.clone();

        assertThat(clone.hasTraversal(), is(false));
        assertThat(clone.getTraversalValue(), is(nullValue()));
        assertEquals("name", clone.getKey());
        assertEquals(P.eq("marko"), clone.getPredicate());
    }

    @Test
    public void shouldReturnNullTraversalValueForLiteralConstructor() {
        final HasContainer hc = new HasContainer("name", P.eq("marko"));
        assertThat(hc.getTraversalValue(), is(nullValue()));
    }
}

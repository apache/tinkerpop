/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server.util;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TraversalIteratorTest {
    @Test
    public void shouldIterateWithNoSideEffects() {
        final Graph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        final Traversal t = g.V();
        final TraversalIterator itty = new TraversalIterator(t.asAdmin());

        int counter = 0;
        while(itty.hasNext()) {
            counter++;

            // there are no side effects so there should never be a side-effect key
            assertNull(itty.getCurrentSideEffectKey());
            assertThat(itty.isNextBatchComingUp(), is(false));

            itty.next();
        }

        assertEquals(6, counter);
    }

    @Test
    public void shouldIterateWithOneSideEffect() {
        final Graph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        final Traversal t = g.V().aggregate("a");
        final TraversalIterator itty = new TraversalIterator(t.asAdmin());

        int counter = 0;
        while(itty.hasNext()) {
            counter++;

            itty.next();

            if (counter == 6)
                assertThat(itty.isNextBatchComingUp(), is(true));
            else
                assertThat(itty.isNextBatchComingUp(), is(false));

            // first 6 should be "result" and the second 6 should be "a"
            if (counter <= 6)
                assertNull(itty.getCurrentSideEffectKey());
            else
                assertEquals("a", itty.getCurrentSideEffectKey());
        }

        assertEquals(12, counter);
    }

    @Test
    public void shouldIterateWithMultipleSideEffects() {
        final Graph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        final Traversal t = g.V().aggregate("a").outE("knows").aggregate("b").outV();
        final TraversalIterator itty = new TraversalIterator(t.asAdmin());

        int counter = 0;
        while(itty.hasNext()) {
            counter++;

            // just be sure multiple calls to hasNext() don't foul things up
            itty.hasNext();
            itty.hasNext();
            itty.hasNext();

            itty.next();

            // just be sure multiple calls to hasNext() don't foul things up
            itty.hasNext();
            itty.hasNext();
            itty.hasNext();

            // batches occur at switch between result and "a" side effect then between side-effect "a" and "b"
            if (counter == 2 || counter == 8)
                assertThat(itty.isNextBatchComingUp(), is(true));
            else
                assertThat(itty.isNextBatchComingUp(), is(false));

            // first 2 should be "result" and the second 6 should be "a" and the third 2 should be "b" - i think it
            // is safe to assume deterministic order here on the side-effects
            if (counter <= 2)
                assertNull(itty.getCurrentSideEffectKey());
            else if (counter <= 8)
                assertEquals("a", itty.getCurrentSideEffectKey());
            else
                assertEquals("b", itty.getCurrentSideEffectKey());
        }

        assertEquals(10, counter);
    }
}

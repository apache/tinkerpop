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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.ExceptionCoverage;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.remote.EmbeddedRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inject;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Pieter Martin
 */
@ExceptionCoverage(exceptionClass = Traversal.Exceptions.class, methods = {
        "traversalIsLocked"
})
@ExceptionCoverage(exceptionClass = Graph.Exceptions.class, methods = {
        "idArgsMustBeEitherIdOrElement"
})
public class CoreTraversalTest extends AbstractGremlinProcessTest {

    @Test
    @LoadGraphWith
    public void shouldNeverPropagateANoBulkTraverser() {
        try {
            assertFalse(g.V().dedup().sideEffect(t -> t.asAdmin().setBulk(0)).hasNext());
            assertEquals(0, g.V().dedup().sideEffect(t -> t.asAdmin().setBulk(0)).toList().size());
            g.V().dedup().sideEffect(t -> t.asAdmin().setBulk(0)).sideEffect(t -> fail("this should not have happened")).iterate();
        } catch (VerificationException e) {
            // its okay if lambdas can't be serialized by the test suite
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldCloneTraversalForReuse() {
        final DefaultTraversal<Vertex, Long> t = (DefaultTraversal) g.V().count();
        assertEquals(6, t.next().intValue());
        assertThat(t.hasNext(), is(false));

        final DefaultTraversal<Vertex, Long> t1 = t.clone();
        assertEquals(6, t1.next().intValue());
        assertThat(t1.hasNext(), is(false));

        final DefaultTraversal<Vertex, Long> t2 = t.clone();
        assertEquals(6, t2.next().intValue());
        assertThat(t2.hasNext(), is(false));

        final DefaultTraversal<Vertex, Long> t3 = t1.clone();
        assertEquals(6, t3.next().intValue());
        assertThat(t3.hasNext(), is(false));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldFilterOnIterate() {
        final Traversal<Vertex,String> traversal = g.V().out().out().<String>values("name").aggregate("x").iterate();
        assertFalse(traversal.hasNext());
        assertEquals(2, traversal.asAdmin().getSideEffects().<BulkSet>get("x").size());
        assertTrue(traversal.asAdmin().getSideEffects().<BulkSet>get("x").contains("ripple"));
        assertTrue(traversal.asAdmin().getSideEffects().<BulkSet>get("x").contains("lop"));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldAllowIdsOfMixedTypes() {
        final List<Vertex> vertices = g.V().toList();
        assertEquals(2, g.V(vertices.get(0), vertices.get(1).id()).count().next().intValue());
        assertEquals(2, g.V(vertices.get(0).id(), vertices.get(1)).count().next().intValue());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldHaveNextAndToCollectionWorkProperly() {
        final Traversal<Vertex, Vertex> traversal = g.V();
        assertTrue(traversal.next() instanceof Vertex);
        assertEquals(4, traversal.next(4).size());
        assertTrue(traversal.hasNext());
        assertTrue(traversal.tryNext().isPresent());
        assertFalse(traversal.hasNext());
        assertFalse(traversal.tryNext().isPresent());
        assertFalse(traversal.hasNext());

        Traversal<Integer, Integer> intTraversal = inject(7, 7, 2, 3, 6);
        assertTrue(intTraversal.hasNext());
        final List<Integer> list = intTraversal.toList();
        assertFalse(intTraversal.hasNext());
        assertEquals(5, list.size());
        assertEquals(7, list.get(0).intValue());
        assertEquals(7, list.get(1).intValue());
        assertEquals(2, list.get(2).intValue());
        assertEquals(3, list.get(3).intValue());
        assertEquals(6, list.get(4).intValue());
        assertFalse(intTraversal.hasNext());
        assertFalse(intTraversal.tryNext().isPresent());

        intTraversal = inject(7, 7, 2, 3, 6);
        assertTrue(intTraversal.hasNext());
        final Set<Integer> set = intTraversal.toSet();
        assertFalse(intTraversal.hasNext());
        assertEquals(4, set.size());
        assertTrue(set.contains(7));
        assertTrue(set.contains(2));
        assertTrue(set.contains(3));
        assertTrue(set.contains(6));
        assertFalse(intTraversal.hasNext());
        assertFalse(intTraversal.tryNext().isPresent());

        intTraversal = inject(7, 7, 2, 3, 6);
        assertTrue(intTraversal.hasNext());
        final BulkSet<Integer> bulkSet = intTraversal.toBulkSet();
        assertFalse(intTraversal.hasNext());
        assertEquals(4, bulkSet.uniqueSize());
        assertEquals(5, bulkSet.longSize());
        assertEquals(5, bulkSet.size());
        assertTrue(bulkSet.contains(7));
        assertTrue(bulkSet.contains(2));
        assertTrue(bulkSet.contains(3));
        assertTrue(bulkSet.contains(6));
        assertEquals(2, bulkSet.get(7));
        assertFalse(intTraversal.hasNext());
        assertFalse(intTraversal.tryNext().isPresent());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldHavePropertyForEachRemainingBehaviorEvenWithStrategyRewrite() {
        final GraphTraversal<Vertex, Map<Object, Long>> traversal = g.V().out().groupCount();
        traversal.forEachRemaining(Map.class, map -> assertTrue(map instanceof Map));
    }

    @Test
    @Ignore
    @LoadGraphWith(MODERN)
    public void shouldNotAlterTraversalAfterTraversalBecomesLocked() {
        final GraphTraversal<Vertex, Vertex> traversal = this.g.V();
        assertTrue(traversal.hasNext());
        try {
            traversal.count().next();
            fail("Should throw: " + Traversal.Exceptions.traversalIsLocked());
        } catch (IllegalStateException e) {
            assertEquals(Traversal.Exceptions.traversalIsLocked().getMessage(), e.getMessage());
        } catch (Exception e) {
            fail("Should throw: " + Traversal.Exceptions.traversalIsLocked() + " not " + e + ":" + e.getMessage());
        }
        traversal.iterate();
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
    public void shouldTraverseIfAutoTxEnabledAndOriginalTxIsClosed() {
        // this should be the default, but manually set in just in case the implementation has other ideas
        g.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.AUTO);

        // close down the current transaction
        final Traversal t = g.V().has("name", "marko");
        g.tx().rollback();

        // the traversal should still work since there are auto transactions
        assertEquals(1, IteratorUtils.count(t));
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
    public void shouldTraverseIfManualTxEnabledAndOriginalTxIsClosed() {
        // auto should be the default, so force manual
        g.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);

        // close down the current transaction and fire up a fresh one
        g.tx().open();
        final Traversal t = g.V().has("name", "marko");
        g.tx().rollback();

        // the traversal should still work since there are auto transactions
        g.tx().open();
        assertEquals(1, IteratorUtils.count(t));
        g.tx().rollback();
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldNotThrowFastNoSuchElementException() {
        //FastNoSuchElement exceptions don't have a stack trace.
        //They should be converted to regular exceptions before returning to the the user.
        try {
            g.V().has("foo").next();
            fail("Expected a user facing NoSuchElementException");
        } catch (NoSuchElementException e) {
            assertEquals(NoSuchElementException.class, e.getClass());
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldThrowFastNoSuchElementExceptionInNestedTraversals() {
        //The nested traversal should throw a regular FastNoSuchElementException

        final GraphTraversal<Object, Object> nestedTraversal = __.has("name", "foo");
        final GraphTraversal<Vertex, Object> traversal = g.V().has("name", "marko").branch(nestedTraversal);

        final GraphTraversal.Admin<Object, Object> nestedTraversalAdmin = nestedTraversal.asAdmin();
        nestedTraversalAdmin.reset();
        nestedTraversalAdmin.addStart(nestedTraversalAdmin.getTraverserGenerator().generate(g.V().has("name", "marko").next(), (Step) traversal.asAdmin().getStartStep(), 1l));

        try {
            nestedTraversal.next();
        } catch (NoSuchElementException e) {
            assertEquals(FastNoSuchElementException.class, e.getClass());
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldAllowEmbeddedRemoteConnectionUsage() {
        final GraphTraversalSource simulatedRemoteG = traversal().withRemote(new EmbeddedRemoteConnection(g));
        assertEquals(6, simulatedRemoteG.V().count().next().intValue());
        assertEquals("marko", simulatedRemoteG.V().has("name", "marko").values("name").next());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldThrowNiceExceptionWhenMapKeyNotFoundInMathStep() {
        try {
            g.V().hasLabel("person").project("age").by("age").as("x").math("x").by("aged").iterate();
            fail("Traversal should no have succeeded since the 'aged' key does not exist");
        } catch (IllegalStateException ise) {
            assertThat(ise.getMessage(), startsWith("The variable x for math() step must resolve to a Number"));
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldThrowNiceExceptionWhenMapKeyIsNotResolvingToNumberInMathStep() {
        try {
            g.V().hasLabel("person").project("age").by("age").as("x").math("x").by("name").iterate();
            fail("Traversal should no have succeeded since the 'name' key does not resolve to Number");
        } catch (IllegalStateException ise) {
            assertThat(ise.getMessage(), startsWith("The variable x for math() step must resolve to a Number"));
        }
    }
}

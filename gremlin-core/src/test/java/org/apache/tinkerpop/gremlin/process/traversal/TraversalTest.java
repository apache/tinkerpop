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

import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TraversalTest {

    @Test
    public void shouldTryNext() {
        final MockTraversal<Integer> t = new MockTraversal<>(1, 2, 3);
        final Optional<Integer> optFirst = t.tryNext();
        assertEquals(1, optFirst.get().intValue());
        final Optional<Integer> optSecond = t.tryNext();
        assertEquals(2, optSecond.get().intValue());
        final Optional<Integer> optThird = t.tryNext();
        assertEquals(3, optThird.get().intValue());

        IntStream.range(0, 100).forEach(i -> {
            assertThat(t.tryNext().isPresent(), is(false));
        });
    }

    @Test
    public void shouldGetTwoAtATime() {
        final MockTraversal<Integer> t = new MockTraversal<>(1, 2, 3, 4, 5, 6, 7);
        final List<Integer> batchOne = t.next(2);
        assertEquals(2, batchOne.size());
        assertThat(batchOne, hasItems(1 ,2));

        final List<Integer> batchTwo = t.next(2);
        assertEquals(2, batchTwo.size());
        assertThat(batchTwo, hasItems(3 ,4));

        final List<Integer> batchThree = t.next(2);
        assertEquals(2, batchThree.size());
        assertThat(batchThree, hasItems(5, 6));

        final List<Integer> batchFour = t.next(2);
        assertEquals(1, batchFour.size());
        assertThat(batchFour, hasItems(7));

        final List<Integer> batchFive = t.next(2);
        assertEquals(0, batchFive.size());
    }

    @Test
    public void shouldFillList() {
        final MockTraversal<Integer> t = new MockTraversal<>(1, 2, 3, 4, 5, 6, 7);
        final List<Integer> listToFill = new ArrayList<>();
        final List<Integer> batch = t.fill(listToFill);
        assertEquals(7, batch.size());
        assertThat(batch, hasItems(1 ,2, 3, 4, 5, 6, 7));
        assertThat(t.hasNext(), is(false));
        assertSame(listToFill, batch);
    }

    @Test
    public void shouldStream() {
        final MockTraversal<Integer> t = new MockTraversal<>(1, 2, 3, 4, 5, 6, 7);
        final List<Integer> batch = t.toStream().collect(Collectors.toList());
        assertEquals(7, batch.size());
        assertThat(batch, hasItems(1 ,2, 3, 4, 5, 6, 7));
        assertThat(t.hasNext(), is(false));
    }

    @Test
    public void shouldIterate() {
        final MockTraversal<Integer> t = new MockTraversal<>(1, 2, 3, 4, 5, 6, 7);
        assertThat(t.hasNext(), is(true));
        t.iterate();
        assertThat(t.hasNext(), is(false));
    }

    @Test
    public void shouldCloseWithoutACloseableStep() throws Exception {
        final MockTraversal<Integer> t = new MockTraversal<>(1, 2, 3, 4, 5, 6, 7);
        assertThat(t.hasNext(), is(true));
        t.close();
        assertThat(t.isClosed(), is(false));
    }

    @Test
    public void shouldCloseWithCloseableStep() throws Exception {
        final MockTraversal<Integer> t = new MockTraversal<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7).iterator(), true);
        assertThat(t.hasNext(), is(true));
        t.close();
        assertThat(t.isClosed(), is(true));
    }

    @Test
    public void shouldOnlyAllowAnonymousChildren() {
        final GraphTraversalSource g = traversal().with(EmptyGraph.instance());
        g.V(1).addE("self").to(__.V(1));
        try {
            g.V(1).addE("self").to(g.V(1));
            fail("Should not allow child traversals spawned from 'g'");
        } catch (IllegalStateException ignored) {}
    }

    private static class MockCloseStep<E> extends MockStep<E> implements AutoCloseable {
        private boolean closed = false;

        public MockCloseStep(final Iterator<E> itty) {
            super(itty);
        }

        boolean isClosed() {
            return closed;
        }

        @Override
        public void close() throws Exception {
            closed = true;
        }
    }

    private static class MockStep<E> implements Step<E,E> {

        private final Iterator<E> itty;

        MockStep(final Iterator<E> itty) {
            this.itty = itty;
        }

        @Override
        public void addStarts(final Iterator starts) { }

        @Override
        public void addStart(final Traverser.Admin start) { }

        @Override
        public boolean hasStarts() {
            return false;
        }

        @Override
        public void setPreviousStep(final Step step) { }

        @Override
        public Step getPreviousStep() {
            return null;
        }

        @Override
        public void setNextStep(final Step step) { }

        @Override
        public Step getNextStep() {
            return null;
        }

        @Override
        public Traversal.Admin getTraversal() {
            return null;
        }

        @Override
        public void setTraversal(final Traversal.Admin traversal) { }

        @Override
        public void reset() { }

        @Override
        public Step clone() {
            return null;
        }

        @Override
        public Set<String> getLabels() {
            return null;
        }

        @Override
        public void addLabel(final String label) { }

        @Override
        public void removeLabel(final String label) { }

        @Override
        public void clearLabels() { }

        @Override
        public void setId(final String id) { }

        @Override
        public String getId() {
            return null;
        }

        @Override
        public boolean hasNext() {
            return itty.hasNext();
        }

        @Override
        public Traverser.Admin<E> next() {
            return new DefaultRemoteTraverser<>(itty.next(), 1L);
        }
    }

    private static class MockTraversal<T> implements Traversal.Admin<T,T> {

        private Iterator<T> itty;

        private Step mockEndStep;

        private List<Step> steps;

        MockTraversal(final T... objects) {
            this(Arrays.asList(objects));
        }

        MockTraversal(final List<T> list) {
            this(list.iterator(), false);
        }

        MockTraversal(final Iterator<T> itty, final boolean asCloseable) {
            this.itty = itty;
            mockEndStep = asCloseable ? new MockCloseStep<>(itty) : new MockStep<>(itty);
            steps = Collections.singletonList(mockEndStep);
        }

        boolean isClosed() {
            return mockEndStep instanceof MockCloseStep && ((MockCloseStep) mockEndStep).isClosed();
        }

        @Override
        public GremlinLang getGremlinLang() {
            return new GremlinLang();
        }

        @Override
        public List<Step> getSteps() {
            return steps;
        }

        @Override
        public <S2, E2> Admin<S2, E2> addStep(final int index, final Step<?, ?> step) throws IllegalStateException {
            return null;
        }

        @Override
        public <S2, E2> Admin<S2, E2> removeStep(final int index) throws IllegalStateException {
            return null;
        }

        @Override
        public void applyStrategies() throws IllegalStateException {

        }

        @Override
        public TraverserGenerator getTraverserGenerator() {
            return null;
        }

        @Override
        public Set<TraverserRequirement> getTraverserRequirements() {
            return null;
        }

        @Override
        public void setSideEffects(final TraversalSideEffects sideEffects) {

        }

        @Override
        public TraversalSideEffects getSideEffects() {
            return null;
        }

        @Override
        public void setStrategies(final TraversalStrategies strategies) {

        }

        @Override
        public TraversalStrategies getStrategies() {
            return null;
        }

        @Override
        public void setParent(final TraversalParent step) {

        }

        @Override
        public TraversalParent getParent() {
            return null;
        }

        @Override
        public Admin<T, T> clone() {
            return null;
        }

        @Override
        public boolean isLocked() {
            return false;
        }

        @Override
        public void lock() {

        }

        @Override
        public Optional<Graph> getGraph() {
            return null;
        }

        @Override
        public void setGraph(final Graph graph) {

        }

        @Override
        public boolean hasNext() {
            return itty.hasNext();
        }

        @Override
        public T next() {
            if (Thread.interrupted()) throw new TraversalInterruptedException();
            return itty.next();
        }
    }
}

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

import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.process.remote.traversal.RemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Provide a way to convert a {@link Traversal} and its related side-effects into a single {@code Iterator}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TraversalIterator implements Iterator<Object> {

    private final Traversal.Admin traversal;
    private Iterator<Object> traversalIterator;
    private Iterator<Object> sideEffectIterator = Collections.emptyIterator();
    private final HaltedTraverserStrategy haltedTraverserStrategy;
    private Iterator<String> sideEffectKeys = Collections.emptyIterator();
    private String currentSideEffectKey = null;
    private String currentSideEffectAggregator = null;
    private boolean latch = false;

    public TraversalIterator(final Traversal.Admin traversal) {
        this.traversal = traversal;
        this.traversalIterator = traversal.getEndStep();
        this.haltedTraverserStrategy = traversal.getStrategies().getStrategy(HaltedTraverserStrategy.class).orElse(
                Boolean.valueOf(System.getProperty("is.testing", "false")) ?
                        HaltedTraverserStrategy.detached() :
                        HaltedTraverserStrategy.reference());
    }

    public String getCurrentSideEffectKey() {
        return this.currentSideEffectKey;
    }

    public String getCurrentSideEffectAggregator() {
        return currentSideEffectAggregator;
    }

    /**
     * Checks if the next "batch" of results are being returned. The "batch" refers to sets of results - in this case
     * traversal results and individual sets of traversal side-effects.
     */
    public boolean isNextBatchComingUp() {
        boolean nextBatch = false;
        if (!latch) {
            nextBatch = !traversalIterator.hasNext() && !sideEffectIterator.hasNext() && sideEffectKeys.hasNext();
            if (nextBatch) latch = true;
        }

        return nextBatch;
    }

    @Override
    public boolean hasNext() {
        return this.traversalIterator.hasNext() || sideEffectIterator.hasNext() || sideEffectKeys.hasNext();
    }

    @Override
    public Object next() {
        Object next = null;

        // first iterate the traversal end step
        if (traversalIterator.hasNext()) {
            final Traverser.Admin t = this.haltedTraverserStrategy.halt((Traverser.Admin) traversalIterator.next());
            next = new RemoteTraverser<>(t.get(), t.bulk());

            // since there are no items left in the "result" then get the side-effect keys iterator
            if (!traversalIterator.hasNext())
                sideEffectKeys = traversal.getSideEffects().keys().iterator();
        } else {
            // if there are side-effects and iteration of the current side-effect is not in action then set it off
            if (sideEffectKeys.hasNext() && !sideEffectIterator.hasNext()) {
                currentSideEffectKey = sideEffectKeys.next();

                // coerce everything to a state of bulking so that the client deals with a common incoming stream
                // of BulkedResult
                final Object currentSideEffect = traversal.getSideEffects().get(currentSideEffectKey);
                currentSideEffectAggregator = getAggregatorType(currentSideEffect);
                sideEffectIterator = currentSideEffect instanceof BulkSet ?
                        new BulkResultIterator((BulkSet) currentSideEffect) :
                        IteratorUtils.asIterator(currentSideEffect);
            }

            if (sideEffectIterator.hasNext())
                next = sideEffectIterator.next();
        }

        if (null == next) throw new NoSuchElementException();

        // force a reset of the latch as the iterator has moved forward and checking for the next batch can be
        // re-enabled
        latch = false;

        return next;
    }

    private String getAggregatorType(final Object o) {
        if (o instanceof BulkSet)
            return Tokens.VAL_AGGREGATE_TO_BULKSET;
        else if (o instanceof Iterable || o instanceof Iterator)
            return Tokens.VAL_AGGREGATE_TO_LIST;
        else if (o instanceof Map)
            return Tokens.VAL_AGGREGATE_TO_MAP;
        else
            return Tokens.VAL_AGGREGATE_TO_NONE;
    }

    static class BulkResultIterator implements Iterator {

        private final Iterator<Map.Entry<Object,Long>> itty;

        public BulkResultIterator(final BulkSet<Object> bulkSet) {
            itty = bulkSet.asBulk().entrySet().iterator();
        }

        @Override
        public boolean hasNext() {
            return itty.hasNext();
        }

        @Override
        public Object next() {
            final Map.Entry<Object, Long> entry = itty.next();
            return new RemoteTraverser<>(entry.getKey(), entry.getValue());
        }
    }
}
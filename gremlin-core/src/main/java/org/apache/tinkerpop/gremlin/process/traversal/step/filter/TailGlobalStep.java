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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.Bypassing;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Matt Frantz (http://github.com/mhfrantz)
 */
public final class TailGlobalStep<S> extends AbstractStep<S, S> implements Bypassing, Barrier<TraverserSet<S>> {

    private final GValue<Long> limit;
    private Deque<Traverser.Admin<S>> tail;
    private long tailBulk = 0L;
    private boolean bypass = false;

    public TailGlobalStep(final Traversal.Admin traversal, final long limit) {
        this(traversal, GValue.ofLong(null, limit));
    }

    public TailGlobalStep(final Traversal.Admin traversal, final GValue<Long> limit) {
        super(traversal);
        this.limit = limit;
        this.tail = new ArrayDeque<>(limit.get().intValue());
    }

    public void setBypass(final boolean bypass) {
        this.bypass = bypass;
    }

    @Override
    public Traverser.Admin<S> processNextStart() {
        if (this.bypass) {
            // If we are bypassing this step, let everything through.
            return this.starts.next();
        } else {
            // Pull everything available before we start delivering from the tail buffer.
            if (this.starts.hasNext()) {
                this.starts.forEachRemaining(this::addTail);
            }
            // Pull the oldest traverser from the tail buffer.
            final Traverser.Admin<S> oldest = this.tail.pop();
            // Trim any excess from the oldest traverser.
            final long excess = this.tailBulk - this.limit.get();
            if (excess > 0) {
                oldest.setBulk(oldest.bulk() - excess);
                // Account for the loss of excess in the tail buffer
                this.tailBulk -= excess;
            }
            // Account for the loss of bulk in the tail buffer as we emit the oldest traverser.
            this.tailBulk -= oldest.bulk();
            return oldest;
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.tail.clear();
        this.tailBulk = 0L;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.limit.get());
    }

    @Override
    public TailGlobalStep<S> clone() {
        final TailGlobalStep<S> clone = (TailGlobalStep<S>) super.clone();
        clone.tail = new ArrayDeque<>(this.limit.get().intValue());
        clone.tailBulk = 0L;
        return clone;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Long.hashCode(this.limit.get());
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.BULK);
    }

    private void addTail(Traverser.Admin<S> start) {
        // Calculate the tail bulk including this new start.
        this.tailBulk += start.bulk();
        // Evict from the tail buffer until we have enough room.
        while (!this.tail.isEmpty()) {
            final Traverser.Admin<S> oldest = this.tail.getFirst();
            final long bulk = oldest.bulk();
            if (this.tailBulk - bulk < limit.get())
                break;
            this.tail.pop();
            this.tailBulk -= bulk;
        }
        this.tail.add(start);
    }


    @Override
    public MemoryComputeKey<TraverserSet<S>> getMemoryComputeKey() {
        return MemoryComputeKey.of(this.getId(), new RangeGlobalStep.RangeBiOperator<>(this.limit.get()), false, true);
    }

    @Override
    public void processAllStarts() {

    }

    @Override
    public boolean hasNextBarrier() {
        return this.starts.hasNext();
    }

    @Override
    public TraverserSet<S> nextBarrier() throws NoSuchElementException {
        if (!this.starts.hasNext())
            throw FastNoSuchElementException.instance();
        final TraverserSet<S> barrier = (TraverserSet<S>) this.traversal.getTraverserSetSupplier().get();
        while (this.starts.hasNext()) {
            barrier.add(this.starts.next());
        }
        return barrier;
    }

    @Override
    public void addBarrier(final TraverserSet<S> barrier) {
        IteratorUtils.removeOnNext(barrier.iterator()).forEachRemaining(traverser -> {
            traverser.setSideEffects(this.getTraversal().getSideEffects());
            this.addStart(traverser);
        });
    }
}

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

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BinaryOperator;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Bypassing;
import org.apache.tinkerpop.gremlin.process.traversal.step.FilteringBarrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.Ranging;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

/**
 * @author Bob Briody (http://bobbriody.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RangeGlobalStep<S> extends FilterStep<S> implements Ranging, Bypassing, FilteringBarrier<TraverserSet<S>>, RangeGlobalStepContract<S> {

    private long low;
    private long high;
    private AtomicLong counter = new AtomicLong(0l);
    private boolean bypass;
    
    // Per-iteration counter tracking for repeat steps
    private Map<String, AtomicLong> perIterationCounters = new HashMap<>();
    private boolean usePerIterationCounters = false;

    public RangeGlobalStep(final Traversal.Admin traversal, final long low, final long high) {
        super(traversal);
        if (low != -1 && high != -1 && low > high) {
            throw new IllegalArgumentException("Not a legal range: [" + low + ", " + high + ']');
        }
        this.low = low;
        this.high = high;
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        System.out.printf("Filter called: %s loops=%d%n", traverser.path(), traverser.loops());
        if (this.bypass) return true;

        // Determine which counter to use
        AtomicLong currentCounter = this.counter;
        if (usePerIterationCounters) {
            // Find the parent repeat step and use its loop context
            String repeatStepId = null;
            int repeatLoops = 0;
            
            Traversal.Admin<?,?> t = this.traversal;
            while (!t.isRoot()) {
                TraversalParent pt = t.getParent();
                Step<?, ?> ps = pt.asStep();
                if (ps.getClass().getSimpleName().equals("RepeatStep")) {
                    repeatStepId = ps.getId();
                    if (traverser.getLoopNames().contains(repeatStepId)) {
                        repeatLoops = traverser.loops(repeatStepId);
                    }
                    break;
                }
                t = ps.getTraversal();
            }
            
            // Create key based on repeat step and its current iteration
            String iterationKey = repeatStepId + ":" + repeatLoops;
            currentCounter = perIterationCounters.computeIfAbsent(iterationKey, k -> new AtomicLong(0L));
            System.out.printf("IterationKey: %s RepeatLoops: %d Counter: %d Path: %s%n", iterationKey, repeatLoops, currentCounter.get(), traverser.path());
        }

        System.out.printf("Traverser: %s%n", traverser);
        if (this.high != -1 && currentCounter.get() >= this.high) {
            if (usePerIterationCounters) {
                 System.out.printf("Filter false for Traverser: %s Counter: %d%n", traverser.path(), currentCounter.get());
                return false;
            }
            // System.out.printf("FastNoSuchElementException for Traverser: %s Counter: %d%n", traverser.path(), currentCounter.get());
            throw FastNoSuchElementException.instance();
        }

        long avail = traverser.bulk();
        if (currentCounter.get() + avail <= this.low) {
            // Will not surpass the low w/ this traverser. Skip and filter the whole thing.
            currentCounter.getAndAdd(avail);
            // System.out.printf("False for Traverser: %s Counter: %d%n", traverser.path(), currentCounter.get());
            return false;
        }

        // Skip for the low and trim for the high. Both can happen at once.

        long toSkip = 0;
        if (currentCounter.get() < this.low) {
            toSkip = this.low - currentCounter.get();
        }

        long toTrim = 0;
        if (this.high != -1 && currentCounter.get() + avail >= this.high) {
            toTrim = currentCounter.get() + avail - this.high;
        }

        long toEmit = avail - toSkip - toTrim;
        currentCounter.getAndAdd(toSkip + toEmit);
        traverser.setBulk(toEmit);

        return true;
    }

    @Override
    public void reset() {
        super.reset();
        this.counter.set(0l);
        this.perIterationCounters.clear();
    }

    /**
     * Enables per-iteration counter tracking for use within repeat steps.
     * When enabled, separate counters are maintained for each repeat iteration.
     */
    public void enablePerIterationCounters() {
        this.usePerIterationCounters = true;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.low, this.high);
    }

    @Override
    public Long getLowRange() {
        return this.low;
    }

    @Override
    public Long getHighRange() {
        return this.high;
    }

    @Override
    public RangeGlobalStep<S> clone() {
        final RangeGlobalStep<S> clone = (RangeGlobalStep<S>) super.clone();
        clone.counter = new AtomicLong(0l);
        clone.perIterationCounters = new HashMap<>();
        clone.usePerIterationCounters = this.usePerIterationCounters;
        return clone;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Long.hashCode(this.low) ^ Long.hashCode(this.high);
    }

    @Override
    public boolean equals(final Object other) {
        if (super.equals(other)) {
            final RangeGlobalStep typedOther = (RangeGlobalStep) other;
            return typedOther.low == this.low && typedOther.high == this.high;
        }
        return false;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        if (usePerIterationCounters) {
            final Set<TraverserRequirement> requirements = new HashSet<>();
            requirements.add(TraverserRequirement.BULK);
            requirements.add(TraverserRequirement.NESTED_LOOP);
            return requirements;
        }
        return Collections.singleton(TraverserRequirement.BULK);
    }


    @Override
    public MemoryComputeKey<TraverserSet<S>> getMemoryComputeKey() {
        return MemoryComputeKey.of(this.getId(), new RangeBiOperator<>(this.high), false, true);
    }

    @Override
    public void setBypass(final boolean bypass) {
        this.bypass = bypass;
    }

    @Override
    public void processAllStarts() {

    }

    @Override
    public TraverserSet<S> getEmptyBarrier() {
        return new TraverserSet<>();
    }

    @Override
    public boolean hasNextBarrier() {
        return this.starts.hasNext();
    }

    @Override
    public TraverserSet<S> nextBarrier() throws NoSuchElementException {
        if(!this.starts.hasNext())
            throw FastNoSuchElementException.instance();
        final TraverserSet<S> barrier = (TraverserSet<S>) this.traversal.getTraverserSetSupplier().get();
        while (this.starts.hasNext()) {
            barrier.add(this.starts.next());
        }
        return barrier;
    }

    @Override
    public void addBarrier(final TraverserSet<S> barrier) {
        System.out.printf("=== addBarrier called with %d traversers ===%n", barrier.size());
        IteratorUtils.removeOnNext(barrier.iterator()).forEachRemaining(traverser -> {
            System.out.printf("Barrier traverser: %s loops=%d%n", traverser.path(), traverser.loops());
            traverser.setSideEffects(this.getTraversal().getSideEffects());
            this.addStart(traverser);
        });
    }

    ////////////////

    public static final class RangeBiOperator<S> implements BinaryOperator<TraverserSet<S>>, Serializable {

        private final long highRange;

        public RangeBiOperator() {
            this(-1);
        }

        public RangeBiOperator(final long highRange) {
            this.highRange = highRange;
        }

        @Override
        public TraverserSet<S> apply(final TraverserSet<S> mutatingSeed, final TraverserSet<S> set) {
            if (this.highRange == -1 || mutatingSeed.size() < this.highRange)
                mutatingSeed.addAll(set);
            return mutatingSeed;
        }
    }
}

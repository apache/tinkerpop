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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BinaryOperator;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * @author Bob Briody (http://bobbriody.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RangeGlobalStep<S> extends FilterStep<S> implements RangeGlobalStepContract<S> {

    private long low;
    private long high;
    /**
     * Flag to indicate if the step is inside a repeat loop. Can be null if the value has not been initialized yet as 
     * the traversal has not been finalized.
     */
    private Boolean insideLoop;
    /**
     * Single counter if this range step is not inside a loop
     */
    private AtomicLong singleCounter = new AtomicLong(0);
    /**
     * If this range step is used inside a loop there can be multiple loop counters
     */
    private Map<String, AtomicLong> loopCounters = new HashMap<>();
    private boolean bypass;

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
        if (this.bypass) return true;

        final AtomicLong counter = getCounter(traverser);

        if (this.high != -1 && counter.get() >= this.high) {
            if (isInsideLoop()) {
                return false;
            }
            throw FastNoSuchElementException.instance();
        }

        long avail = traverser.bulk();
        if (counter.get() + avail <= this.low) {
            // Will not surpass the low w/ this traverser. Skip and filter the whole thing.
            counter.getAndAdd(avail);
            return false;
        }

        // Skip for the low and trim for the high. Both can happen at once.

        long toSkip = 0;
        if (counter.get() < this.low) {
            toSkip = this.low - counter.get();
        }

        long toTrim = 0;
        if (this.high != -1 && counter.get() + avail >= this.high) {
            toTrim = counter.get() + avail - this.high;
        }

        long toEmit = avail - toSkip - toTrim;
        counter.getAndAdd(toSkip + toEmit);
        traverser.setBulk(toEmit);

        return true;
    }

    @Override
    public void reset() {
        super.reset();
        this.singleCounter.set(0);
        this.loopCounters.clear();
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
        clone.singleCounter = new AtomicLong(0);
        clone.loopCounters = new HashMap<>();
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
        return Collections.singleton(TraverserRequirement.BULK);
    }

    @Override
    public void setBypass(final boolean bypass) {
        this.bypass = bypass;
    }

    @Override
    public void processAllStarts() {

    }

    private AtomicLong getCounter(final Traverser.Admin<S> traverser) {
        if (isInsideLoop()) {
            final String counterKey = getCounterKey(traverser);
            return loopCounters.computeIfAbsent(counterKey, k -> new AtomicLong(0L));
        } else {
            return this.singleCounter;
        }
    }

    /**
     * This will initialize the insideLoop flag if it hasn't been set by analyzing the traversal up to the root and 
     * should only be called after the traversal has been finalized.
     * 
     * @return if the step is being used inside a repeat loop.
     */
    private boolean isInsideLoop() {
        if (this.insideLoop == null) {
            this.insideLoop = TraversalHelper.hasRepeatStepParent(this.getTraversal());
        }
        return this.insideLoop;
    }

    private String getCounterKey(final Traverser.Admin<S> traverser) {
        final List<String> counterKeyParts = new ArrayList<>();
        Traversal.Admin<Object, Object> traversal = this.getTraversal();
        if (isInsideLoop()) {
            // the range step is inside a loop so we need to track counters per iteration
            // using a counter key that is composed of the parent steps to the root
            while (!traversal.isRoot()) {
                final TraversalParent pt = traversal.getParent();
                final Step<?, ?> ps = pt.asStep();
                final String pid = ps.getId();
                if (traverser.getLoopNames().contains(pid)) {
                    counterKeyParts.add(pid);
                    counterKeyParts.add(String.valueOf(traverser.loops(pid)));
                }
                traversal = ps.getTraversal();
            }
        }
        // reverse added parts so that it starts from root
        Collections.reverse(counterKeyParts);
        counterKeyParts.add(this.getId());
        if (traverser.getLoopNames().contains(this.getId())) {
            counterKeyParts.add(String.valueOf(traverser.loops(this.getId())));
        }
        return String.join(":", counterKeyParts);
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

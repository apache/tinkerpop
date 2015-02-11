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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.filter;

import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.Ranging;
import org.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Bob Briody (http://bobbriody.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RangeStep<S> extends FilterStep<S> implements Ranging {

    private final long low;
    private final long high;
    private AtomicLong counter = new AtomicLong(0l);

    public RangeStep(final Traversal.Admin traversal, final long low, final long high) {
        super(traversal);
        if (low != -1 && high != -1 && low > high) {
            throw new IllegalArgumentException("Not a legal range: [" + low + ", " + high + ']');
        }
        this.low = low;
        this.high = high;
        RangeStep.generatePredicate(this);
    }

    @Override
    public void reset() {
        super.reset();
        this.counter.set(0l);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.low, this.high);
    }

    public long getLowRange() {
        return this.low;
    }

    public long getHighRange() {
        return this.high;
    }

    @Override
    public RangeStep<S> clone() throws CloneNotSupportedException {
        final RangeStep<S> clone = (RangeStep<S>) super.clone();
        clone.counter = new AtomicLong(0l);
        RangeStep.generatePredicate(clone);
        return clone;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.BULK);
    }

    /////////////////////////////

    private static final <S> void generatePredicate(final RangeStep<S> rangeStep) {
        rangeStep.setPredicate(traverser -> {
            if (rangeStep.high != -1 && rangeStep.counter.get() >= rangeStep.high) {
                throw FastNoSuchElementException.instance();
            }

            long avail = traverser.bulk();
            if (rangeStep.counter.get() + avail <= rangeStep.low) {
                // Will not surpass the low w/ this traverser. Skip and filter the whole thing.
                rangeStep.counter.getAndAdd(avail);
                return false;
            }

            // Skip for the low and trim for the high. Both can happen at once.

            long toSkip = 0;
            if (rangeStep.counter.get() < rangeStep.low) {
                toSkip = rangeStep.low - rangeStep.counter.get();
            }

            long toTrim = 0;
            if (rangeStep.high != -1 && rangeStep.counter.get() + avail >= rangeStep.high) {
                toTrim = rangeStep.counter.get() + avail - rangeStep.high;
            }

            long toEmit = avail - toSkip - toTrim;
            rangeStep.counter.getAndAdd(toSkip + toEmit);
            traverser.asAdmin().setBulk(toEmit);

            return true;
        });
    }
}

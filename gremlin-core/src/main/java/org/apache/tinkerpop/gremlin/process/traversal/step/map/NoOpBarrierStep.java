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

package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.LocalBarrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class NoOpBarrierStep<S> extends AbstractStep<S, S> implements LocalBarrier<S> {

    private int maxBarrierSize;
    private TraverserSet<S> barrier;

    public NoOpBarrierStep(final Traversal.Admin traversal) {
        this(traversal, Integer.MAX_VALUE);
    }

    public NoOpBarrierStep(final Traversal.Admin traversal, final int maxBarrierSize) {
        super(traversal);
        this.maxBarrierSize = maxBarrierSize;
        this.barrier = (TraverserSet<S>) this.traversal.getTraverserSetSupplier().get();
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        if (this.barrier.isEmpty())
            this.processAllStarts();
        return this.barrier.remove();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.BULK);
    }

    @Override
    public void processAllStarts() {
        while (this.starts.hasNext() && (this.maxBarrierSize == Integer.MAX_VALUE || this.barrier.size() < this.maxBarrierSize)) {
            final Traverser.Admin<S> traverser = this.starts.next();
            traverser.setStepId(this.getNextStep().getId()); // when barrier is reloaded, the traversers should be at the next step
            this.barrier.add(traverser);
        }
    }

    @Override
    public boolean hasNextBarrier() {
        this.processAllStarts();
        return !this.barrier.isEmpty();
    }

    @Override
    public TraverserSet<S> nextBarrier() throws NoSuchElementException {
        this.processAllStarts();
        if (this.barrier.isEmpty())
            throw FastNoSuchElementException.instance();
        else {
            final TraverserSet<S> temp = this.barrier;
            this.barrier = (TraverserSet<S>) this.traversal.getTraverserSetSupplier().get();
            return temp;
        }
    }

    @Override
    public void addBarrier(final TraverserSet<S> barrier) {
        this.barrier.addAll(barrier);
    }

    @Override
    public NoOpBarrierStep<S> clone() {
        final NoOpBarrierStep<S> clone = (NoOpBarrierStep<S>) super.clone();
        clone.barrier = (TraverserSet<S>) this.traversal.getTraverserSetSupplier().get();
        return clone;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.maxBarrierSize == Integer.MAX_VALUE ? null : this.maxBarrierSize);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.maxBarrierSize;
    }

    @Override
    public void reset() {
        super.reset();
        this.barrier.clear();
    }
}

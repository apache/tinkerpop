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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.LocalBarrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

import java.util.NoSuchElementException;

public abstract class SideEffectBarrierStep<S> extends AbstractStep<S, S> implements LocalBarrier<S> {
    protected TraverserSet<S> barrier;

    public SideEffectBarrierStep(final Traversal.Admin traversal) {
        super(traversal);
        this.barrier = (TraverserSet<S>) this.traversal.getTraverserSetSupplier().get();
    }

    protected abstract void sideEffect(final Traverser.Admin<S> traverser);

    @Override
    protected Traverser.Admin<S> processNextStart() {
        if (this.barrier.isEmpty()) {
            this.processAllStarts();
        }
        return this.barrier.remove();
    }

    @Override
    public void processAllStarts() {
        while (this.starts.hasNext()) {
            final Traverser.Admin<S> traverser = this.starts.next();
            sideEffect(traverser);

            traverser.setStepId(this.getNextStep().getId());
            // when barrier is reloaded, the traversers should be at the next step
            this.barrier.add(traverser);
        }
    }

    @Override
    public boolean hasNextBarrier() {
        if (this.barrier.isEmpty()) {
            this.processAllStarts();
        }
        return !this.barrier.isEmpty();
    }

    @Override
    public TraverserSet<S> nextBarrier() throws NoSuchElementException {
        if (this.barrier.isEmpty()) {
            this.processAllStarts();
        }
        if (this.barrier.isEmpty())
            throw FastNoSuchElementException.instance();
        else {
            final TraverserSet<S> temp = this.barrier;
            this.barrier = (TraverserSet<S>) this.traversal.getTraverserSetSupplier().get();
            return temp;
        }
    }

    @Override
    public void addBarrier(TraverserSet<S> barrier) {
        this.barrier.addAll(barrier);
    }

    @Override
    public void reset() {
        super.reset();
        this.barrier.clear();
    }

    @Override
    public SideEffectBarrierStep<S> clone() {
        final SideEffectBarrierStep<S> clone = (SideEffectBarrierStep<S>) super.clone();
        clone.barrier = (TraverserSet<S>) this.traversal.getTraverserSetSupplier().get();
        return clone;
    }
}

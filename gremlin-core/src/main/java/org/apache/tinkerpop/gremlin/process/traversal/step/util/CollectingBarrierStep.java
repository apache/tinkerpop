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
package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.ProjectedTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.BinaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class CollectingBarrierStep<S> extends AbstractStep<S, S> implements Barrier<TraverserSet<S>> {

    protected TraverserSet<S> traverserSet = new TraverserSet<>();
    private int maxBarrierSize;
    private boolean barrierConsumed = false;

    public CollectingBarrierStep(final Traversal.Admin traversal) {
        this(traversal, Integer.MAX_VALUE);
    }

    public CollectingBarrierStep(final Traversal.Admin traversal, final int maxBarrierSize) {
        super(traversal);
        this.maxBarrierSize = maxBarrierSize;
    }

    public abstract void barrierConsumer(final TraverserSet<S> traverserSet);

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.BULK);
    }

    @Override
    public void processAllStarts() {
        if (this.starts.hasNext()) {
            if (Integer.MAX_VALUE == this.maxBarrierSize) {
                this.starts.forEachRemaining(this.traverserSet::add);
            } else {
                while (this.starts.hasNext() && this.traverserSet.size() < this.maxBarrierSize) {
                    this.traverserSet.add(this.starts.next());
                }
            }
        }
    }

    @Override
    public boolean hasNextBarrier() {
        this.processAllStarts();
        return !this.traverserSet.isEmpty();
    }

    @Override
    public TraverserSet<S> nextBarrier() throws NoSuchElementException {
        this.processAllStarts();
        if (this.traverserSet.isEmpty())
            throw FastNoSuchElementException.instance();
        else {
            final TraverserSet<S> temp = new TraverserSet<>();
            IteratorUtils.removeOnNext(this.traverserSet.iterator()).forEachRemaining(t -> {
                DetachedFactory.detach(t, true); // this should be dynamic
                temp.add(t);
            });
            return temp;
        }
    }

    @Override
    public void addBarrier(final TraverserSet<S> barrier) {
        this.traverserSet = barrier;
        this.traverserSet.forEach(traverser -> traverser.setSideEffects(this.getTraversal().getSideEffects()));
        this.barrierConsumed = false;
    }

    @Override
    public Traverser.Admin<S> processNextStart() {
        if (this.traverserSet.isEmpty() && this.starts.hasNext()) {
            this.processAllStarts();
            this.barrierConsumed = false;
        }
        //
        if (!this.barrierConsumed) {
            this.barrierConsumer(this.traverserSet);
            this.barrierConsumed = true;
        }
        return ProjectedTraverser.tryUnwrap(this.traverserSet.remove());
    }

    @Override
    public CollectingBarrierStep<S> clone() {
        final CollectingBarrierStep<S> clone = (CollectingBarrierStep<S>) super.clone();
        clone.traverserSet = new TraverserSet<>();
        clone.barrierConsumed = false;
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
        this.traverserSet.clear();
    }

    @Override
    public MemoryComputeKey<TraverserSet<S>> getMemoryComputeKey() {
        return MemoryComputeKey.of(this.getId(), (BinaryOperator) Operator.addAll, false, true);
    }
}

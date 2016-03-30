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
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.Generating;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

import java.util.Iterator;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */

public abstract class ReducingBarrierStep<S, E> extends AbstractStep<S, E> implements Barrier<E>, Generating<E, E> {

    protected Supplier<E> seedSupplier;
    protected BinaryOperator<E> reducingBiOperator;
    private boolean done = false;
    private E seed = null;
    protected boolean onGraphComputer = false;

    public ReducingBarrierStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    public void setSeedSupplier(final Supplier<E> seedSupplier) {
        this.seedSupplier = seedSupplier;
    }

    public Supplier<E> getSeedSupplier() {
        return this.seedSupplier;
    }

    public abstract E projectTraverser(final Traverser.Admin<S> traverser);

    public void setReducingBiOperator(final BinaryOperator<E> reducingBiOperator) {
        this.reducingBiOperator = reducingBiOperator;
    }

    public BinaryOperator<E> getBiOperator() {
        return this.reducingBiOperator;
    }

    public void reset() {
        super.reset();
        this.done = false;
        this.seed = null;
    }

    @Override
    public void addStarts(final Iterator<Traverser.Admin<S>> starts) {
        if (starts.hasNext()) {
            this.done = false;
            super.addStarts(starts);
        }
    }

    @Override
    public void done() {
        this.done = true;
    }

    @Override
    public void addStart(final Traverser.Admin<S> start) {
        this.done = false;
        super.addStart(start);
    }

    @Override
    public void processAllStarts() {
        if (this.seed == null) this.seed = this.seedSupplier.get();
        while (this.starts.hasNext())
            this.seed = this.reducingBiOperator.apply(this.seed, this.projectTraverser(this.starts.next()));
    }

    @Override
    public boolean hasNextBarrier() {
        return !this.done;
    }

    @Override
    public E nextBarrier() {
        if (this.done)
            throw FastNoSuchElementException.instance();
        else {
            processAllStarts();
            final E temp = this.seed;
            this.seed = null;
            this.done = true;
            return temp;
        }
    }

    @Override
    public void addBarrier(final E barrier) {
        this.done = false;
        this.seed = null == this.seed ?
                barrier :
                this.reducingBiOperator.apply(this.seed, barrier);
    }

    @Override
    public Traverser.Admin<E> processNextStart() {
        if (this.done)
            throw FastNoSuchElementException.instance();
        this.processAllStarts();
        this.done = true;
        final Traverser.Admin<E> traverser = this.getTraversal().getTraverserGenerator().generate(this.generateFinalResult(this.seed), (Step<E, E>) this, 1l);
        this.seed = null;
        return traverser;
    }

    @Override
    public ReducingBarrierStep<S, E> clone() {
        final ReducingBarrierStep<S, E> clone = (ReducingBarrierStep<S, E>) super.clone();
        clone.done = false;
        clone.seed = null;
        return clone;
    }

    @Override
    public MemoryComputeKey<E> getMemoryComputeKey() {
        return MemoryComputeKey.of(this.getId(), this.getBiOperator(), false, true);
    }

}

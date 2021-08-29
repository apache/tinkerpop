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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SumGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

import java.io.Serializable;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class ReducingBarrierStep<S, E> extends AbstractStep<S, E> implements Barrier<E>, Generating<E, E> {

    /**
     * A seed value not to be emitted from the {@code ReducingBarrierStep} helping with flow control within this step.
     */
    public static final Object NON_EMITTING_SEED = NonEmittingSeed.INSTANCE;

    /**
     * If the {@code seedSupplier} is {@code null} then the default behavior is to generate the seed from the starts.
     * This supplier must be callable as a constant and not rely on state from the class. Prefer overriding
     * {@link #generateSeedFromStarts()} otherwise.
     */
    protected Supplier<E> seedSupplier = null;
    protected BinaryOperator<E> reducingBiOperator;
    protected boolean hasProcessedOnce = false;

    private E seed = (E) NON_EMITTING_SEED;

    public ReducingBarrierStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    public void setSeedSupplier(final Supplier<E> seedSupplier) {
        this.seedSupplier = seedSupplier;
    }

    /**
     * Gets the provided seed supplier or provides {@link #generateSeedFromStarts()}.
     */
    public Supplier<E> getSeedSupplier() {
        return Optional.ofNullable(this.seedSupplier).orElse(this::generateSeedFromStarts);
    }

    /**
     * If the {@code seedSupplier} is {@code null} then this method is called.
     */
    protected E generateSeedFromStarts() {
        return starts.hasNext() ? projectTraverser(this.starts.next()) : null;
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
        this.hasProcessedOnce = false;
        this.seed = (E) NON_EMITTING_SEED;
    }

    @Override
    public void done() {
        this.hasProcessedOnce = true;
        this.seed = (E) NON_EMITTING_SEED;
    }

    @Override
    public void processAllStarts() {
        if (this.hasProcessedOnce && !this.starts.hasNext())
            return;
        this.hasProcessedOnce = true;

        if (this.seed == NON_EMITTING_SEED) {
            this.seed = getSeedSupplier().get();
        }

        while (this.starts.hasNext())
            this.seed = this.reducingBiOperator.apply(this.seed, this.projectTraverser(this.starts.next()));
    }

    @Override
    public boolean hasNextBarrier() {
        this.processAllStarts();
        return NON_EMITTING_SEED != this.seed;
    }

    @Override
    public E nextBarrier() {
        if (!this.hasNextBarrier())
            throw FastNoSuchElementException.instance();
        else {
            final E temp = this.seed;
            this.seed = (E) NON_EMITTING_SEED;
            return temp;
        }
    }

    @Override
    public void addBarrier(final E barrier) {
        this.seed = NON_EMITTING_SEED == this.seed ?
                barrier :
                this.reducingBiOperator.apply(this.seed, barrier);
    }

    @Override
    public Traverser.Admin<E> processNextStart() {
        this.processAllStarts();
        if (this.seed == NON_EMITTING_SEED) throw FastNoSuchElementException.instance();
        final Traverser.Admin<E> traverser = this.getTraversal().getTraverserGenerator().generate(this.generateFinalResult(this.seed), (Step<E, E>) this, 1l);
        this.seed = (E) NON_EMITTING_SEED;
        return traverser;
    }

    @Override
    public ReducingBarrierStep<S, E> clone() {
        final ReducingBarrierStep<S, E> clone = (ReducingBarrierStep<S, E>) super.clone();
        clone.hasProcessedOnce = false;
        clone.seed = (E) NON_EMITTING_SEED;
        return clone;
    }

    @Override
    public MemoryComputeKey<E> getMemoryComputeKey() {
        return MemoryComputeKey.of(this.getId(), this.getBiOperator(), false, true);
    }

    public static final class NonEmittingSeed implements Serializable {
        public static final NonEmittingSeed INSTANCE = new NonEmittingSeed();
        private NonEmittingSeed() {}
    }
}

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
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.BinaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class SupplyingBarrierStep<S, E> extends AbstractStep<S, E> implements Barrier<Boolean> {

    private boolean done = false;

    public SupplyingBarrierStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    protected abstract E supply();

    @Override
    public void addStarts(final Iterator<Traverser.Admin<S>> starts) {
        if (starts.hasNext()) {
            this.done = false;
            super.addStarts(starts);
        }
    }

    @Override
    public void addStart(final Traverser.Admin<S> start) {
        this.done = false;
        super.addStart(start);
    }

    @Override
    public void reset() {
        super.reset();
        this.done = false;
    }

    @Override
    public Traverser.Admin<E> processNextStart() {
        if (this.done)
            throw FastNoSuchElementException.instance();
        this.processAllStarts();
        this.done = true;
        return this.getTraversal().asAdmin().getTraverserGenerator().generate(this.supply(), (Step<E, E>) this, 1l);
    }

    @Override
    public SupplyingBarrierStep<S, E> clone() {
        final SupplyingBarrierStep<S, E> clone = (SupplyingBarrierStep<S, E>) super.clone();
        clone.done = false;
        return clone;
    }

    public void processAllStarts() {
        while (this.starts.hasNext())
            this.starts.next();
    }

    @Override
    public boolean hasNextBarrier() {
        return !this.done;
    }

    @Override
    public Boolean nextBarrier() throws NoSuchElementException {
        this.processAllStarts();
        this.done = true;
        return true;
    }

    @Override
    public void addBarrier(final Boolean barrier) {
        this.done = false;
    }

    @Override
    public void done() {
        this.done = true;
    }

    @Override
    public MemoryComputeKey<Boolean> getMemoryComputeKey() {
        return MemoryComputeKey.of(this.getId(), (BinaryOperator) Operator.and, false, true);
    }
}

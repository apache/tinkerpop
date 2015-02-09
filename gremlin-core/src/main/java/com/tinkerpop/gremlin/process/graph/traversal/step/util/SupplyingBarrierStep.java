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
package com.tinkerpop.gremlin.process.graph.traversal.step.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.traversal.step.AbstractStep;
import com.tinkerpop.gremlin.process.FastNoSuchElementException;

import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class SupplyingBarrierStep<S, E> extends AbstractStep<S, E> {

    public Supplier<E> supplier;
    private boolean done = false;

    public SupplyingBarrierStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    public void setSupplier(final Supplier<E> supplier) {
        this.supplier = supplier;
    }

    @Override
    public void reset() {
        super.reset();
        this.done = false;
    }

    @Override
    public Traverser<E> processNextStart() {
        if (this.done)
            throw FastNoSuchElementException.instance();
        while (this.starts.hasNext())
            this.starts.next();
        this.done = true;
        return this.getTraversal().asAdmin().getTraverserGenerator().generate(this.supplier.get(), (Step) this, 1l);
    }

    @Override
    public SupplyingBarrierStep<S, E> clone() throws CloneNotSupportedException {
        final SupplyingBarrierStep<S, E> clone = (SupplyingBarrierStep<S, E>) super.clone();
        clone.done = false;
        return clone;
    }


}

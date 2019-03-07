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
package org.apache.tinkerpop.machine.pipes;

import org.apache.tinkerpop.machine.functions.CFunction;
import org.apache.tinkerpop.machine.traversers.Traverser;
import org.apache.tinkerpop.machine.traversers.TraverserSet;
import org.apache.tinkerpop.machine.util.FastNoSuchElementException;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractStep<C, S, E> implements Iterator<Traverser<C, E>> {

    protected final CFunction<C> function;
    protected final AbstractStep previousStep;
    protected TraverserSet<C, S> traverserSet = new TraverserSet<>();

    public AbstractStep(final AbstractStep<C, ?, S> previousStep, final CFunction<C> function) {
        this.previousStep = previousStep;
        this.function = function;
    }

    public void addTraverser(final Traverser<C, S> traverser) {
        this.traverserSet.add(traverser);
    }

    @Override
    public boolean hasNext() {
        return !this.traverserSet.isEmpty() || this.previousStep.hasNext();
    }

    @Override
    public abstract Traverser<C, E> next();

    protected Traverser<C, S> getNextTraverser() {
        Traverser<C, S> traverser = null;
        if (!this.traverserSet.isEmpty())
            traverser = this.traverserSet.remove();
        else if (this.previousStep.hasNext())
            traverser = this.previousStep.next();
        if (null == traverser)
            throw FastNoSuchElementException.instance();
        return traverser;
    }

    @Override
    public String toString() {
        return this.function.toString();
    }
}

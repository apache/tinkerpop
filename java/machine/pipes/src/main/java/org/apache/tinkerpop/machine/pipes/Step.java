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
import org.apache.tinkerpop.machine.functions.FilterFunction;
import org.apache.tinkerpop.machine.functions.InitialFunction;
import org.apache.tinkerpop.machine.functions.MapFunction;
import org.apache.tinkerpop.machine.traversers.Traverser;
import org.apache.tinkerpop.machine.util.FastNoSuchElementException;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Step<C, S, E> implements Iterator<Traverser<?, E>> {

    private final CFunction<C> function;
    private final Step previousStep;
    private Iterator<Traverser<C, E>> currentIterator = null;

    public Step(final Step previousStep, final CFunction<C> function) {
        this.previousStep = previousStep;
        this.function = function;
        if (this.function instanceof InitialFunction) {
            this.currentIterator = ((InitialFunction<C, E>) this.function).get();
        }
    }

    @Override
    public boolean hasNext() {
        if (null != this.currentIterator)
            return this.currentIterator.hasNext();
        else return this.previousStep.hasNext();
    }

    @Override
    public Traverser<?, E> next() {
        if (null != this.currentIterator && this.currentIterator.hasNext())
            return this.currentIterator.next();
        if (!this.previousStep.hasNext())
            throw FastNoSuchElementException.instance();

        if (this.function instanceof MapFunction) {
            return ((MapFunction<C, ?, E>) this.function).apply(this.previousStep.next());
        } else {
            Traverser<C, E> traverser = null;
            while (this.previousStep.hasNext()) {
                traverser = this.previousStep.next();
                if (((FilterFunction<C, E>) this.function).test(traverser))
                    return traverser;
            }
        }
        throw FastNoSuchElementException.instance();

    }
}

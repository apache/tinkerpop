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
package org.apache.tinkerpop.machine.processor.rxjava;

import io.reactivex.disposables.Disposable;
import org.apache.tinkerpop.machine.bytecode.compiler.Compilation;
import org.apache.tinkerpop.machine.processor.Processor;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.TraverserSet;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractRxJava<C, S, E> implements Processor<C, S, E> {

    static final int MAX_REPETITIONS = 8; // TODO: this needs to be a dynamic configuration

    boolean executed = false;
    Disposable disposable;
    final TraverserSet<C, S> starts = new TraverserSet<>();
    final TraverserSet<C, E> ends = new TraverserSet<>();
    final Compilation<C, S, E> compilation;

    AbstractRxJava(final Compilation<C, S, E> compilation) {
        this.compilation = compilation;
    }

    @Override
    public void addStart(final Traverser<C, S> traverser) {
        this.starts.add(traverser);
    }

    @Override
    public Traverser<C, E> next() {
        this.prepareFlow();
        return this.ends.remove();
    }

    @Override
    public boolean hasNext() {
        this.prepareFlow();
        return !this.ends.isEmpty();
    }

    @Override
    public void reset() {
        if (null != this.disposable)
            this.disposable.dispose();
        this.starts.clear();
        this.ends.clear();
        this.executed = false;
    }

    protected abstract void prepareFlow();

    void waitForCompletionOrResult() {
        while (!this.disposable.isDisposed() && this.ends.isEmpty()) {
            // wait until either the flow is complete or there is a traverser result
        }
    }
}
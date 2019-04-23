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

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractRxJava<C, S, E> implements Processor<C, S, E> {

    static final int MAX_REPETITIONS = 8; // TODO: this needs to be a dynamic configuration

    Disposable disposable;
    final AtomicBoolean running = new AtomicBoolean(Boolean.FALSE);
    final TraverserSet<C, S> starts = new TraverserSet<>();
    private final TraverserSet<C, E> ends = new TraverserSet<>();
    final Compilation<C, S, E> compilation;

    AbstractRxJava(final Compilation<C, S, E> compilation) {
        this.compilation = compilation;
    }

    @Override
    public void stop() {
        if (null != this.disposable) {
            this.disposable.dispose();
            this.disposable = null;
        }
        this.starts.clear();
        this.ends.clear();
        this.running.set(Boolean.FALSE);
    }

    @Override
    public boolean isRunning() {
        return this.running.get();
    }

    @Override
    public Iterator<Traverser<C, E>> iterator(final Iterator<Traverser<C, S>> starts) {
        if (this.isRunning())
            throw Processor.Exceptions.processorIsCurrentlyRunning(this);

        this.running.set(Boolean.TRUE);
        this.starts.clear();
        this.ends.clear();
        starts.forEachRemaining(this.starts::add);
        this.prepareFlow(this.ends::add);
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                waitForCompletionOrResult();
                return !ends.isEmpty();
            }

            @Override
            public Traverser<C, E> next() {
                waitForCompletionOrResult();
                return ends.remove();
            }
        };
    }

    @Override
    public void subscribe(final Iterator<Traverser<C, S>> starts, final Consumer<Traverser<C, E>> consumer) {
        if (this.isRunning())
            throw Processor.Exceptions.processorIsCurrentlyRunning(this);

        this.running.set(Boolean.TRUE);
        this.starts.clear();
        this.ends.clear();
        starts.forEachRemaining(this.starts::add);
        this.prepareFlow(consumer::accept);
    }

    protected abstract void prepareFlow(final io.reactivex.functions.Consumer<? super Traverser<C, E>> consumer);

    private void waitForCompletionOrResult() {
        while (this.ends.isEmpty() && this.isRunning()) {
            // wait until either the flow is complete or there is a traverser result
        }
    }
}
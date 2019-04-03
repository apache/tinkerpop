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

import io.reactivex.Flowable;
import org.apache.tinkerpop.machine.bytecode.compiler.Compilation;
import org.apache.tinkerpop.machine.processor.Processor;
import org.apache.tinkerpop.machine.processor.rxjava.util.TopologyUtil;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.TraverserSet;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RxJava<C, S, E> implements Processor<C, S, E> {

    private final AtomicBoolean alive = new AtomicBoolean(Boolean.TRUE);
    private boolean executed = false;
    private final TraverserSet<C, S> starts = new TraverserSet<>();
    private final TraverserSet<C, E> ends = new TraverserSet<>();
    private final Compilation<C, S, E> compilation;

    public RxJava(final Compilation<C, S, E> compilation) {
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
        this.starts.clear();
        this.ends.clear();
        this.executed = false;
    }

    private void prepareFlow() {
        if (!this.executed) {
            this.executed = true;
            TopologyUtil.compile(Flowable.fromIterable(this.starts), compilation).
                    doOnNext(this.ends::add).
                    doOnComplete(() -> this.alive.set(Boolean.FALSE)).
                    subscribe();
        }
        if (!this.ends.isEmpty())
            return;
        while (this.alive.get()) {
            if (!this.ends.isEmpty())
                return;
        }
    }
}

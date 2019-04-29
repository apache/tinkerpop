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
package org.apache.tinkerpop.machine.processor.util;

import org.apache.tinkerpop.machine.bytecode.compiler.Compilation;
import org.apache.tinkerpop.machine.processor.Processor;
import org.apache.tinkerpop.machine.processor.ProcessorFactory;
import org.apache.tinkerpop.machine.traverser.Traverser;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class EmptyProcessor<C, S, E> implements Processor<C, S, E>, ProcessorFactory {

    private static final EmptyProcessor INSTANCE = new EmptyProcessor();

    private EmptyProcessor() {
        // static instance
    }

    @Override
    public void stop() {

    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public Iterator<Traverser<C, E>> iterator(final Iterator<Traverser<C,S>> starts) {
        return Collections.emptyIterator();
    }

    @Override
    public void subscribe(final Iterator<Traverser<C,S>> starts,  final Consumer<Traverser<C, E>> consumer) {

    }

    @Override
    public <D, A, B> Processor<D, A, B> mint(final Compilation<D, A, B> compilation) {
        return (Processor<D, A, B>) this;
    }

    public static <C, S, E> EmptyProcessor<C, S, E> instance() {
        return INSTANCE;
    }
}

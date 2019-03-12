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
package org.apache.tinkerpop.machine.bytecode;

import org.apache.tinkerpop.machine.functions.CFunction;
import org.apache.tinkerpop.machine.processor.Processor;
import org.apache.tinkerpop.machine.processor.ProcessorFactory;
import org.apache.tinkerpop.machine.traversers.Traverser;
import org.apache.tinkerpop.machine.traversers.TraverserFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Compilation<C, S, E> implements Serializable {

    private final List<CFunction<C>> functions;
    private final ProcessorFactory processorFactory;
    private final TraverserFactory<C> traverserFactory;
    private transient Processor<C, S, E> processor;

    public Compilation(final Bytecode<C> bytecode) {
        BytecodeUtil.strategize(bytecode);
        this.processorFactory = BytecodeUtil.getProcessorFactory(bytecode).get();
        this.traverserFactory = BytecodeUtil.getTraverserFactory(bytecode).get();
        this.functions = BytecodeUtil.compile(bytecode);

    }

    public Processor<C, S, E> getProcessor() {
        this.prepareProcessor();
        return this.processor;
    }

    public void reset() {
        if (null != this.processor)
            this.processor.reset();
    }

    private void prepareProcessor() {
        if (null == this.processor)
            this.processor = this.processorFactory.mint(this.traverserFactory, this.functions);
    }

    public Traverser<C, E> mapTraverser(final Traverser<C, S> traverser) {
        this.reset();
        this.prepareProcessor();
        this.processor.addStart(traverser);
        return this.processor.next();
    }

    public Iterator<Traverser<C, E>> flatMapTraverser(final Traverser<C, S> traverser) {
        this.reset();
        this.prepareProcessor();
        this.processor.addStart(traverser);
        return this.processor;
    }

    public boolean filterTraverser(final Traverser<C, S> traverser) {
        this.reset();
        this.prepareProcessor();
        this.processor.addStart(traverser);
        return this.processor.hasNext();
    }

    @Override
    public String toString() {
        return this.functions.toString();
    }

    public List<CFunction<C>> getFunctions() {
        return this.functions;
    }

    ////////

    public static <C, S, E> Compilation<C, S, E> compile(final Bytecode<C> bytecode) {
        return new Compilation<>(bytecode);
    }

    public static <C, S, E> Compilation<C, S, E> compileOne(final Object arg) {
        return new Compilation<>((Bytecode<C>) arg);
    }

    public static <C, S, E> List<Compilation<C, S, E>> compile(final Object... args) {
        final List<Compilation<C, S, E>> compilations = new ArrayList<>();
        for (final Object arg : args) {
            if (arg instanceof Bytecode)
                compilations.add(new Compilation<>((Bytecode<C>) arg));
        }
        return compilations;
    }
}

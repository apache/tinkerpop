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
package org.apache.tinkerpop.machine.bytecode.compiler;

import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.BytecodeUtil;
import org.apache.tinkerpop.machine.function.CFunction;
import org.apache.tinkerpop.machine.processor.FilterProcessor;
import org.apache.tinkerpop.machine.processor.LoopsProcessor;
import org.apache.tinkerpop.machine.processor.Processor;
import org.apache.tinkerpop.machine.processor.ProcessorFactory;
import org.apache.tinkerpop.machine.structure.EmptyStructure;
import org.apache.tinkerpop.machine.structure.StructureFactory;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.TraverserFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Compilation<C, S, E> implements Serializable {

    private final List<CFunction<C>> functions;
    private final StructureFactory structureFactory;
    private final ProcessorFactory processorFactory;
    private final TraverserFactory<C> traverserFactory;
    private transient Processor<C, S, E> processor;

    public Compilation(final Bytecode<C> bytecode) {
        BytecodeUtil.strategize(bytecode);
        this.structureFactory = BytecodeUtil.getStructureFactory(bytecode).orElse(EmptyStructure.instance());
        this.processorFactory = BytecodeUtil.getProcessorFactory(bytecode).get();
        this.traverserFactory = BytecodeUtil.getTraverserFactory(bytecode).get();
        this.functions = BytecodeUtil.getCompilers(bytecode).compile(bytecode);
    }

    public Compilation(final ProcessorFactory processorFactory) {
        this.functions = Collections.emptyList(); // TODO: somehow strings for primitive processors
        this.structureFactory = null;
        this.processorFactory = processorFactory;
        this.traverserFactory = null;
    }

    public Processor<C, S, E> getProcessor() {
        if (null == this.processor)
            this.processor = this.processorFactory.mint(this);
        return this.processor;
    }

    private void prepareProcessor() {
        if (null == this.processor)
            this.processor = this.processorFactory.mint(this);
        else
            this.processor.reset();
    }

    private Traverser<C, S> prepareTraverser(final Traverser<C, S> traverser) {
        final Traverser<C, S> clone = traverser.clone();
        clone.coefficient().unity();
        return clone;
    }

    public Traverser<C, E> mapTraverser(final Traverser<C, S> traverser) {
        this.prepareProcessor();
        this.processor.addStart(this.prepareTraverser(traverser));
        return this.processor.next();
    }

    public Traverser<C, E> mapObject(final S object) {
        this.prepareProcessor();
        this.processor.addStart(this.traverserFactory.create(this.functions.get(0), object));
        return this.processor.next();
    }

    public Iterator<Traverser<C, E>> flatMapTraverser(final Traverser<C, S> traverser) {
        this.prepareProcessor();
        this.processor.addStart(this.prepareTraverser(traverser));
        return this.processor;
    }

    public boolean filterTraverser(final Traverser<C, S> traverser) {
        this.prepareProcessor();
        this.processor.addStart(this.prepareTraverser(traverser));
        return this.processor.hasNext();
    }

    public Processor<C, S, E> addTraverser(final Traverser<C, S> traverser) {
        this.getProcessor().addStart(traverser);
        return this.processor;
    }

    public List<CFunction<C>> getFunctions() {
        return this.functions;
    }

    public TraverserFactory<C> getTraverserFactory() {
        return this.traverserFactory;
    }

    @Override
    public String toString() {
        return this.functions.toString();
    } // TODO: functions need access to compilations for nesting

    ////////

    public static <C, S, E> Compilation<C, S, E> compile(final Bytecode<C> bytecode) {
        return new Compilation<>(bytecode);
    }

    public static <C, S, E> Compilation<C, S, E> compileOne(final Object arg) {
        return new Compilation<>((Bytecode<C>) arg);
    }

    public static <C, S, E> Compilation<C, S, E> compileOrNull(final int index, final Object... args) {
        return args.length > index && args[index] instanceof Bytecode ?
                new Compilation<>((Bytecode<C>) args[index]) :
                null;
    }

    public static <C, S, E> List<Compilation<C, S, E>> compile(final Object... args) {
        final List<Compilation<C, S, E>> compilations = new ArrayList<>();
        for (final Object arg : args) {
            if (arg instanceof Bytecode)
                compilations.add(new Compilation<>((Bytecode<C>) arg));
        }
        return compilations;
    }

    public static List<Object> repeatCompile(final Object... args) {
        final List<Object> objects = new ArrayList<>();
        for (final Object arg : args) {
            if (arg instanceof Bytecode)
                objects.add(new Compilation<>((Bytecode<?>) arg));
            else if (arg instanceof Character)
                objects.add(arg);
            else if (arg instanceof Integer)
                objects.add(new Compilation<>(new LoopsProcessor<>((int) arg)));
            else if (arg instanceof Boolean)
                objects.add(new Compilation<>(new FilterProcessor<>((boolean) arg)));
        }
        return objects;
    }
}

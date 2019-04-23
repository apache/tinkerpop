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
import org.apache.tinkerpop.machine.processor.EmptyProcessor;
import org.apache.tinkerpop.machine.processor.Processor;
import org.apache.tinkerpop.machine.processor.ProcessorFactory;
import org.apache.tinkerpop.machine.structure.EmptyStructure;
import org.apache.tinkerpop.machine.structure.StructureFactory;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.TraverserFactory;
import org.apache.tinkerpop.machine.util.IteratorUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Compilation<C, S, E> implements Serializable, Cloneable {

    private final Bytecode<C> bytecode;
    private List<CFunction<C>> functions;
    private final StructureFactory structureFactory;
    private final ProcessorFactory processorFactory;
    private final TraverserFactory<C> traverserFactory;
    private transient Processor<C, S, E> processor;

    public Compilation(final Bytecode<C> bytecode) {
        this.bytecode = bytecode;
        BytecodeUtil.strategize(bytecode);
        this.structureFactory = BytecodeUtil.getStructureFactory(bytecode).orElse(EmptyStructure.instance());
        this.processorFactory = BytecodeUtil.getProcessorFactory(bytecode).orElse(EmptyProcessor.instance());
        this.traverserFactory = BytecodeUtil.getTraverserFactory(bytecode).get();
        this.functions = BytecodeUtil.getCompilers(bytecode).compile(bytecode);
    }

    public Compilation(final SourceCompilation<C> source, final Bytecode<C> bytecode) {
        this.bytecode = bytecode;
        BytecodeUtil.mergeSourceInstructions(source.getSourceCode(), bytecode);
        BytecodeUtil.strategize(bytecode, source.getStrategies());
        this.structureFactory = source.getStructureFactory();
        this.processorFactory = source.getProcessorFactory();
        this.traverserFactory = BytecodeUtil.getTraverserFactory(bytecode).get();
        this.functions = source.getCompilers().compile(bytecode);
    }

    public Compilation(final ProcessorFactory processorFactory) {
        this.bytecode = new Bytecode<>();
        this.structureFactory = EmptyStructure.instance();
        this.processorFactory = processorFactory;
        this.traverserFactory = null;
        this.functions = Collections.emptyList(); // TODO: somehow strings for primitive processors
    }

    public Processor<C, S, E> getProcessor() {
        this.prepareProcessor();
        return this.processor;
    }

    private void prepareProcessor() {
        if (null == this.processor)
            this.processor = this.processorFactory.mint(this);
    }

    private Iterator<Traverser<C, S>> prepareTraverser(final Traverser<C, S> traverser) {
        final Traverser<C, S> clone = traverser.clone();
        clone.coefficient().unity();
        return IteratorUtils.of(clone);
    }

    public Traverser<C, E> mapTraverser(final Traverser<C, S> traverser) {
        this.prepareProcessor();
        final Iterator<Traverser<C, E>> iterator = this.processor.iterator(this.prepareTraverser(traverser));
        if (!iterator.hasNext())
            throw new RuntimeException("The nested traversal is not a map function: " + this);
        final Traverser<C, E> result = iterator.next();
        this.processor.stop();
        return result;
    }

    public Traverser<C, E> mapObject(final S object) {
        this.prepareProcessor();
        final Iterator<Traverser<C, E>> iterator = this.processor.iterator(this.prepareTraverser(this.traverserFactory.create(this.functions.get(0), object)));
        final Traverser<C, E> result = iterator.next();
        this.processor.stop();
        return result;
    }

    public Iterator<Traverser<C, E>> flatMapTraverser(final Traverser<C, S> traverser) {
        this.prepareProcessor();
        final Iterator<Traverser<C, E>> iterator = this.processor.iterator(this.prepareTraverser(traverser));
        return IteratorUtils.onLast(iterator, () -> processor.stop());
    }

    public boolean filterTraverser(final Traverser<C, S> traverser) {
        this.prepareProcessor();
        final Iterator<Traverser<C, E>> iterator = this.processor.iterator(this.prepareTraverser(traverser));
        final boolean hasNext = iterator.hasNext();
        this.processor.stop();
        return hasNext;
    }

    public List<CFunction<C>> getFunctions() {
        return this.functions;
    }

    public TraverserFactory<C> getTraverserFactory() {
        return this.traverserFactory;
    }

    public Bytecode<C> getBytecode() {
        return this.bytecode;
    }

    @Override
    public String toString() {
        return this.functions.toString();
    } // TODO: functions need access to compilations for nesting


    @Override
    public int hashCode() {
        return this.processorFactory.hashCode() ^ this.structureFactory.hashCode() ^ this.functions.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof Compilation &&
                this.functions.equals(((Compilation) object).functions) &&
                this.processorFactory.equals(((Compilation) object).processorFactory) &&
                this.structureFactory.equals(((Compilation) object).structureFactory);
    }

    @Override
    public Compilation<C, S, E> clone() {
        try {
            final Compilation<C, S, E> clone = (Compilation<C, S, E>) super.clone();
            clone.processor = null;
            clone.functions = new ArrayList<>(this.functions.size());
            for (final CFunction<C> function : this.functions) {
                clone.functions.add(function.clone());
            }
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    ////////

    public static <C, S, E> Compilation<C, S, E> compile(final SourceCompilation<C> source, final Bytecode<C> bytecode) {
        return new Compilation<>(source, bytecode);
    }

    public static <C, S, E> Compilation<C, S, E> compile(final Bytecode<C> bytecode) {
        return new Compilation<>(bytecode);
    }

    public static <C, S, E> Compilation<C, S, E> compile(final Object arg) {
        return new Compilation<>((Bytecode<C>) arg);
    }

    public static <C, S, E> Compilation<C, S, E> compileOrNull(final int index, final Object... args) {
        return args.length > index && args[index] instanceof Bytecode ?
                new Compilation<>((Bytecode<C>) args[index]) :
                null;
    }
}

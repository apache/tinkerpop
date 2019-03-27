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
import org.apache.tinkerpop.machine.processor.EmptyProcessor;
import org.apache.tinkerpop.machine.processor.ProcessorFactory;
import org.apache.tinkerpop.machine.strategy.Strategy;
import org.apache.tinkerpop.machine.structure.EmptyStructure;
import org.apache.tinkerpop.machine.structure.StructureFactory;

import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SourceCompilation<C> {

    private final Bytecode<C> originalSource;
    private final ProcessorFactory processorFactory;
    private final StructureFactory structureFactory;
    private final Set<Strategy<?>> strategies;
    private final CompositeCompiler compilers;

    public SourceCompilation(final Bytecode<C> sourcecode) {
        this.originalSource = sourcecode.clone();
        this.processorFactory = BytecodeUtil.getProcessorFactory(sourcecode).orElse(EmptyProcessor.instance());
        this.structureFactory = BytecodeUtil.getStructureFactory(sourcecode).orElse(EmptyStructure.instance());
        this.strategies = BytecodeUtil.getStrategies(sourcecode);
        this.compilers = BytecodeUtil.getCompilers(sourcecode);
    }

    public ProcessorFactory getProcessorFactory() {
        return this.processorFactory;
    }

    public StructureFactory getStructureFactory() {
        return this.structureFactory;
    }

    public Set<Strategy<?>> getStrategies() {
        return this.strategies;
    }

    public CompositeCompiler getCompilers() {
        return this.compilers;
    }

    public Bytecode<C> getSourceCode() {
        return this.originalSource;
    }

    @Override
    public int hashCode() {
        return this.originalSource.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof SourceCompilation && this.originalSource.equals(((SourceCompilation) object).originalSource);
    }

    @Override
    public String toString() {
        return this.originalSource.getSourceInstructions().toString();
    }

}

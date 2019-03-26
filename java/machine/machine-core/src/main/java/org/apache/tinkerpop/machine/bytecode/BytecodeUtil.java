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

import org.apache.tinkerpop.machine.bytecode.compiler.BytecodeCompiler;
import org.apache.tinkerpop.machine.bytecode.compiler.CompositeCompiler;
import org.apache.tinkerpop.machine.bytecode.compiler.CoreCompiler.Symbols;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.processor.ProcessorFactory;
import org.apache.tinkerpop.machine.strategy.Strategy;
import org.apache.tinkerpop.machine.strategy.StrategyUtil;
import org.apache.tinkerpop.machine.structure.StructureFactory;
import org.apache.tinkerpop.machine.traverser.TraverserFactory;
import org.apache.tinkerpop.machine.traverser.species.COP_TraverserFactory;
import org.apache.tinkerpop.machine.traverser.species.COR_TraverserFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class BytecodeUtil {

    public static <C> void strategize(final Bytecode<C> bytecode, final Set<Strategy<?>> strategies) {
        for (final Strategy strategy : strategies) {
            BytecodeUtil.strategize(bytecode, strategy);
        }
    }

    public static <C> void strategize(final Bytecode<C> bytecode) {
        BytecodeUtil.strategize(bytecode, BytecodeUtil.getStrategies(bytecode));
    }

    public static <C> void strategize(final Bytecode<C> bytecode, Strategy strategy) {
        strategy.apply(bytecode);
        for (final Instruction<C> instruction : bytecode.getInstructions()) {
            for (Object arg : instruction.args()) {
                if (arg instanceof Bytecode)
                    BytecodeUtil.strategize((Bytecode<C>) arg, strategy);
            }
        }
    }

    public static <C> Set<Strategy<?>> getStrategies(final Bytecode<C> bytecode) {
        try {
            final Set<Strategy<?>> strategies = new HashSet<>();
            for (final SourceInstruction sourceInstruction : bytecode.getSourceInstructions()) {
                if (sourceInstruction.op().equals(Symbols.WITH_STRATEGY)) {
                    strategies.add(((Class<? extends Strategy>) sourceInstruction.args()[0]).getConstructor().newInstance());
                } else if (sourceInstruction.op().equals(Symbols.WITH_PROCESSOR)) {
                    strategies.addAll(ProcessorFactory.processorStrategies(((Class<? extends ProcessorFactory>) sourceInstruction.args()[0])));
                } else if (sourceInstruction.op().equals(Symbols.WITH_STRUCTURE)) {
                    strategies.addAll(StructureFactory.structureStrategies(((Class<? extends StructureFactory>) sourceInstruction.args()[0])));
                }
            }
            return StrategyUtil.sortStrategies(strategies);

        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static <C> CompositeCompiler getCompilers(final Bytecode<C> bytecode) {
        final List<BytecodeCompiler> compilers = new ArrayList<>();
        BytecodeUtil.getProcessorFactory(bytecode).ifPresent(f -> compilers.addAll(f.getCompilers()));
        BytecodeUtil.getStructureFactory(bytecode).ifPresent(f -> compilers.addAll(f.getCompilers()));
        return CompositeCompiler.create(compilers);
    }

    public static <C> Optional<Coefficient<C>> getCoefficient(final Bytecode<C> bytecode) {
        try {
            Coefficient<C> coefficient = null;
            for (final SourceInstruction sourceInstruction : bytecode.getSourceInstructions()) {
                if (sourceInstruction.op().equals(Symbols.WITH_COEFFICIENT)) {
                    coefficient = ((Class<? extends Coefficient<C>>) sourceInstruction.args()[0]).getConstructor().newInstance();
                }
            }

            return Optional.ofNullable(coefficient);
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static <C> Optional<ProcessorFactory> getProcessorFactory(final Bytecode<C> bytecode) {
        try {
            ProcessorFactory processor = null;
            for (final SourceInstruction sourceInstruction : bytecode.getSourceInstructions()) {
                if (sourceInstruction.op().equals(Symbols.WITH_PROCESSOR)) {
                    processor = 1 == sourceInstruction.args().length ?
                            ((Class<? extends ProcessorFactory>) sourceInstruction.args()[0]).getConstructor().newInstance() :
                            ((Class<? extends ProcessorFactory>) sourceInstruction.args()[0]).getConstructor(Map.class).newInstance((Map) sourceInstruction.args()[1]);
                }
            }
            return Optional.ofNullable(processor);
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static <C> Optional<StructureFactory> getStructureFactory(final Bytecode<C> bytecode) {
        try {
            StructureFactory structure = null;
            for (final SourceInstruction sourceInstruction : bytecode.getSourceInstructions()) {
                if (sourceInstruction.op().equals(Symbols.WITH_STRUCTURE)) {
                    structure = ((Class<? extends StructureFactory>) sourceInstruction.args()[0]).getConstructor().newInstance();
                }
            }
            return Optional.ofNullable(structure);
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static boolean hasSourceInstruction(final Bytecode<?> bytecode, final String op) {
        for (final SourceInstruction sourceInstruction : bytecode.getSourceInstructions()) {
            if (sourceInstruction.op().equals(op))
                return true;
        }
        return false;
    }

    public static <C> void replaceInstruction(final Bytecode<C> bytecode, final Instruction<C> oldInstruction, final Instruction<C> newInstruction) {
        int index = bytecode.getInstructions().indexOf(oldInstruction);
        bytecode.getInstructions().remove(index);
        bytecode.getInstructions().add(index, newInstruction);
    }

    public static <C> void mergeSourceInstructions(final Bytecode<C> from, final Bytecode<C> to) {
        to.getSourceInstructions().addAll(0, from.getSourceInstructions());
    }

    public static <C> Optional<TraverserFactory<C>> getTraverserFactory(final Bytecode<C> bytecode) {
        // TODO: make this real
        for (final Instruction<C> instruction : bytecode.getInstructions()) {
            if (instruction.op().equals(Symbols.PATH))
                return Optional.of(COP_TraverserFactory.instance());
            else if (instruction.op().equals(Symbols.REPEAT))
                return Optional.of(COR_TraverserFactory.instance());
        }
        return Optional.of(COP_TraverserFactory.instance());
    }
}

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

import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.processor.ProcessorFactory;
import org.apache.tinkerpop.machine.strategy.Strategy;
import org.apache.tinkerpop.machine.structure.StructureFactory;
import org.apache.tinkerpop.machine.traverser.COPTraverserFactory;
import org.apache.tinkerpop.machine.traverser.CORTraverserFactory;
import org.apache.tinkerpop.machine.traverser.TraverserFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class BytecodeUtil {

    public static <C> void strategize(final Bytecode<C> bytecode) {
        for (final Strategy strategy : BytecodeUtil.getStrategies(bytecode)) {
            BytecodeUtil.strategize(bytecode, strategy);
        }
    }

    private static <C> void strategize(final Bytecode<C> bytecode, Strategy strategy) {
        strategy.apply(bytecode);
        for (final Instruction<C> instruction : bytecode.getInstructions()) {
            for (Object arg : instruction.args()) {
                if (arg instanceof Bytecode)
                    BytecodeUtil.strategize((Bytecode<C>) arg, strategy);
            }
        }
    }

    public static <C> List<Strategy> getStrategies(final Bytecode<C> bytecode) {
        try {
            final List<Strategy> strategies = new ArrayList<>();
            for (final SourceInstruction sourceInstruction : bytecode.getSourceInstructions()) {
                if (sourceInstruction.op().equals(CoreCompiler.Symbols.WITH_STRATEGY))
                    strategies.add(((Class<? extends Strategy>) sourceInstruction.args()[0]).getConstructor().newInstance());
            }
            // TODO: sort strategies
            return strategies;
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static <C> Optional<Coefficient<C>> getCoefficient(final Bytecode<C> bytecode) {
        try {
            Coefficient<C> coefficient = null;
            for (final SourceInstruction sourceInstruction : bytecode.getSourceInstructions()) {
                if (sourceInstruction.op().equals(CoreCompiler.Symbols.WITH_COEFFICIENT)) {
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
                if (sourceInstruction.op().equals(CoreCompiler.Symbols.WITH_PROCESSOR)) {
                    processor = (ProcessorFactory) ((Class<? extends Coefficient<C>>) sourceInstruction.args()[0]).getConstructor().newInstance();
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
                if (sourceInstruction.op().equals(CoreCompiler.Symbols.WITH_STRUCTURE)) {
                    structure = (StructureFactory) ((Class<? extends Coefficient<C>>) sourceInstruction.args()[0]).getConstructor().newInstance();
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

    public static <C> Optional<TraverserFactory<C>> getTraverserFactory(final Bytecode<C> bytecode) {
        // TODO: make this real
        for (final Instruction<C> instruction : bytecode.getInstructions()) {
            if (instruction.op().equals(CoreCompiler.Symbols.PATH))
                return Optional.of(COPTraverserFactory.instance());
            else if (instruction.op().equals(CoreCompiler.Symbols.REPEAT))
                return Optional.of(CORTraverserFactory.instance());
        }
        return Optional.of(COPTraverserFactory.instance());
    }
}

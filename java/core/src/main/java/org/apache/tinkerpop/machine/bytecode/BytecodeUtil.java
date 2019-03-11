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

import org.apache.tinkerpop.language.Symbols;
import org.apache.tinkerpop.machine.coefficients.Coefficient;
import org.apache.tinkerpop.machine.functions.CFunction;
import org.apache.tinkerpop.machine.functions.filter.IdentityFilter;
import org.apache.tinkerpop.machine.functions.filter.IsFilter;
import org.apache.tinkerpop.machine.functions.initial.InjectInitial;
import org.apache.tinkerpop.machine.functions.map.IncrMap;
import org.apache.tinkerpop.machine.functions.map.MapMap;
import org.apache.tinkerpop.machine.functions.map.PathMap;
import org.apache.tinkerpop.machine.functions.reduce.CountReduce;
import org.apache.tinkerpop.machine.processor.ProcessorFactory;
import org.apache.tinkerpop.machine.strategies.Strategy;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class BytecodeUtil {

    public static <C> Bytecode<C> strategize(final Bytecode<C> bytecode) {
        for (final Strategy strategy : BytecodeUtil.getStrategies(bytecode)) {
            strategy.apply(bytecode);
            for (final Instruction<C> instruction : bytecode.getInstructions()) {
                for (Object arg : instruction.args()) {
                    if (arg instanceof Bytecode)
                        strategy.apply((Bytecode<C>) arg);
                }
            }
        }
        return bytecode;
    }

    public static <C> List<CFunction<C>> compile(final Bytecode<C> bytecode) {
        final List<CFunction<C>> functions = new ArrayList<>();
        for (final Instruction<C> instruction : bytecode.getInstructions()) {
            functions.add(BytecodeUtil.generateFunction(instruction));
        }
        return functions;
    }

    public static <C> List<Strategy> getStrategies(final Bytecode<C> bytecode) {
        try {
            final List<Strategy> strategies = new ArrayList<>();
            for (final SourceInstruction sourceInstruction : bytecode.getSourceInstructions()) {
                if (sourceInstruction.op().equals(Symbols.WITH_STRATEGY))
                    strategies.add((Strategy) Class.forName(sourceInstruction.args()[0].toString()).getConstructor().newInstance());
            }
            // sort strategies
            return strategies;
        } catch (final ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static <C> Optional<Coefficient<C>> getCoefficient(final Bytecode<C> bytecode) {
        try {
            Coefficient<C> coefficient = null;
            for (final SourceInstruction sourceInstruction : bytecode.getSourceInstructions()) {
                if (sourceInstruction.op().equals(Symbols.WITH_COEFFICIENT)) {
                    coefficient = (Coefficient<C>) Class.forName(sourceInstruction.args()[0].toString()).getConstructor().newInstance();
                }
            }

            return Optional.ofNullable(coefficient);
        } catch (final ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static <C> Optional<ProcessorFactory> getProcessorFactory(final Bytecode<C> bytecode) {
        try {
            ProcessorFactory processor = null;
            for (final SourceInstruction sourceInstruction : bytecode.getSourceInstructions()) {
                if (sourceInstruction.op().equals(Symbols.WITH_PROCESSOR)) {
                    processor = (ProcessorFactory) Class.forName(sourceInstruction.args()[0].toString()).getConstructor().newInstance();
                }
            }
            return Optional.ofNullable(processor);
        } catch (final ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static <C> CFunction<C> generateFunction(final Instruction<C> instruction) {
        final String op = instruction.op();
        final Coefficient<C> coefficient = instruction.coefficient();
        final Set<String> labels = instruction.labels();
        switch (op) {
            case Symbols.COUNT:
                return new CountReduce<>(coefficient, labels);
            case Symbols.IDENTITY:
                return new IdentityFilter<>(coefficient, labels);
            case Symbols.INJECT:
                return new InjectInitial<>(coefficient, labels, instruction.args());
            case Symbols.IS:
                return new IsFilter<>(coefficient, labels, instruction.args()[0]);
            case Symbols.INCR:
                return new IncrMap<>(coefficient, labels);
            case Symbols.MAP:
                return new MapMap<>(coefficient, labels, compile((Bytecode<C>) instruction.args()[0]));
            case Symbols.PATH:
                return new PathMap<>(coefficient, labels);
            default:
                throw new RuntimeException("This is an unknown instruction:" + instruction.op());
        }
    }
}

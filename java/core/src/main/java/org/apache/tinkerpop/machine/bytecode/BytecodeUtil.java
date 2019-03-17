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
import org.apache.tinkerpop.machine.function.CFunction;
import org.apache.tinkerpop.machine.function.barrier.JoinBarrier;
import org.apache.tinkerpop.machine.function.barrier.StallBarrier;
import org.apache.tinkerpop.machine.function.branch.ChooseBranch;
import org.apache.tinkerpop.machine.function.branch.RepeatBranch;
import org.apache.tinkerpop.machine.function.branch.UnionBranch;
import org.apache.tinkerpop.machine.function.filter.FilterFilter;
import org.apache.tinkerpop.machine.function.filter.HasKeyFilter;
import org.apache.tinkerpop.machine.function.filter.HasKeyValueFilter;
import org.apache.tinkerpop.machine.function.filter.IdentityFilter;
import org.apache.tinkerpop.machine.function.filter.IsFilter;
import org.apache.tinkerpop.machine.function.flatmap.UnfoldFlatMap;
import org.apache.tinkerpop.machine.function.initial.InjectInitial;
import org.apache.tinkerpop.machine.function.map.ConstantMap;
import org.apache.tinkerpop.machine.function.map.IncrMap;
import org.apache.tinkerpop.machine.function.map.LoopsMap;
import org.apache.tinkerpop.machine.function.map.MapMap;
import org.apache.tinkerpop.machine.function.map.PathMap;
import org.apache.tinkerpop.machine.function.reduce.CountReduce;
import org.apache.tinkerpop.machine.function.reduce.GroupCountReduce;
import org.apache.tinkerpop.machine.function.reduce.SumReduce;
import org.apache.tinkerpop.machine.processor.ProcessorFactory;
import org.apache.tinkerpop.machine.strategy.Strategy;
import org.apache.tinkerpop.machine.traverser.COPTraverserFactory;
import org.apache.tinkerpop.machine.traverser.CORTraverserFactory;
import org.apache.tinkerpop.machine.traverser.TraverserFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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
                if (sourceInstruction.op().equals(Symbols.WITH_STRATEGY))
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
                    processor = (ProcessorFactory) ((Class<? extends Coefficient<C>>) sourceInstruction.args()[0]).getConstructor().newInstance();
                }
            }
            return Optional.ofNullable(processor);
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

    public static <C> Optional<TraverserFactory<C>> getTraverserFactory(final Bytecode<C> bytecode) {
        // TODO: make this real
        for (final Instruction<C> instruction : bytecode.getInstructions()) {
            if (instruction.op().equals(Symbols.PATH))
                return Optional.of(COPTraverserFactory.instance());
            else if (instruction.op().equals(Symbols.REPEAT))
                return Optional.of(CORTraverserFactory.instance());
        }
        return Optional.of(COPTraverserFactory.instance());
    }

    public static <C> List<CFunction<C>> compile(final Bytecode<C> bytecode) {
        final List<CFunction<C>> functions = new ArrayList<>();
        for (final Instruction<C> instruction : bytecode.getInstructions()) {
            functions.add(BytecodeUtil.generateFunction(instruction));
        }
        return functions;
    }

    private static <C> CFunction<C> generateFunction(final Instruction<C> instruction) {
        final String op = instruction.op();
        final Coefficient<C> coefficient = instruction.coefficient();
        final Set<String> labels = instruction.labels();
        switch (op) {
            case Symbols.BARRIER:
                return new StallBarrier<>(coefficient, labels, 1000);
            case Symbols.CHOOSE_IF_THEN:
                return new ChooseBranch<>(coefficient, labels,
                        Compilation.compileOne(instruction.args()[0]),
                        Compilation.compileOne(instruction.args()[1]),
                        null);
            case Symbols.CHOOSE_IF_THEN_ELSE:
                return new ChooseBranch<>(coefficient, labels,
                        Compilation.compileOne(instruction.args()[0]),
                        Compilation.compileOne(instruction.args()[1]),
                        Compilation.compileOne(instruction.args()[2]));
            case Symbols.CONSTANT:
                return new ConstantMap<>(coefficient, labels, instruction.args()[0]);
            case Symbols.COUNT:
                return new CountReduce<>(coefficient, labels);
            case Symbols.FILTER:
                return new FilterFilter<>(coefficient, labels, Compilation.compileOne(instruction.args()[0]));
            case Symbols.GROUP_COUNT:
                return new GroupCountReduce<>(coefficient, labels, Compilation.<C, Object, Object>compileMaybe(instruction.args()).orElse(null));
            case Symbols.HAS_KEY:
                return new HasKeyFilter<>(coefficient, labels, Argument.create(instruction.args()[0]));
            case Symbols.HAS_KEY_VALUE:
                return new HasKeyValueFilter<>(coefficient, labels, Argument.create(instruction.args()[0]), Argument.create(instruction.args()[1]));
            case Symbols.IDENTITY:
                return new IdentityFilter<>(coefficient, labels);
            case Symbols.INJECT:
                return new InjectInitial<>(coefficient, labels, instruction.args());
            case Symbols.IS:
                return new IsFilter<>(coefficient, labels, Argument.create(instruction.args()[0]));
            case Symbols.INCR:
                return new IncrMap<>(coefficient, labels);
            case Symbols.JOIN:
                return new JoinBarrier<>(coefficient, labels, Compilation.compileOne(instruction.args()[0]), Argument.create(instruction.args()[1]));
            case Symbols.LOOPS:
                return new LoopsMap<>(coefficient, labels);
            case Symbols.MAP:
                return new MapMap<>(coefficient, labels, Compilation.compileOne(instruction.args()[0]));
            case Symbols.PATH:
                return new PathMap<>(coefficient, labels, Compilation.compile(instruction.args()));
            case Symbols.REPEAT:
                return new RepeatBranch<>(coefficient, labels, Compilation.repeatCompile(instruction.args()));
            case Symbols.SUM:
                return new SumReduce<>(coefficient, labels);
            case Symbols.UNFOLD:
                return new UnfoldFlatMap<>(coefficient, labels);
            case Symbols.UNION:
                return new UnionBranch<>(coefficient, labels, Compilation.compile(instruction.args()));
            default:
                throw new RuntimeException("This is an unknown instruction:" + instruction.op());
        }
    }
}
